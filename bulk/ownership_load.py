#!/usr/bin/env python3
"""Load a yearly Aksjonaerregisteret CSV from Skatteetaten into the ownership table.

One file is one snapshot of who owned what at year end. The diff between two
years is what turns into transactions later, so every year is loaded with its
own source_year and never overwritten by another year.

Typical use:

    # Look at the first 50 rows without touching the database.
    python3 ownership_load.py ~/Downloads/aksjonaerregisteret_2025.csv --year 2025 --dry-run --limit 50

    # Real load.
    python3 ownership_load.py ~/Downloads/aksjonaerregisteret_2025.csv --year 2025
"""

import argparse
import csv
import json
import os
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

from dotenv import load_dotenv

import psycopg

BULK_DIR = Path(__file__).resolve().parent

# Header aliases. Skatteetaten changes the wording between years, so adjust the
# lists here rather than the parsing code when a new file shows up.
ALIASES = {
    "company_orgnr": ["Orgnr", "Organisasjonsnummer"],
    "company_name": ["Selskap", "Navn selskap"],
    "share_class": ["Aksjeklasse"],
    "owner_name": ["Navn aksjonær", "Navn aksjonaer", "Aksjonær", "Aksjonaer"],
    "owner_id": ["Fødselsår/orgnr", "Fodselsar/orgnr", "Fødselsår/Orgnr"],
    "owner_country": ["Landkode"],
    "shares": ["Antall aksjer"],
    "total_shares": ["Antall aksjer selskap"],
}

# Without these three the file cannot be loaded at all.
REQUIRED_FIELDS = ("company_orgnr", "owner_name", "shares")

# company_name and owner_country are parsed for logging and dry-run output only.
# The ownership table has no columns for them.

# Matches the schema default on ownership.share_class.
DEFAULT_SHARE_CLASS = "ordinär"

ENCODINGS = ("utf-8-sig", "cp1252", "latin-1")
SAMPLE_BYTES = 64 * 1024
PROGRESS_EVERY = 250_000
MAX_ERROR_LOG = 25
MAX_WARN_LOG = 10

UPSERT_SQL = """
insert into ownership (
  company_orgnr, owner_name, owner_orgnr, owner_birth_year,
  share_class, shares, total_shares, source_year
)
values (%s, %s, %s, %s, %s, %s, %s, %s)
on conflict (company_orgnr, owner_name, share_class, source_year) do update set
  shares = excluded.shares,
  total_shares = excluded.total_shares,
  owner_orgnr = excluded.owner_orgnr,
  owner_birth_year = excluded.owner_birth_year
"""


def log(message):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


# --------------------------------------------------------------------------
# Encoding and headers
# --------------------------------------------------------------------------

def detect_encoding(path):
    """Try utf-8-sig, then cp1252, then latin-1 on the first chunk of the file."""
    with path.open("rb") as handle:
        sample = handle.read(SAMPLE_BYTES)

    for encoding in ENCODINGS:
        try:
            sample.decode(encoding)
        except UnicodeDecodeError as exc:
            # A multi byte character can be cut in half by the sample boundary.
            # That is not a real decoding failure.
            if exc.start >= len(sample) - 4:
                return encoding
            continue
        return encoding
    return "latin-1"


def _norm(value):
    return (value or "").strip().lower()


def _loose(value):
    return "".join(ch for ch in _norm(value) if ch.isalnum())


def map_headers(headers):
    """Map canonical field names onto column indexes. Raises on missing required."""
    exact = {}
    loose = {}
    for index, header in enumerate(headers):
        exact.setdefault(_norm(header), index)
        loose.setdefault(_loose(header), index)

    mapping = {}
    for field, aliases in ALIASES.items():
        for alias in aliases:
            if _norm(alias) in exact:
                mapping[field] = exact[_norm(alias)]
                break
            if _loose(alias) in loose:
                mapping[field] = loose[_loose(alias)]
                break

    missing = [field for field in REQUIRED_FIELDS if field not in mapping]
    if missing:
        raise ValueError(
            "Could not match required column(s): "
            + ", ".join(missing)
            + ". Headers found in the file: "
            + ", ".join(repr(h) for h in headers)
            + ". Adjust the ALIASES dict at the top of ownership_load.py."
        )
    return mapping


# --------------------------------------------------------------------------
# Value parsing
# --------------------------------------------------------------------------

def strip_spaces(value):
    """Drop every whitespace character.

    Covers plain spaces as well as the non breaking spaces Skatteetaten uses as
    thousand separators (str.isspace() is True for U+00A0 and U+202F).
    """
    return "".join(ch for ch in str(value) if not ch.isspace())


def parse_decimal(value):
    """Parse Norwegian style numbers: space as thousand separator, comma as decimal."""
    if value is None:
        return None
    text = strip_spaces(value)
    if not text:
        return None
    if "." in text and "," in text:
        # Both present: the dot is the thousand separator.
        text = text.replace(".", "")
    text = text.replace(",", ".")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def parse_owner_id(value):
    """9 digits means a company, 4 digits means a birth year, anything else is unknown."""
    if value is None:
        return None, None
    digits = strip_spaces(value)
    if not digits.isdigit():
        return None, None
    if len(digits) == 9:
        return digits, None
    if len(digits) == 4:
        return None, int(digits)
    return None, None


def cell(row, mapping, field):
    index = mapping.get(field)
    if index is None or index >= len(row):
        return None
    value = row[index]
    return value.strip() if isinstance(value, str) else value


def json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


# --------------------------------------------------------------------------
# Reading and aggregating
# --------------------------------------------------------------------------

def aggregate_file(path, args, stats):
    """Read the CSV and fold duplicate primary keys together.

    Duplicates inside one file are real: the same owner can appear on several
    lines for the same share class. Those must be summed before the upsert,
    otherwise the last line silently wins.

    Memory note: the dict holds one entry per unique
    (company_orgnr, owner_name, share_class). A full year is a few million rows,
    which lands around 1-2 GB of RAM worst case. If that becomes a problem, sort
    the file by company_orgnr first and flush one company at a time.
    """
    encoding = detect_encoding(path)
    log(f"Reading {path} with encoding {encoding} and delimiter {args.delimiter!r}.")

    aggregated = {}
    year = args.year

    with path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.reader(handle, delimiter=args.delimiter)
        try:
            headers = next(reader)
        except StopIteration:
            raise ValueError("The CSV file is empty")

        mapping = map_headers(headers)
        log("Column mapping: " + ", ".join(f"{k} -> {headers[v]!r}" for k, v in sorted(mapping.items())))

        for row in reader:
            if not row or all(not (c or "").strip() for c in row):
                continue
            stats["read"] += 1

            company_orgnr = cell(row, mapping, "company_orgnr")
            owner_name = cell(row, mapping, "owner_name")
            if company_orgnr:
                company_orgnr = strip_spaces(company_orgnr)

            if not company_orgnr or not owner_name:
                stats["skipped"] += 1
                if stats["skipped"] <= MAX_WARN_LOG:
                    log(f"WARNING: row {stats['read']} has no company orgnr or no owner name, skipped.")
            else:
                shares = parse_decimal(cell(row, mapping, "shares"))
                if shares is None:
                    stats["skipped"] += 1
                    if stats["skipped"] <= MAX_WARN_LOG:
                        log(f"WARNING: row {stats['read']} has an unparsable share count, skipped.")
                else:
                    share_class = cell(row, mapping, "share_class") or DEFAULT_SHARE_CLASS
                    total_shares = parse_decimal(cell(row, mapping, "total_shares"))
                    owner_orgnr, owner_birth_year = parse_owner_id(cell(row, mapping, "owner_id"))

                    key = (company_orgnr, owner_name, share_class, year)
                    existing = aggregated.get(key)
                    if existing is None:
                        aggregated[key] = {
                            "shares": shares,
                            "total_shares": total_shares,
                            "owner_orgnr": owner_orgnr,
                            "owner_birth_year": owner_birth_year,
                        }
                    else:
                        stats["duplicates"] += 1
                        existing["shares"] += shares
                        if total_shares is not None and (
                            existing["total_shares"] is None or total_shares > existing["total_shares"]
                        ):
                            existing["total_shares"] = total_shares
                        if existing["owner_orgnr"] is None:
                            existing["owner_orgnr"] = owner_orgnr
                        if existing["owner_birth_year"] is None:
                            existing["owner_birth_year"] = owner_birth_year

            if stats["read"] % PROGRESS_EVERY == 0:
                log(f"  {stats['read']:,} rows read, {len(aggregated):,} unique keys so far.")

            if args.limit is not None and stats["read"] >= args.limit:
                log(f"Reached --limit {args.limit}, stopping the read.")
                break

    stats["unique"] = len(aggregated)
    return aggregated


def to_params(key, value):
    company_orgnr, owner_name, share_class, source_year = key
    return (
        company_orgnr,
        owner_name,
        value["owner_orgnr"],
        value["owner_birth_year"],
        share_class,
        value["shares"],
        value["total_shares"],
        source_year,
    )


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------

def require_database_url():
    url = os.environ.get("DATABASE_URL")
    if not url:
        log("FATAL: DATABASE_URL is not set. Put it in bulk/.env or export it.")
        sys.exit(1)
    return url


def flush_batch(conn, cur, batch, stats):
    """Upsert one batch inside its own transaction, isolating bad rows."""
    if not batch:
        return
    try:
        cur.executemany(UPSERT_SQL, batch)
        conn.commit()
        stats["upserted"] += len(batch)
    except psycopg.Error as exc:
        conn.rollback()
        log(f"WARNING: batch of {len(batch)} rows failed ({exc}). Retrying row by row.")
        for params in batch:
            try:
                cur.execute(UPSERT_SQL, params)
                conn.commit()
                stats["upserted"] += 1
            except psycopg.Error as row_exc:
                conn.rollback()
                stats["errors"] += 1
                if stats["errors"] <= MAX_ERROR_LOG:
                    log(f"ERROR: company_orgnr={params[0]} owner={params[1]!r}: {row_exc}")
    finally:
        batch.clear()


def write_rows(aggregated, args, stats):
    database_url = require_database_url()
    total = len(aggregated)
    log(f"Upserting {total:,} rows in batches of {args.batch_size}.")
    started = time.monotonic()
    log_every = args.batch_size * 100

    with psycopg.connect(database_url, application_name="brreg-ownership-load") as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            batch = []
            logged_at = 0
            for key, value in aggregated.items():
                batch.append(to_params(key, value))
                if len(batch) >= args.batch_size:
                    flush_batch(conn, cur, batch, stats)
                    if stats["upserted"] - logged_at >= log_every:
                        logged_at = stats["upserted"]
                        log(f"  {stats['upserted']:,}/{total:,} rows written.")
            flush_batch(conn, cur, batch, stats)

    log(f"Write finished in {(time.monotonic() - started) / 60:.1f} min.")


def print_rows(aggregated):
    for key, value in aggregated.items():
        company_orgnr, owner_name, share_class, source_year = key
        record = {
            "company_orgnr": company_orgnr,
            "owner_name": owner_name,
            "owner_orgnr": value["owner_orgnr"],
            "owner_birth_year": value["owner_birth_year"],
            "share_class": share_class,
            "shares": value["shares"],
            "total_shares": value["total_shares"],
            "source_year": source_year,
        }
        print(json.dumps(record, ensure_ascii=False, default=json_default))


# --------------------------------------------------------------------------
# Run
# --------------------------------------------------------------------------

def new_stats():
    return {"read": 0, "unique": 0, "duplicates": 0, "upserted": 0, "skipped": 0, "errors": 0}


def print_summary(stats, dry_run):
    log("---- summary ----")
    log(f"rows read        : {stats['read']:,}")
    log(f"unique keys      : {stats['unique']:,}")
    log(f"duplicates merged: {stats['duplicates']:,}  (shares summed, largest total_shares kept)")
    if dry_run:
        log("upserted         : 0  (dry run)")
    else:
        log(f"upserted         : {stats['upserted']:,}  (inserted or updated)")
    log(f"skipped          : {stats['skipped']:,}  (missing orgnr, owner name or share count)")
    log(f"errors           : {stats['errors']:,}")
    if stats["duplicates"]:
        log(
            f"WARNING: {stats['duplicates']:,} duplicate primary keys were merged into "
            f"{stats['unique']:,} rows. Check the file if that number looks unexpected."
        )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Load an Aksjonaerregisteret CSV into the ownership table.",
    )
    parser.add_argument("csv_path", help="Path to the semicolon separated CSV from Skatteetaten.")
    parser.add_argument("--year", type=int, required=True, help="Register year, written to source_year.")
    parser.add_argument("--dry-run", action="store_true", help="Print mapped rows as JSON lines, no DB writes.")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N data rows.")
    parser.add_argument("--delimiter", default=";", help="Field delimiter (default ';', use '\\t' for tab).")
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per upsert batch (default 1000).")
    args = parser.parse_args(argv)

    if args.delimiter == "\\t":
        args.delimiter = "\t"
    if len(args.delimiter) != 1:
        parser.error("--delimiter must be a single character")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be a positive integer")
    if args.batch_size <= 0:
        parser.error("--batch-size must be a positive integer")
    if not 1900 <= args.year <= 2100:
        parser.error("--year looks wrong, expected something like 2025")
    return args


def main(argv=None):
    load_dotenv(BULK_DIR / ".env")
    args = parse_args(argv)
    path = Path(args.csv_path).expanduser()
    stats = new_stats()

    if not path.is_file():
        log(f"FATAL: file not found: {path}")
        return 1

    # Ownership files can carry very long free text fields.
    csv.field_size_limit(10_000_000)

    try:
        aggregated = aggregate_file(path, args, stats)
        if not aggregated:
            log("No usable rows found.")
            print_summary(stats, args.dry_run)
            return 1
        if args.dry_run:
            log("Dry run: printing aggregated rows, nothing is written.")
            print_rows(aggregated)
        else:
            write_rows(aggregated, args, stats)
    except KeyboardInterrupt:
        log("Interrupted by user.")
        print_summary(stats, args.dry_run)
        return 130
    except psycopg.Error as exc:
        log(f"FATAL: database error: {exc}")
        return 1
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        log(f"FATAL: {exc}")
        return 1

    print_summary(stats, args.dry_run)
    return 1 if stats["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
