#!/usr/bin/env python3
"""One-time bulk load of the Norwegian Enhetsregisteret into the companies table.

Downloads the full gzipped JSON dump from BRREG, parses it incrementally with
ijson and upserts every entity into Postgres (Supabase).

Typical use:

    # Parse the first 20 entities straight from HTTP, print JSON, no DB needed.
    python3 bulk_load.py --dry-run --limit 20

    # Real run: download once to <repo>/data/enheter.json.gz, then upsert ~1.1M rows.
    python3 bulk_load.py

    # Re-download and load without storing the raw JSON blob.
    python3 bulk_load.py --refresh --no-raw
"""

import argparse
import gzip
import json
import os
import sys
import time
from contextlib import closing
from decimal import Decimal
from pathlib import Path

import ijson
import requests
from dotenv import load_dotenv

import psycopg
from psycopg.types.json import Jsonb

BULK_DIR = Path(__file__).resolve().parent
REPO_ROOT = BULK_DIR.parent
DEFAULT_FILE = REPO_ROOT / "data" / "enheter.json.gz"

DOWNLOAD_URL = "https://data.brreg.no/enhetsregisteret/api/enheter/lastned"
# BRREG versioned the media type. The unversioned form now answers 406 Not
# Acceptable, so the .v2 variant is required. application/gzip also works if
# BRREG ever bumps the version again.
ACCEPT_HEADER = "application/vnd.brreg.enhetsregisteret.enhet.v2+gzip;charset=UTF-8"
USER_AGENT = "brreg-pipeline/1.0 (local n8n + python)"

# Canonical org form translation. Anything unknown falls back to lowercase raw.
ORG_FORM_MAP = {
    "AS": "aktiebolag",
    "ASA": "publikt_aktiebolag",
    "ENK": "enskild_firma",
    "NUF": "filial",
}

PROGRESS_EVERY = 50_000
EXPECTED_TOTAL = 1_100_000
DOWNLOAD_LOG_BYTES = 50 * 1024 * 1024
CHUNK_BYTES = 1024 * 1024
MAX_ERROR_LOG = 25
GZIP_MAGIC = b"\x1f\x8b"

UPSERT_SQL = """
insert into companies (
  orgnr, name, org_form, org_form_raw, nace_code, founded, employees,
  municipality, address, status, website, raw, last_updated
)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
on conflict (orgnr) do update set
  name = excluded.name,
  org_form = excluded.org_form,
  org_form_raw = excluded.org_form_raw,
  nace_code = excluded.nace_code,
  founded = excluded.founded,
  employees = excluded.employees,
  municipality = excluded.municipality,
  address = excluded.address,
  status = excluded.status,
  website = excluded.website,
  raw = excluded.raw,
  last_updated = now()
"""


def log(message):
    """Timestamped line on stdout, flushed so tail -f and n8n logs stay live."""
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def _json_default(obj):
    """Fallback encoder for values the stdlib json module cannot handle.

    ijson yields decimal.Decimal for every JSON number that carries a fraction
    or an exponent (plain integers stay int). Real BRREG entities contain such
    numbers, for example kapital.belop, and json.dumps raises TypeError on them.
    """
    if isinstance(obj, Decimal):
        as_int = int(obj)
        return as_int if obj == as_int else float(obj)
    raise TypeError(f"not JSON serializable: {type(obj)!r}")


def json_dumps(obj):
    """The one JSON serializer used for everything that leaves this script.

    Covers the dry-run output line as well as the address and raw jsonb columns,
    which psycopg would otherwise serialize with a plain json.dumps.
    """
    return json.dumps(obj, ensure_ascii=False, default=_json_default)


def build_session():
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


# --------------------------------------------------------------------------
# Mapping
# --------------------------------------------------------------------------

def _clean_text(value):
    """Return a stripped string, or None for empty / non-string input."""
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    return value or None


def _to_int(value):
    """Coerce to int. Handles the Decimal values ijson can hand back.

    ArithmeticError catches decimal.InvalidOperation, which int() raises for a
    Decimal NaN or Infinity.
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError, ArithmeticError):
        return None


def _to_date(value):
    """BRREG dates are ISO strings. Keep the YYYY-MM-DD part, drop garbage."""
    text = _clean_text(value)
    if text is None:
        return None
    candidate = text[:10]
    if len(candidate) != 10 or candidate[4] != "-" or candidate[7] != "-":
        return None
    return candidate


def derive_status(entity):
    """Map BRREG lifecycle flags onto the status values used by the schema."""
    if entity.get("slettedato"):
        return "upphörd"
    if entity.get("konkurs") is True:
        return "konkurs"
    if entity.get("underAvvikling") is True or entity.get(
        "underTvangsavviklingEllerTvangsopplosning"
    ) is True:
        return "under_avveckling"
    return "aktiv"


def map_enhet(e, include_raw=True):
    """Map one Enhetsregisteret entity onto a companies row.

    Returns a dict of plain Python values (JSON serializable so --dry-run can
    print it), or None when the row must be skipped (no orgnr or no name).
    """
    orgnr = _clean_text(e.get("organisasjonsnummer"))
    name = _clean_text(e.get("navn"))
    if not orgnr or not name:
        return None

    org_form_obj = e.get("organisasjonsform") or {}
    org_form_raw = _clean_text(org_form_obj.get("kode"))
    if org_form_raw:
        org_form = ORG_FORM_MAP.get(org_form_raw, org_form_raw.lower())
    else:
        org_form = None

    nace_obj = e.get("naeringskode1") or {}
    nace_code = _clean_text(nace_obj.get("kode"))

    address = e.get("forretningsadresse")
    if not isinstance(address, dict):
        address = None
    municipality = _clean_text(address.get("kommune")) if address else None

    return {
        "orgnr": orgnr,
        "name": name,
        "org_form": org_form,
        "org_form_raw": org_form_raw,
        "nace_code": nace_code,
        "founded": _to_date(e.get("stiftelsesdato")),
        "employees": _to_int(e.get("antallAnsatte")),
        "municipality": municipality,
        "address": address,
        "status": derive_status(e),
        "website": _clean_text(e.get("hjemmeside")),
        "raw": e if include_raw else None,
    }


def to_params(row):
    """Turn a mapped dict into the positional tuple used by UPSERT_SQL."""
    address = row["address"]
    raw = row["raw"]
    return (
        row["orgnr"],
        row["name"],
        row["org_form"],
        row["org_form_raw"],
        row["nace_code"],
        row["founded"],
        row["employees"],
        row["municipality"],
        # dumps= is required here: psycopg's own default is a bare json.dumps,
        # which chokes on the Decimal values ijson produces.
        Jsonb(address, dumps=json_dumps) if address is not None else None,
        row["status"],
        row["website"],
        Jsonb(raw, dumps=json_dumps) if raw is not None else None,
    )


# --------------------------------------------------------------------------
# Download and parsing
# --------------------------------------------------------------------------

def download_dump(session, dest, refresh):
    """Stream the full dump to dest. Skips the download if the file is there."""
    if dest.exists() and not refresh and dest.stat().st_size > 0:
        size_mb = dest.stat().st_size / (1024 * 1024)
        log(f"Reusing existing dump {dest} ({size_mb:.1f} MB). Pass --refresh to download again.")
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    log(f"Downloading {DOWNLOAD_URL} to {dest}")

    with session.get(
        DOWNLOAD_URL,
        headers={"Accept": ACCEPT_HEADER},
        stream=True,
        timeout=(30, 600),
    ) as resp:
        resp.raise_for_status()
        declared = _to_int(resp.headers.get("Content-Length")) or 0
        written = 0
        next_mark = DOWNLOAD_LOG_BYTES
        started = time.monotonic()
        with tmp.open("wb") as handle:
            for chunk in resp.iter_content(chunk_size=CHUNK_BYTES):
                if not chunk:
                    continue
                handle.write(chunk)
                written += len(chunk)
                if written >= next_mark:
                    elapsed = max(time.monotonic() - started, 0.001)
                    speed = written / elapsed / (1024 * 1024)
                    total_note = f" of {declared / (1024 * 1024):.0f} MB" if declared else ""
                    log(f"  downloaded {written / (1024 * 1024):.0f} MB{total_note} ({speed:.1f} MB/s)")
                    next_mark += DOWNLOAD_LOG_BYTES

    if written == 0:
        tmp.unlink(missing_ok=True)
        raise RuntimeError("Download produced an empty file")

    tmp.replace(dest)
    log(f"Download complete: {written / (1024 * 1024):.1f} MB")
    return dest


def open_dump(path):
    """Open the dump for reading, transparently handling gzip or plain JSON.

    requests transparently decodes Content-Encoding, so a server that labels the
    body as gzip encoded rather than gzip content leaves us with plain JSON on
    disk. Sniffing the magic bytes keeps both cases working.
    """
    with path.open("rb") as probe:
        magic = probe.read(2)
    if magic == GZIP_MAGIC:
        return gzip.open(path, "rb")
    log("NOTE: dump file is not gzipped, reading it as plain JSON.")
    return path.open("rb")


def iter_entities_from_file(path):
    """Yield entities from the on-disk dump (one big JSON array)."""
    with open_dump(path) as handle:
        for entity in ijson.items(handle, "item"):
            yield entity


def iter_entities_from_stream(session):
    """Yield entities straight off the HTTP response, without saving the file.

    Used by --dry-run so a smoke test never downloads the full ~1 GB dump. The
    connection is closed as soon as the consumer stops iterating.
    """
    resp = session.get(
        DOWNLOAD_URL,
        headers={"Accept": ACCEPT_HEADER},
        stream=True,
        timeout=(30, 600),
    )
    try:
        resp.raise_for_status()
        # decode_content=True lets urllib3 undo Content-Encoding if the server
        # set one. The gzip container of the dump itself is handled below.
        resp.raw.decode_content = True
        with gzip.GzipFile(fileobj=resp.raw) as gz:
            for entity in ijson.items(gz, "item"):
                yield entity
    finally:
        resp.close()


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
                    log(f"ERROR: orgnr={params[0]}: {row_exc}")
    finally:
        batch.clear()


# --------------------------------------------------------------------------
# Runs
# --------------------------------------------------------------------------

def new_stats():
    return {"parsed": 0, "mapped": 0, "skipped": 0, "errors": 0, "upserted": 0}


def run_dry(args, session, stats):
    log("Dry run: streaming from BRREG, printing mapped rows, no database writes.")
    if args.limit is None:
        log("WARNING: no --limit given, the whole dump will be streamed and printed.")

    started = time.monotonic()

    with closing(iter_entities_from_stream(session)) as entities:
        for entity in entities:
            stats["parsed"] += 1
            row = map_enhet(entity, include_raw=not args.no_raw)
            if row is None:
                stats["skipped"] += 1
            else:
                stats["mapped"] += 1
                print(json_dumps(row))
            if args.limit is not None and stats["parsed"] >= args.limit:
                break

    log(f"Dry run finished in {time.monotonic() - started:.1f}s (connection closed).")


def run_load(args, session, stats):
    database_url = require_database_url()
    path = Path(args.file).expanduser().resolve()
    download_dump(session, path, args.refresh)

    started = time.monotonic()
    batch = []
    include_raw = not args.no_raw
    if not include_raw:
        log("--no-raw: the raw column will be written as NULL.")

    log(f"Connecting to Postgres and upserting in batches of {args.batch_size}.")
    with psycopg.connect(database_url, application_name="brreg-bulk-load") as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            for entity in iter_entities_from_file(path):
                stats["parsed"] += 1
                row = map_enhet(entity, include_raw=include_raw)
                if row is None:
                    stats["skipped"] += 1
                else:
                    stats["mapped"] += 1
                    batch.append(to_params(row))
                    if len(batch) >= args.batch_size:
                        flush_batch(conn, cur, batch, stats)

                if stats["parsed"] % PROGRESS_EVERY == 0:
                    elapsed = max(time.monotonic() - started, 0.001)
                    rate = stats["parsed"] / elapsed
                    remaining = max(EXPECTED_TOTAL - stats["parsed"], 0)
                    eta = remaining / rate if rate > 0 else 0
                    log(
                        f"  {stats['parsed']:,} parsed / ~{EXPECTED_TOTAL:,} "
                        f"| upserted {stats['upserted']:,} | skipped {stats['skipped']:,} "
                        f"| {rate:.0f} rows/s | ETA {eta / 60:.1f} min"
                    )

                if args.limit is not None and stats["parsed"] >= args.limit:
                    log(f"Reached --limit {args.limit}, stopping.")
                    break

            flush_batch(conn, cur, batch, stats)

    log(f"Load finished in {(time.monotonic() - started) / 60:.1f} min.")


def print_summary(stats):
    log("---- summary ----")
    log(f"parsed entities : {stats['parsed']:,}")
    log(f"mapped rows     : {stats['mapped']:,}")
    log(f"upserted        : {stats['upserted']:,}  (inserted or updated, ON CONFLICT does not tell them apart)")
    log(f"skipped         : {stats['skipped']:,}  (missing orgnr or name)")
    log(f"errors          : {stats['errors']:,}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Bulk load Enhetsregisteret (~1.1M companies) into the companies table.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Stream from HTTP, print mapped rows as JSON lines, no DB and no DATABASE_URL needed.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Stop after N entities.")
    parser.add_argument("--refresh", action="store_true", help="Force a re-download of the dump.")
    parser.add_argument(
        "--file",
        default=str(DEFAULT_FILE),
        help=f"Path to the gzipped dump (default: {DEFAULT_FILE}).",
    )
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per upsert batch (default 1000).")
    parser.add_argument(
        "--no-raw",
        action="store_true",
        help="Write NULL into the raw column instead of the full entity JSON (saves space).",
    )
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be a positive integer")
    if args.batch_size <= 0:
        parser.error("--batch-size must be a positive integer")
    return args


def main(argv=None):
    load_dotenv(BULK_DIR / ".env")
    args = parse_args(argv)
    session = build_session()
    stats = new_stats()
    try:
        if args.dry_run:
            run_dry(args, session, stats)
        else:
            run_load(args, session, stats)
    except KeyboardInterrupt:
        log("Interrupted by user.")
        print_summary(stats)
        return 130
    except requests.RequestException as exc:
        log(f"FATAL: HTTP error: {exc}")
        return 1
    except psycopg.Error as exc:
        log(f"FATAL: database error: {exc}")
        return 1
    except (OSError, ValueError, RuntimeError) as exc:
        log(f"FATAL: {exc}")
        return 1
    finally:
        session.close()

    print_summary(stats)
    return 1 if stats["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
