#!/usr/bin/env python3
"""Fetch annual accounts from BRREG Regnskapsregisteret into the financials table.

The orgnr list comes from the companies table (AS and ASA only, those are the
forms that must file accounts). One HTTP call per orgnr, throttled and retried,
then an upsert per accounting year returned.

Typical use:

    # Single company, nothing written, no DATABASE_URL needed.
    python3 regnskap_batch.py --orgnr 923609016 --dry-run

    # Full run at 5 requests per second, skipping companies already up to date.
    python3 regnskap_batch.py --skip-existing

    # Small live test.
    python3 regnskap_batch.py --limit 50 --rate 2
"""

import argparse
import datetime
import json
import os
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests
from dotenv import load_dotenv

import psycopg
from psycopg.types.json import Jsonb

BULK_DIR = Path(__file__).resolve().parent

API_URL = "https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}"
USER_AGENT = "brreg-pipeline/1.0 (local n8n + python)"

MAX_ATTEMPTS = 3
BACKOFF_BASE = 2.0
MAX_BACKOFF = 60.0
COMMIT_EVERY = 200
PROGRESS_EVERY = 1000
MAX_ERROR_LOG = 25
ROW_SAVEPOINT = "fin_row"

ORGNR_SQL = """
select orgnr
from companies
where org_form_raw in ('AS', 'ASA')
order by orgnr
"""

ORGNR_SKIP_EXISTING_SQL = """
select c.orgnr
from companies c
where c.org_form_raw in ('AS', 'ASA')
  and not exists (
    select 1 from financials f
    where f.orgnr = c.orgnr and f.year >= %s
  )
order by c.orgnr
"""

UPSERT_SQL = """
insert into financials (
  orgnr, year, currency, revenue, ebit, net_result, equity, debt, assets, raw
)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
on conflict (orgnr, year) do update set
  currency = excluded.currency,
  revenue = excluded.revenue,
  ebit = excluded.ebit,
  net_result = excluded.net_result,
  equity = excluded.equity,
  debt = excluded.debt,
  assets = excluded.assets,
  raw = excluded.raw
returning (xmax = 0) as was_inserted
"""


def log(message):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def build_session():
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return session


class RateLimiter:
    """Simple pacing: never start two requests closer than 1/rate seconds."""

    def __init__(self, rate):
        self.min_interval = 1.0 / rate if rate and rate > 0 else 0.0
        self._next_at = 0.0

    def wait(self):
        if self.min_interval <= 0:
            return
        now = time.monotonic()
        if now < self._next_at:
            time.sleep(self._next_at - now)
            now = time.monotonic()
        self._next_at = now + self.min_interval


# --------------------------------------------------------------------------
# Mapping
# --------------------------------------------------------------------------

def dig(obj, *keys):
    """Safe navigation through nested dicts. Any missing level returns None."""
    current = obj
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def to_number(value):
    """Numbers come back as JSON ints or floats. Decimal keeps numeric exact."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def map_statement(orgnr, entry):
    """Map one accounting statement onto a financials row, or None if unusable."""
    if not isinstance(entry, dict):
        return None

    til_dato = dig(entry, "regnskapsperiode", "tilDato")
    if not isinstance(til_dato, str) or len(til_dato) < 4:
        return None
    try:
        year = int(til_dato[:4])
    except ValueError:
        return None

    currency = entry.get("valuta") or "NOK"
    if not isinstance(currency, str) or not currency.strip():
        currency = "NOK"
    currency = currency.strip().upper()[:3]

    return {
        "orgnr": orgnr,
        "year": year,
        "currency": currency,
        "revenue": to_number(
            dig(entry, "resultatregnskapResultat", "driftsresultat", "driftsinntekter", "sumDriftsinntekter")
        ),
        "ebit": to_number(
            dig(entry, "resultatregnskapResultat", "driftsresultat", "driftsresultat")
        ),
        "net_result": to_number(dig(entry, "resultatregnskapResultat", "aarsresultat")),
        "equity": to_number(dig(entry, "egenkapitalGjeld", "egenkapital", "sumEgenkapital")),
        "debt": to_number(dig(entry, "egenkapitalGjeld", "gjeldOversikt", "sumGjeld")),
        "assets": to_number(dig(entry, "eiendeler", "sumEiendeler")),
        "raw": entry,
    }


def to_params(row):
    return (
        row["orgnr"],
        row["year"],
        row["currency"],
        row["revenue"],
        row["ebit"],
        row["net_result"],
        row["equity"],
        row["debt"],
        row["assets"],
        Jsonb(row["raw"]) if row["raw"] is not None else None,
    )


def json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

def fetch_accounts(session, orgnr, limiter):
    """Return (outcome, payload).

    outcome is one of:
      'ok'    -> payload is a list of statement dicts (possibly empty)
      'none'  -> nothing filed (404 / 410), counts as skipped
      'error' -> gave up after retries, payload is the message
    """
    url = API_URL.format(orgnr=orgnr)
    last_error = "unknown error"

    for attempt in range(1, MAX_ATTEMPTS + 1):
        limiter.wait()
        try:
            resp = session.get(url, timeout=(10, 60))
        except requests.RequestException as exc:
            last_error = f"connection error: {exc}"
            if attempt == MAX_ATTEMPTS:
                break
            _sleep_backoff(attempt, None)
            continue

        status = resp.status_code
        if status == 200:
            try:
                payload = resp.json()
            except ValueError as exc:
                return "error", f"invalid JSON: {exc}"
            if isinstance(payload, dict):
                payload = [payload]
            if not isinstance(payload, list):
                return "error", f"unexpected payload type {type(payload).__name__}"
            return "ok", payload

        if status in (404, 410):
            return "none", None

        if status == 429 or 500 <= status < 600:
            last_error = f"HTTP {status}"
            if attempt == MAX_ATTEMPTS:
                break
            _sleep_backoff(attempt, resp.headers.get("Retry-After"))
            continue

        return "error", f"HTTP {status}"

    return "error", f"gave up after {MAX_ATTEMPTS} attempts ({last_error})"


def _sleep_backoff(attempt, retry_after):
    delay = BACKOFF_BASE ** attempt
    if retry_after:
        try:
            delay = max(delay, float(retry_after))
        except (TypeError, ValueError):
            pass
    time.sleep(min(delay, MAX_BACKOFF))


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------

def require_database_url():
    url = os.environ.get("DATABASE_URL")
    if not url:
        log("FATAL: DATABASE_URL is not set. Put it in bulk/.env or export it.")
        sys.exit(1)
    return url


def load_orgnrs(database_url, skip_existing, limit):
    """Read the orgnr worklist into memory (a few hundred thousand strings)."""
    cutoff_year = datetime.date.today().year - 1
    with psycopg.connect(database_url, application_name="brreg-regnskap-list") as conn:
        with conn.cursor() as cur:
            if skip_existing:
                log(f"Loading orgnrs without a financials row for year >= {cutoff_year}.")
                cur.execute(ORGNR_SKIP_EXISTING_SQL, (cutoff_year,))
            else:
                log("Loading all AS and ASA orgnrs from companies.")
                cur.execute(ORGNR_SQL)
            orgnrs = [row[0] for row in cur.fetchall()]
    if limit is not None:
        orgnrs = orgnrs[:limit]
    log(f"Worklist: {len(orgnrs):,} orgnrs.")
    return orgnrs


def upsert_rows(conn, cur, rows, stats):
    """Upsert one company's statements, then commit once COMMIT_EVERY is reached.

    Every row runs inside its own SAVEPOINT so one bad row does not throw away
    the rows still waiting for the next commit. conn.transaction() is not used
    here on purpose: the outermost transaction block commits when it exits,
    which would turn COMMIT_EVERY into a commit per row.
    """
    for row in rows:
        params = to_params(row)
        try:
            # psycopg opens the transaction implicitly before this statement.
            cur.execute(f"savepoint {ROW_SAVEPOINT}")
            cur.execute(UPSERT_SQL, params)
            result = cur.fetchone()
            cur.execute(f"release savepoint {ROW_SAVEPOINT}")
        except psycopg.Error as exc:
            stats["errors"] += 1
            if stats["errors"] <= MAX_ERROR_LOG:
                log(f"ERROR: upsert orgnr={row['orgnr']} year={row['year']}: {exc}")
            try:
                cur.execute(f"rollback to savepoint {ROW_SAVEPOINT}")
                cur.execute(f"release savepoint {ROW_SAVEPOINT}")
            except psycopg.Error:
                # The whole transaction is unusable, drop everything pending.
                conn.rollback()
                if stats["pending"]:
                    log(
                        f"WARNING: transaction aborted, {stats['pending']} uncommitted rows "
                        "were rolled back. Re-run to pick them up again."
                    )
                    stats["rolled_back"] += stats["pending"]
                    stats["pending"] = 0
            continue

        if result and result[0]:
            stats["inserted"] += 1
        else:
            stats["updated"] += 1
        stats["pending"] += 1

    if stats["pending"] >= COMMIT_EVERY:
        conn.commit()
        stats["pending"] = 0


# --------------------------------------------------------------------------
# Run
# --------------------------------------------------------------------------

def new_stats():
    return {
        "processed": 0,
        "with_accounts": 0,
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "rolled_back": 0,
        "pending": 0,
    }


def process(orgnrs, session, limiter, conn, cur, args, stats):
    total = len(orgnrs)
    started = time.monotonic()

    for orgnr in orgnrs:
        outcome, payload = fetch_accounts(session, orgnr, limiter)
        stats["processed"] += 1

        if outcome == "none":
            stats["skipped"] += 1
        elif outcome == "error":
            stats["errors"] += 1
            if stats["errors"] <= MAX_ERROR_LOG:
                log(f"ERROR: orgnr={orgnr}: {payload}")
        else:
            rows = []
            for entry in payload:
                row = map_statement(orgnr, entry)
                if row is None:
                    stats["skipped"] += 1
                    continue
                rows.append(row)

            if not rows:
                stats["skipped"] += 1
            else:
                stats["with_accounts"] += 1
                if args.dry_run:
                    for row in rows:
                        print(json.dumps(row, ensure_ascii=False, default=json_default))
                else:
                    upsert_rows(conn, cur, rows, stats)

        if stats["processed"] % PROGRESS_EVERY == 0:
            elapsed = max(time.monotonic() - started, 0.001)
            rate = stats["processed"] / elapsed
            remaining = max(total - stats["processed"], 0)
            eta_min = (remaining / rate) / 60 if rate > 0 else 0
            log(
                f"  {stats['processed']:,}/{total:,} orgnrs | with accounts {stats['with_accounts']:,} "
                f"| inserted {stats['inserted']:,} | updated {stats['updated']:,} "
                f"| skipped {stats['skipped']:,} | errors {stats['errors']:,} "
                f"| {rate:.1f} req/s | ETA {eta_min:.1f} min"
            )

    if conn is not None and stats["pending"]:
        conn.commit()
        stats["pending"] = 0


def run(args, session, stats):
    limiter = RateLimiter(args.rate)

    # A single orgnr in dry-run mode never touches the database.
    if args.orgnr and args.dry_run:
        log(f"Dry run for a single orgnr {args.orgnr}, no database used.")
        process([args.orgnr], session, limiter, None, None, args, stats)
        return

    if args.orgnr:
        orgnrs = [args.orgnr]
        database_url = require_database_url()
    else:
        database_url = require_database_url()
        orgnrs = load_orgnrs(database_url, args.skip_existing, args.limit)

    if not orgnrs:
        log("Nothing to do, worklist is empty.")
        return

    if args.dry_run:
        log("Dry run: rows are printed as JSON lines, nothing is written.")
        process(orgnrs, session, limiter, None, None, args, stats)
        return

    log(f"Fetching accounts for {len(orgnrs):,} orgnrs at {args.rate} req/s.")
    with psycopg.connect(database_url, application_name="brreg-regnskap") as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            try:
                process(orgnrs, session, limiter, conn, cur, args, stats)
            finally:
                if stats["pending"]:
                    conn.commit()
                    stats["pending"] = 0


def print_summary(stats):
    log("---- summary ----")
    log(f"orgnrs processed : {stats['processed']:,}")
    log(f"with accounts    : {stats['with_accounts']:,}")
    log(f"inserted rows    : {stats['inserted']:,}")
    log(f"updated rows     : {stats['updated']:,}")
    log(f"skipped          : {stats['skipped']:,}  (no accounts filed or unusable statement)")
    log(f"errors           : {stats['errors']:,}")
    if stats["rolled_back"]:
        log(f"rolled back      : {stats['rolled_back']:,}  (uncommitted rows lost to an aborted transaction)")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Load annual accounts from Regnskapsregisteret into the financials table.",
    )
    parser.add_argument("--orgnr", help="Fetch a single orgnr instead of the list from companies.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print mapped rows as JSON lines instead of writing. Combined with --orgnr no DB is used.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Stop after N orgnrs.")
    parser.add_argument("--rate", type=float, default=5.0, help="Requests per second (default 5.0).")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip orgnrs that already have a financials row for year >= current year - 1.",
    )
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be a positive integer")
    if args.rate <= 0:
        parser.error("--rate must be greater than 0")
    if args.orgnr:
        args.orgnr = args.orgnr.strip().replace(" ", "")
        if not args.orgnr.isdigit():
            parser.error("--orgnr must be digits only")
    return args


def main(argv=None):
    load_dotenv(BULK_DIR / ".env")
    args = parse_args(argv)
    session = build_session()
    stats = new_stats()
    try:
        run(args, session, stats)
    except KeyboardInterrupt:
        log("Interrupted by user.")
        print_summary(stats)
        return 130
    except psycopg.Error as exc:
        log(f"FATAL: database error: {exc}")
        return 1
    except requests.RequestException as exc:
        log(f"FATAL: HTTP error: {exc}")
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
