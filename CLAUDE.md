# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Nordic company-data pipeline. Country 1 is Norway via BRREG's open APIs, live in production since 2026-08-17: Supabase Postgres project `bolagsdata` (ref `apgfltdiaqnieeicklls`, eu-north-1, org S&P) holds ~1.17M companies; a local n8n (2.34.6, via npx, localhost:5678) runs the recurring flows. Architecture in one sentence: Python for the one-time bulk load, n8n for recurring deltas, Postgres as truth, append-only for history.

`README.md` (Swedish) is the operational runbook: deployment status, monitoring routine, and the 10 documented deviations from the original pipeline spec. Read its "Status" section before assuming anything needs setting up; it is already deployed.

## Commands

Everything Python runs through the project venv (already provisioned):

```bash
.venv/bin/python bulk/bulk_load.py --dry-run --limit 20        # live smoke test against BRREG, no DB
.venv/bin/python bulk/regnskap_batch.py --orgnr 923609016 --dry-run   # financials mapping vs a real response
.venv/bin/python bulk/ownership_load.py <csv> --year 2025 --dry-run   # parse-only check of shareholder CSV
.venv/bin/python -m py_compile bulk/*.py                       # sanity check; there is no test suite or linter
```

- Real (non-dry) runs read `DATABASE_URL` from `bulk/.env`, which also holds `DB_PASSWORD` and `N8N_API_KEY` (key label `brreg-pipeline`, expires 2026-09-16). Never print that file's contents; load values via dotenv or shell variables.
- DB inspection: short psycopg heredoc scripts, pattern `psycopg.connect(dotenv_values('bulk/.env')['DATABASE_URL'])`. The Supabase CLI is authenticated on this machine.
- n8n management: REST at `http://localhost:5678/api/v1` with header `X-N8N-API-KEY`. The public API cannot update credentials and cannot trigger manual executions; both require the UI (Caesar's Chrome is logged in to n8n and stays logged in).

## Architecture

### Postgres (sql/schema.sql)

- `companies`: current state per orgnr, upserted; `first_seen` is never overwritten. `status` is one of `aktiv | konkurs | under_avveckling | upphörd`.
- `company_events`: append-only log; `event_id` IS BRREG's `oppdateringsid` (PK) and inserts use `on conflict do nothing`, which makes every flow re-runnable.
- `people` + `roles`: temporal (valid_from/valid_to). Rows get closed, never deleted; a partial unique index on active roles makes the role diff idempotent.
- `financials` (orgnr, year), `ownership` (one generation per `source_year`, from yearly Skatteetaten CSVs), `companies_snapshot` (monthly frame, PK (orgnr, snapshot_month)).
- `sync_state`: cursors `brreg_oppdateringsid` and `brreg_roles_event_id`. Empty string means "first run" and makes flow 1 fall back to `?dato=<yesterday>`. Cursors store max id + 1 and are written LAST in each flow, so a crashed run is safe to re-run.

### n8n flows

The files in `n8n/` are the canonical definitions; the running copies live in n8n's SQLite. When changing a flow, keep the two in sync (edit JSON, re-import via API, or edit in UI and export back). Current workflow ids: `GET /api/v1/workflows`.

- **BRREG 1 - Dagliga ändringar** (06:00): cursor, oppdateringer feed (HAL pagination), append events, dedupe orgnr, enrich via `/enheter/{orgnr}`, upsert companies (status-only path for Sletting/Fjerning), write cursor.
- **BRREG 2 - Roller** (06:45): its own cursor against `company_events`, deliberately NOT triggered by flow 1 (workflow-id references do not survive import). The diff runs as ONE atomic CTE statement, annotated copy in `sql/roles_diff.sql`. Contains a disabled "Backfill segment (manuell)" node for one-off segment backfills.
- **BRREG 3 - Regnskap veckovis** (Sun 07:00): AS/ASA with events in the last 8 days; 404 means no accounts filed and is normal, not an error.
- **BRREG 4 - Månadssnapshot** (1st, 05:00): see operational facts below.
- **BRREG Error Handler**: every flow's `settings.errorWorkflow` points here; no notification channel wired yet.

All flows: timezone Europe/Oslo, Postgres credential named **Supabase Postgres** with "Ignore SSL Issues" ON (node-postgres rejects the Supabase pooler's CA chain; encryption level equals psycopg's `sslmode=require`), and ~5 req/s BRREG throttle via HTTP-node batching (batchSize 5 / batchInterval 1000).

### Conventions that prevent regressions

- The BRREG bulk download requires `Accept: application/vnd.brreg.enhetsregisteret.enhet.v2+gzip;charset=UTF-8`; the unversioned media type now returns 406.
- ijson yields `decimal.Decimal`; all entity serialization in `bulk_load.py` must go through its `json_dumps()` helper and `Jsonb(..., dumps=json_dumps)`. A bare `json.dumps` or bare `Jsonb()` will crash on real data.
- n8n Postgres nodes take parameters as a single expression returning a JS array (`{{ [ ... ] }}`), never string interpolation (company names contain apostrophes). JSON payloads travel as one `::jsonb` parameter.
- Enrichment HTTP nodes use neverError + fullResponse, and downstream Code nodes pair responses to inputs BY INDEX; preserve 1:1 item order if editing them.
- The canonical enhet mapping (org_form AS to aktiebolag etc, status derivation) lives in TWO places that must stay in sync: `map_enhet()` in `bulk/bulk_load.py` and the "Map to companies" Code node in flow 1.

### Operational facts

- The monthly snapshot writes 1.17M rows in one transaction and pushed the micro compute instance into ~10 min of crash recovery on deploy day (data committed correctly first). Expect the same on the 1st each month, or upgrade compute to small.
- Pending: Skatteetaten shareholder-register CSV (email draft at the bottom of README.md); load it with `ownership_load.py` when it arrives.
- Next country (Denmark CVR, UK Companies House): the schema is country-agnostic (`country` column). Only a new endpoint plus mapping layer is written per country: the bulk mapping function and flow 1's mapping node.
- `data/` holds the downloaded 209 MB BRREG dump and `supabase/.temp/` is Supabase CLI state; both are gitignored and disposable.
