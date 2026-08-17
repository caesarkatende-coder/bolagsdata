-- BRREG -> Supabase datamodell
-- Kors i Supabase SQL Editor (eller via psql). Idempotent: saker att kora om.
-- Bygger pa Caesars pipeline-spec, med sex fixar (se README, avsnitt "Avvikelser fran specen").

-- Nulage per bolag (uppdateras via upsert)
create table if not exists companies (
  orgnr         text primary key,
  name          text not null,
  org_form      text,            -- kanoniskt: 'aktiebolag','enskild_firma','filial'...
  org_form_raw  text,            -- originalvarde: 'AS','ENK','NUF'...
  nace_code     text,            -- naringskod, t.ex. '43.220'
  founded       date,
  employees     int,
  municipality  text,
  address       jsonb,
  status        text default 'aktiv',   -- 'aktiv','konkurs','under_avveckling','upphörd'
  country       char(2) default 'NO',
  website       text,
  raw           jsonb,           -- hela originalsvaret
  first_seen    timestamptz default now(),
  last_updated  timestamptz default now()
);

-- Append-only handelselogg (raderas ALDRIG, detta ar historiken)
create table if not exists company_events (
  event_id    bigint primary key,   -- BRREG:s oppdateringsid
  orgnr       text,
  event_type  text,                 -- 'Ny','Endring','Sletting','Fjerning'
  event_time  timestamptz,
  raw         jsonb
);
create index if not exists company_events_orgnr_idx on company_events (orgnr);
create index if not exists company_events_time_idx  on company_events (event_time);

-- Fix 2: nulls not distinct sa att personer utan fodelsedatum inte dubbleras
create table if not exists people (
  person_id   uuid primary key default gen_random_uuid(),
  name        text not null,
  birth_date  date,
  country     char(2) default 'NO',
  unique nulls not distinct (name, birth_date)
);

-- Roller med giltighetstid (stang rader, skriv aldrig over)
create table if not exists roles (
  role_id      uuid primary key default gen_random_uuid(),
  orgnr        text references companies(orgnr),
  person_id    uuid references people(person_id),
  holder_orgnr text,               -- nar rollen innehas av ett bolag (t.ex. revisor)
  role_type    text,               -- 'styreleder','styremedlem','daglig_leder','revisor'...
  valid_from   date default current_date,
  valid_to     date                -- null = aktiv just nu
);
-- Fix 3: unikt index pa aktiva roller gor roll-diffen idempotent
create unique index if not exists roles_active_uniq
  on roles (orgnr, role_type, person_id, holder_orgnr) nulls not distinct
  where valid_to is null;
create index if not exists roles_active_orgnr_idx on roles (orgnr) where valid_to is null;

create table if not exists financials (
  orgnr      text references companies(orgnr),
  year       int,
  currency   char(3) default 'NOK',
  revenue    numeric,
  ebit       numeric,
  net_result numeric,
  equity     numeric,
  debt       numeric,
  assets     numeric,
  raw        jsonb,
  primary key (orgnr, year)
);

-- Aktieagare per registerar (en argang = en filmruta; diffen mellan ar = transaktioner)
create table if not exists ownership (
  company_orgnr    text,
  owner_name       text,
  owner_orgnr      text,            -- ifyllt nar agaren ar ett bolag
  owner_birth_year int,             -- ifyllt nar agaren ar en person
  share_class      text default 'ordinär',
  shares           numeric,
  total_shares     numeric,
  source_year      int,             -- vilket aksjonaerregister-ar raden kommer fran
  primary key (company_orgnr, owner_name, share_class, source_year)
);

-- Cursor-tabell sa flodena vet var de slutade
create table if not exists sync_state (
  key        text primary key,
  value      text,
  updated_at timestamptz default now()
);

-- Fix 4: seed med tomma cursors. Tomt varde = forsta korningen anvander ?dato=<igar>
-- istallet for oppdateringsid (spec §8 dag 2 sker darmed automatiskt).
insert into sync_state (key, value) values ('brreg_oppdateringsid', '')
  on conflict (key) do nothing;
insert into sync_state (key, value) values ('brreg_roles_event_id', '')
  on conflict (key) do nothing;

-- Fix 1: snapshot-tabell. Specens "like companies including all" hade kopierat
-- PK(orgnr) och kraschat vid manad 2. Har: composite PK (orgnr, snapshot_month).
create table if not exists companies_snapshot (
  like companies including defaults,
  snapshot_month date not null,
  primary key (orgnr, snapshot_month)
);
