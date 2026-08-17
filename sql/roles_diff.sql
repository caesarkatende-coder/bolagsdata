-- roles_diff.sql
-- Referenskopia av SQL:en i n8n-flödet "BRREG 2 - Roller", noden "Apply role diff".
-- Filen körs inte av något schema, den finns för att kunna läsas, testas och
-- felsökas i psql eller Supabase SQL Editor utan att öppna n8n.
--
-- Parametrar (skickas som options.queryReplacement från noden "Build diff input"):
--   $1 = desired   jsonb-array med objekt
--                  {company_orgnr, role_type, person_name, birth_date, holder_orgnr}
--                  Önskat läge: alla aktiva roller enligt BRREG just nu.
--   $2 = processed jsonb-array med orgnr som strängar.
--                  Bara orgnr som faktiskt kunde läsas av (HTTP 200 eller 404).
--                  Orgnr med 5xx utesluts medvetet, se noten om serverfel nedan.
--
-- Testkörning för hand:
--   select ... (klistra in uttrycket nedan och byt $1 / $2 mot literaler)
--     $1 -> '[{"company_orgnr":"999999999","role_type":"styreleder",
--              "person_name":"Ola Nordmann","birth_date":"1970-01-01",
--              "holder_orgnr":null}]'
--     $2 -> '["999999999"]'
--
-- ---------------------------------------------------------------------------
-- Designval
-- ---------------------------------------------------------------------------
-- 1. ETT enda uttryck = atomärt. Allt (skapa personer, stänga roller, öppna
--    roller) sker i samma statement och därmed i samma transaktion. Antingen
--    landar hela diffen eller ingenting. Ingen mellanliggande körning kan se
--    ett halvfärdigt tillstånd.
--
-- 2. all_people-unionen är nödvändig. Rader som skrivs av en datamodifierande
--    CTE (ins_people) är INTE synliga för en vanlig läsning av samma tabell
--    inom samma statement, eftersom alla CTE:er ser samma ögonblicksbild av
--    databasen. Nya personer måste därför hämtas via RETURNING, och redan
--    befintliga personer via en separat select mot people. Unionen slår ihop
--    dem till en fullständig uppslagstabell.
--
-- 3. exists-vakten mot companies i "opened" undviker FK-brott. roles.orgnr har
--    en foreign key mot companies(orgnr). Ett orgnr kan dyka upp i
--    company_events innan bolaget hunnit skrivas till companies, och då skulle
--    en insert krascha hela flödet. Rollen hoppas i stället över och plockas
--    upp nästa gång bolaget ändras.
--
-- 4. Oförändrade roller varken stängs eller återöppnas. Villkoret i "closed"
--    stänger bara rader som saknas i önskat läge, och "opened" skriver bara
--    rader som inte redan är öppna. Det bevarar valid_from, så en styrelsepost
--    som suttit i fem år behåller sitt ursprungliga startdatum i stället för
--    att nollställas varje natt. Det är hela poängen: historiken byggs upp.
--
-- 5. Ett orgnr som finns i "touched" men saknar rader i "desired" får ALLA sina
--    öppna roller stängda. Det är precis vad HTTP 404 från BRREG betyder:
--    bolaget har noll registrerade roller.
--
-- 6. Serverfel (5xx) får aldrig hamna i $2. Om ett 5xx-svar räknades som
--    "behandlat" skulle diffen tolka det tomma svaret som "inga roller kvar"
--    och stänga hela styrelsen på ett bolag som mår bra. Filtreringen görs i
--    kodnoden, kommentaren står här som påminnelse.
--
-- 7. Ingenting raderas. valid_to sätts, rader ligger kvar. roles blir därmed en
--    fullständig tidslinje och inte en ögonblicksbild.
--
-- 8. Idempotent. Kör samma indata två gånger i rad och andra körningen gör
--    varken stängningar eller öppningar. Vilar på det partiella unika indexet
--    roles_active_uniq (orgnr, role_type, person_id, holder_orgnr) nulls not
--    distinct where valid_to is null, plus "on conflict do nothing" som
--    sista skyddsnät vid samtidiga körningar.
-- ---------------------------------------------------------------------------

with input as (
  -- Rå indata från BRREG, normaliserad. distinct städar bort dubbletter som
  -- uppstår när samma person har samma roll i flera rollgrupper.
  -- nullif(btrim(...), '') gör tomma strängar till null, så att jämförelserna
  -- med "is not distinct from" längre ned beter sig konsekvent.
  select distinct
    x.company_orgnr,
    x.role_type,
    nullif(btrim(x.person_name), '') as person_name,
    x.birth_date::date as birth_date,
    nullif(btrim(x.holder_orgnr), '') as holder_orgnr
  from jsonb_to_recordset($1::jsonb)
    as x(company_orgnr text, role_type text, person_name text, birth_date text, holder_orgnr text)
),
touched as (
  -- Orgnr som får diffas den här körningen. Allt utanför denna lista lämnas orört.
  select value as orgnr from jsonb_array_elements_text($2::jsonb)
),
ins_people as (
  -- Skapa personer som inte redan finns. people har
  -- unique nulls not distinct (name, birth_date), så personer utan födelsedatum
  -- dubbleras inte.
  insert into people (name, birth_date)
  select distinct person_name, birth_date from input where person_name is not null
  on conflict (name, birth_date) do nothing
  returning person_id, name, birth_date
),
all_people as (
  -- Nyss skapade personer (via RETURNING) plus redan befintliga (via select).
  -- Se designval 2: båda delarna behövs för att få en komplett uppslagning.
  select person_id, name, birth_date from ins_people
  union
  select p.person_id, p.name, p.birth_date
  from people p
  where exists (
    select 1 from input i
    where i.person_name = p.name and i.birth_date is not distinct from p.birth_date
  )
),
resolved as (
  -- Önskat läge med person_id ifyllt. Bolagsroller (holder_orgnr) får person_id null.
  select i.company_orgnr, i.role_type, i.holder_orgnr, ap.person_id
  from input i
  left join all_people ap
    on i.person_name is not null
   and ap.name = i.person_name
   and ap.birth_date is not distinct from i.birth_date
),
closed as (
  -- Stäng aktiva roller som inte längre finns i önskat läge.
  -- "is not distinct from" krävs eftersom person_id och holder_orgnr är null
  -- i varannan rad, och null = null aldrig är sant.
  update roles r
  set valid_to = current_date
  where r.valid_to is null
    and r.orgnr in (select orgnr from touched)
    and not exists (
      select 1 from resolved d
      where d.company_orgnr = r.orgnr
        and d.role_type = r.role_type
        and d.person_id is not distinct from r.person_id
        and d.holder_orgnr is not distinct from r.holder_orgnr
    )
  returning r.role_id
),
opened as (
  -- Öppna roller som är nya. valid_from får default current_date, valid_to null.
  -- exists-vakten mot companies: se designval 3.
  insert into roles (orgnr, person_id, holder_orgnr, role_type)
  select d.company_orgnr, d.person_id, d.holder_orgnr, d.role_type
  from resolved d
  where exists (select 1 from companies c where c.orgnr = d.company_orgnr)
    and not exists (
      select 1 from roles r
      where r.orgnr = d.company_orgnr
        and r.valid_to is null
        and r.role_type = d.role_type
        and r.person_id is not distinct from d.person_id
        and r.holder_orgnr is not distinct from d.holder_orgnr
    )
  on conflict do nothing
  returning role_id
)
-- Kvitto som syns i n8n-körningen.
select
  (select count(*) from closed) as roles_closed,
  (select count(*) from opened) as roles_opened;
