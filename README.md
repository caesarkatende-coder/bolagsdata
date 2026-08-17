# BRREG-pipeline: Norge till Supabase

Arkitekturen i en mening: Python-script för engångs-bulken, n8n (lokalt) för de löpande deltona, Supabase Postgres som sanning, append-only för historiken.

Alla datakällor är gratis och kräver ingen API-nyckel. Byggd mot n8n 2.34.6.

## Status: driftsatt 2026-08-17

Allt nedan är redan gjort och verifierat mot riktig data:

- Supabase-projektet **bolagsdata** (ref `apgfltdiaqnieeicklls`, eu-north-1, org S&P). Schema kört. Anslutning via session pooler, credentials i `bulk/.env` (gitignorerad).
- Bulk-loaden klar: 1 170 472 bolag på 4 minuter, 0 fel, raw ifylld till 100 %.
- Alla 5 flöden importerade i n8n, kopplade till credentialen **Supabase Postgres** och error-workflown, och aktiverade. n8n-API-nyckeln `brreg-pipeline` ligger i `bulk/.env`.
- Flow 1, 2 och 3 testkörda skarpt: 200 händelser, 341 roller/257 personer, 28 bokslut. Cursors skrivna. Flow 4 körd manuellt: augusti-snapshot med 1 170 510 rader.
- Credential-detalj: "Ignore SSL Issues" är på, eftersom Supabase-poolerns CA-kedja inte verifieras av Nodes pg-driver (samma krypteringsnivå som psycopg:s `sslmode=require`).

Kvarstående manuellt: mejla Skatteetaten (utkast längst ner).

**Driftvarning, compute:** projektet kör på minsta compute-storleken (micro). Månadssnapshotten skriver 1,17 M rader i en transaktion, vilket vid driftsättningen tryckte omkull instansen i cirka 10 minuter (crash recovery, datat committades korrekt innan). Räkna med samma sak den 1:a varje månad kl 05:00, eller uppgradera compute till small i Supabase-dashboarden om det stör.

## Katalogstruktur

```
sql/schema.sql                  Datamodellen. Körs först, i Supabase SQL Editor.
sql/roles_diff.sql              Referenskopia av roll-diffens SQL (samma sats som i Flow 2).
bulk/bulk_load.py               Flow 0: engångs bulk-load av ~1,1 M bolag.
bulk/regnskap_batch.py          Flow 3 batchläge: årlig helgkörning av bokslut.
bulk/ownership_load.py          Flow 4: laddar aksjonärregister-CSV från Skatteetaten.
bulk/requirements.txt           Python-beroenden.
bulk/.env.example               Mall för DATABASE_URL.
n8n/flow1_daily_updates.json    BRREG 1: dagliga ändringar (kärnflödet), 06:00.
n8n/flow2_roles.json            BRREG 2: roller med historik, 06:45.
n8n/flow3_regnskap_weekly.json  BRREG 3: bokslut veckovis, söndag 07:00.
n8n/flow4_monthly_snapshot.json BRREG 4: månadssnapshot, 1:a varje månad 05:00.
n8n/error_workflow.json         BRREG Error Handler: gemensam felhantering.
```

## Förutsättningar

- Supabase-projekt (Postgres 15+). OBS lagring: `raw`-kolumnen för 1,1 M bolag blir cirka 2-4 GB. Free-planen (500 MB) räcker inte: använd Pro, eller kör bulken med `--no-raw` (delta-flödet sparar ändå raw för alla bolag som ändras framåt).
- n8n lokalt (körs redan på `localhost:5678`).
- Python 3.13+ med venv.

## Vecka 1, steg för steg

### Dag 1: schema + bulk

1. Skapa Supabase-projektet och kör hela `sql/schema.sql` i SQL Editor. Idempotent, säkert att köra om.
2. Sätt upp Python-miljön och testa mot riktiga API:t utan databas:

```bash
cd ~/Developer/Data && python3 -m venv .venv && .venv/bin/pip install -r bulk/requirements.txt
```

```bash
cd ~/Developer/Data && .venv/bin/python bulk/bulk_load.py --dry-run --limit 20
```

3. Kopiera `bulk/.env.example` till `bulk/.env` och fyll i `DATABASE_URL` (session pooler, port 5432, är det säkra valet; se kommentarerna i filen).
4. Kör bulken (20-40 min):

```bash
cd ~/Developer/Data && .venv/bin/python bulk/bulk_load.py
```

Verifiera i SQL Editor: `select count(*), status from companies group by 2;`

### Dag 2: Flow 1 i n8n

1. Skapa credentialen i n8n: Credentials, New, Postgres. Namnge den exakt **Supabase Postgres**. Värden från Supabase (Settings, Database): host, database `postgres`, user, lösenord, port 5432, SSL på. Funkar inte direktanslutningen lokalt (den är IPv6): använd session pooler-hosten.
2. Importera `n8n/flow1_daily_updates.json` (Workflows, Import from File). Öppna varje Postgres-nod och välj credentialen (import kan inte bära med sig credential-ID:n).
3. Importera `n8n/error_workflow.json`. Öppna Flow 1, Settings, Error Workflow, välj "BRREG Error Handler". Koppla Slack- eller mailnotis i error-workflown när du vill.
4. Testkör Flow 1 manuellt (Execute workflow). Första körningen använder automatiskt `?dato=` (gårdagens datum) eftersom cursorn seedats tom av schemat. Verifiera sedan:

```bash
echo "select count(*) from company_events; select value, updated_at from sync_state;" 
```

(kör i SQL Editor). Aktivera flödet.

### Dag 3: Flow 2, roller

1. Importera `n8n/flow2_roles.json`, välj credential i Postgres-noderna, koppla error-workflown, aktivera.
2. Engångsbackfill för prioriterade segment: i flödet finns en avstängd nod "Backfill segment (manuell)". Justera dess query (t.ex. alla AS i utvalda NACE-koder), koppla in den istället för "Changed orgnrs", kör manuellt en gång, koppla tillbaka och stäng av noden igen. Vid ~5 anrop/sek tar 10 000 bolag cirka 35 min.

### Dag 4: Flow 3 + 4

1. Importera `n8n/flow3_regnskap_weekly.json` och `n8n/flow4_monthly_snapshot.json`, credential + error workflow + aktivera.
2. Boka den årliga batchkörningen (sep-okt när boksluten strömmar in):

```bash
cd ~/Developer/Data && .venv/bin/python bulk/regnskap_batch.py --skip-existing
```

Testa mappningen när som helst utan databas:

```bash
cd ~/Developer/Data && .venv/bin/python bulk/regnskap_batch.py --orgnr 923609016 --dry-run
```

### Dag 5: Aksjonärregisteret

1. Mejla Skatteetaten (utkast nedan). Leveranstid varierar, starta tidigt. Begär historiska år i samma ärende.
2. När CSV:n kommer:

```bash
cd ~/Developer/Data && .venv/bin/python bulk/ownership_load.py /sökväg/till/aksjonarregister-2025.csv --year 2025
```

Kolumnmappningen ligger som konstanter överst i scriptet och justeras lätt om filens rubriker avviker. Diff-queryn mellan två år (= transaktionsdata) finns i pipeline-specen, avsnitt 6.

## Drift (cirka 15 min per vecka)

- n8n, Executions: röda körningar? Error-workflown fångar dem också.
- `select key, value, updated_at from sync_state;` : båda cursorerna ska ha färska `updated_at`.
- `select count(*) from company_events where event_time > now() - interval '7 days';` : normalt några tusen per dag.
- Datorn avstängd en morgon? Ingen fara. Cursorn styr, så nästa körning hämtar allt som hänt sedan sist. Kör manuellt med Execute workflow om du inte vill vänta till nästa dag.

## Avvikelser från pipeline-specen (medvetna fixar)

1. **Snapshot-buggen:** specens `like companies including all` kopierar PK(orgnr), vilket kraschar insert månad 2. Fixat med composite PK `(orgnr, snapshot_month)` plus `on conflict do nothing`.
2. **`people`:** `unique nulls not distinct` så att personer utan födelsedatum inte dubbleras (PG15+).
3. **`roles`:** partiellt unikt index på aktiva roller gör roll-diffen idempotent.
4. **Cursor-seed:** `sync_state` seedas tom av schemat, så första Flow 1-körningen använder `?dato=` automatiskt. Specens "dag 2, sätt cursorn manuellt" behövs inte.
5. **Cursorn sparas som max oppdateringsid + 1** (API-parametern är "större än eller lika med"). Överlapp är ändå ofarligt tack vare event_id-PK och `do nothing`.
6. **Loop + Wait ersatt** med HTTP-nodens inbyggda batchning (5 anrop/sek): samma throttling, färre noder.
7. **Flow 2 triggas inte av Flow 1** utan har egen cursor (`brreg_roles_event_id`) mot `company_events`. Workflow-ID-referenser överlever inte import; resultatet är detsamma och flödena är oberoende.
8. **`Fjerning` hanteras** utöver specens `Sletting` (enheten kan vara helt borta ur API:t; då uppdateras bara status, ingen insert utan namn).
9. **Rollnamn är strukturerade** i API:t (fornavn/mellomnavn/etternavn), inte en platt sträng som specen antyder. Flow 2 slår ihop dem.
10. **psycopg v3** istället för psycopg2 (`execute_values` finns inte där; batchad `executemany` används). Krav från Python 3.14.

## Nästa land

Schemat, historiken och snapshotsen är landsagnostiska (`country`-kolumnen). För Danmark (CVR) och UK (Companies House) skrivs bara nytt per land: HTTP-endpointen och mappningsfunktionen i Flow 1 (noden "Map to companies") plus motsvarigheten i bulk-scriptet.

## Mailutkast till Skatteetaten (norska)

> **Emne:** Innsynsbegjaering: uttrekk fra aksjonaerregisteret
>
> Hei,
>
> Jeg ber med dette om innsyn i opplysninger fra aksjonaerregisteret (innrapportert via RF-1086), i form av et maskinlesbart uttrekk (CSV) per selskap og aksjonaer: organisasjonsnummer, selskapsnavn, aksjeklasse, aksjonaerens navn, fodselsaar/organisasjonsnummer, landkode, antall aksjer og totalt antall aksjer i selskapet.
>
> Jeg onsker uttrekk for siste tilgjengelige inntektsaar, samt tidligere aarganger sa langt tilbake som mulig, i samme leveranse. Levering via nedlastingslenke eller e-post fungerer fint.
>
> Vennlig hilsen
> Caesar Katende

Skicka via kontaktformuläret på skatteetaten.no eller den innsynsadress de anger där (verifiera adressen innan du skickar, den ändras ibland).
