# Transaction Cleaning Pipeline

_This learning project is realized as part of the Onboarding at X-Tends' Machine Learning Team by Joseph Am-Makhlouf_

This file covers what the pipeline is, how to run it, and what it produce. The reasoning behind each cleaning decision - lives in [`ARCHITECTURE.md`](ARCHITECTURE.md), one section per stage.

The project was originally organized by task, but for the sake of an easier understand of the repo, it will be explained as a one project:

# Task 2 - Reproducible Pipeline

Ingestion, validation, cleaning and persistence of the card transaction
extract. The output is `cleaned_transactions`: one typed, deduplicated row per
transaction, with every derived figure carrying a status that says how far it
can be trusted.

## What changed since last presentation

All of it concerns the running balance, which was the weakest column in the
previous version.

- **The mover is detected, not assumed.** The extract does not use one column
  throughout: for its first 188,469 rows the stated balance moves by
  `TXN_AMOUNT_CLEANED` (the account's own currency), and from row 188,470 on it
  moves by `BILLING_AMOUNT`. Nothing in the file announces the change. The
  previous version assumed the first convention everywhere, which is why the
  entire second half of the extract came out `CONTRADICTED`.
- **The seam is found by segmentation, not configured.** Every pair of
  consecutive stated balances is asked which candidate mover explains the step
  between them, and the evidence is segmented in sequence order with a
  per-change penalty. A hardcoded row number is a fact about *this* extract and
  would be carried silently into the next one; the detector handles a file with
  no seam, one seam, or several.
- **Reconstruction now runs in both directions.** Forward from the last trusted
  balance before a row, and backward from the first trusted one after it. Where
  both apply, agreement is the check; where only one does, the arithmetic is no
  less exact for having nothing to confirm it.
- **A figure is stated wherever the arithmetic reaches, with its provenance.**
  The old design withheld any value it could not prove. The principle survives,
  but a withheld value is not neutral downstream - it is a null a feature build
  must either drop or impute, and both are decisions made further from the
  evidence than this stage is. `RUNNING_BALANCE_STATUS` now carries one of
  `OBSERVED`, `DERIVED`, `FORWARD_DERIVED`, `BACKWARD_DERIVED`, `UNVERIFIED`,
  `CONTRADICTED` or `UNAVAILABLE`, and only `UNAVAILABLE` has no number beside
  it - no trusted anchor is reachable from that row in either direction.
- **The currency of the answer is published.** A balance rebuilt from the
  native mover is in `TXN_CCY`; one rebuilt from the billing mover is in USD.
  `RUNNING_BALANCE_CURRENCY` says which on every row that carries a figure, and
  `RUNNING_BALANCE_NORMALIZED` values it in USD so anything aggregating across
  accounts is adding comparable numbers.
- **The reconcile tolerance is evidence-based.** `0.02` rather than `0.01`,
  because the billing-regime chains reconcile on 87.8% of adjacent pairs at
  `0.01` and 100% at `0.02`, and the figure does not move again at `0.05`, `1.0`
  or `5.0`. That plateau is what says the rows between the two thresholds are
  rounding residue, not error.

The reasoning behind each of these lives in
[`ARCHITECTURE.md`](ARCHITECTURE.md); the numbers they produce are in
[Results on the source file](#results-on-the-source-file).

## Quick start

Create the virtual environment and install the dependencies.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
```

On macOS or Linux the activation line is `source .venv/bin/activate`; everything after it is identical.

Run the pipeline against the bundled source file.

```bash
make run
```

That runs the **Spark** path: it reads `data/raw/forecast_balance_data.csv`, cleans it through the eleven ported stages, and upserts the result into Postgres. It needs the containers up — `make verify` checks Java, Spark, Postgres and Kafka and names the fix for anything that is down.

Or read, clean and report while writing nothing at all:

```bash
python main.py --dry-run
```

There is one engine. The pipeline was built twice — once in pandas as the Stage 1 reference, once on Spark as the Stage 2 deliverable — and held to the same answers by a parity harness that ran both over the same sample and compared column for column. It passed on all eleven stages, which is what made the pandas half redundant: a second implementation kept only to be compared against is a second implementation to maintain. It has been removed, and Spark is the pipeline.

What survived is the vocabulary the two shared — the column names, the status values and a handful of pure functions — now in [`src/schema/`](src/schema/) and imported by the Spark stages. Every line of it is unchanged from the reviewed pandas modules it came out of.

Run the test suite.

```bash
make test
```
N.B.: Make sure to download 'make' beforehand. One way of doing that is running the following command in powershell:  
```bash
choco install make
```

Run the read-only profiler to inspect a file before cleaning it.

```bash
python -m eda.profiler
```

Both `make` targets are one-line wrappers, so the direct equivalents work identically on any shell.

```powershell
python main.py
python -m pytest -q
```

When the Spark run finishes it publishes a completion event to Kafka on `pipeline.run.completed.v1`, keyed by the run's `sync_job_id`. The event carries the source, the profile, the config fingerprint, the row counts and every per-stage total — enough that a consumer can act on it without reading the database, and enough that it can find the rows if it wants to. `--no-emit` writes to Postgres and announces nothing; `make kafka-topic` creates the topic, which does not auto-create on purpose.

Kafka carries the *event*, not the transactions. The rows go to Postgres.

Every run is identified by a `sync_job_id` derived from the source file's contents, not generated per run. Re-running the same file mints the same id and upserts the same rows to the same values, so a second run changes nothing a reader can observe — which is what makes the pipeline safe to re-run after a failure, and what will let a re-delivering Kafka consumer be a no-op.

`src/` holds the steps, the orchestrator and the report; `main.py` at the repo root is the entry point that composes them.

---

## Streaming one transaction, end to end

The batch path above cleans a *file*. This is the other way in: a row lands in
a table, an event says so, and a consumer cleans that row and writes it to the
cleaned table without anybody running the pipeline.

```mermaid
flowchart LR
    subgraph you["By hand"]
        INS["INSERT into raw_transactions<br/>psql, or scripts/seed_raw.py"]
        EMIT["scripts/dummy_producer.py --id 42<br/><i>stands in for a trigger / CDC / outbox</i>"]
    end

    subgraph pg1["Postgres"]
        RAWT[("raw_transactions<br/>id + 22 TEXT columns<br/>status PENDING")]
    end

    subgraph k["Kafka - docker, localhost:9092"]
        TOPIC{{"transactions.raw.ingested.v1<br/>key = id<br/>value = JSON event"}}
    end

    subgraph con["consumer.py - one long-lived Spark session"]
        POLL["poll"]
        DEC["ingest_events.decode<br/><i>refuse anything else</i>"]
        READ["raw.read via JDBC<br/><i>same frame read_csv makes</i>"]
        CLEAN["spark.pipeline.run<br/>11 stages"]
        WRITE["db.writer.write<br/>stage + upsert"]
        MARK["raw.mark CLEANED / FAILED"]
        COMMIT["commit offset<br/><b>last, always</b>"]
    end

    subgraph pg2["Postgres"]
        CLEANT[("cleaned_transactions")]
    end

    INS --> RAWT
    INS -.->|"returns id"| EMIT
    EMIT -->|"publish"| TOPIC
    TOPIC -->|"subscribe"| POLL
    POLL --> DEC --> READ
    RAWT -.->|"SELECT WHERE id IN"| READ
    READ --> CLEAN --> WRITE --> CLEANT
    WRITE --> MARK --> COMMIT
    MARK -.->|"status"| RAWT
```

The commit is the **last** step and it is the whole delivery guarantee.
Offsets are committed after Postgres has committed, never before, so a crash
redelivers a row rather than skipping one — and a redelivered row is a no-op,
because the write is an upsert and the job id is derived from the ids rather
than generated. At-least-once by construction, idempotent by design.

### The commands

Nine steps, no `make`. Every one of these is the direct command; the Makefile
targets are one-line wrappers around exactly these.

**1. Start the broker and the database.**

```bash
docker compose up -d
```

**2. Create both Kafka topics.** Auto-create is off on purpose, so a producer
aimed at a typo'd topic fails instead of quietly inventing one.

```bash
python -c "from src.kafka import producer, settings; b = settings.load(); [print('created' if producer.ensure_topic(b, t) else 'already there', t) for t in (b.topic, b.raw_topic)]"
```

**3. Create the tables** — `raw_transactions`, `cleaned_transactions` and the
staging mirror. Every statement is `IF NOT EXISTS`, so this is safe to repeat.

```bash
python -c "from src.db import migrate, settings; migrate.migrate(settings.load()); print('schema applied')"
```

**4. Check the stack is actually up.** Every failure names its own fix.

```bash
python -m scripts.verify_env
```

**5. Start the consumer, and leave it running.** This terminal is where the
cleaning is narrated, so give it its own window.

```bash
python consumer.py --batch-size 1
```

`--batch-size 1` means one Spark job per transaction, which is the clearest
thing to watch. Without it, a burst of ids is cleaned together.

**6. In a second terminal, put a transaction into the raw table.** This prints
the id the database allocated — the id is the whole product of this step,
because it is what travels on Kafka.

```bash
python -m scripts.seed_raw --count 1
```

Raise `--count` and vary `--offset` to seed a wider, more arbitrary sample;
some of those rows will already be clean, and the stages reporting nothing to
do over them is a real result. To do this step by hand instead, any `INSERT`
works — the consumer
cannot tell the difference:

```bash
docker exec -i cleaning-postgres psql -U postgres -d training -c "INSERT INTO raw_transactions (txn_id, txn_date_time, settle_date, txn_amount, txn_ccy, merchant_name, merchant_city, merchant_country, processing_code, processing_type) VALUES ('demo-txn-1', '2022-01-01 07:11:25', '03-Jan-22', '-231.37', 'USD', 'TRM:31659ZAATARWZEIT', 'BEYRUT', 'LB', '0', 'PURCHASE') RETURNING id;"
```

(The `-U` and `-d` values come from `.env`.)

**7. Announce it.** This is the dummy producer: it publishes the id and exits.

```bash
python -m scripts.dummy_producer --id 42
```

Use the id step 6 printed. `--ids 42,43` or `--ids 40-44` for several,
`--pending` to re-announce every row still PENDING — the ones never attempted — and
`--dry-run` to see the exact message without publishing it.

**8. Watch the first terminal.** The consumer picks the event up, names each
stage as it runs, and prints the batch's audit trail before it writes:

```
+- raw ids 21,22,46 ----------------------------------------
| [INGESTION    ] read raw_transactions          3 rows
| [NORMALIZATION] timestamps
| [ENRICHMENT   ] macro
| [DEDUPLICATION] duplicates
| [NORMALIZATION] codes
| [NORMALIZATION] amounts
| [DERIVATION   ] balance
| [NORMALIZATION] missing
| [ENRICHMENT   ] merchant
| [ENRICHMENT   ] geo
| [ENRICHMENT   ] mcc
| [VALIDATION   ] consistency
|     audit:
|       pipeline      input_rows = 3
|       timestamps    txn_ts.status[TIME_UNKNOWN] = 1
|       macro         INTEREST_RATE_INDEX.recovered = 1
|       duplicates    exact_duplicates_dropped = 0
|       codes         processing_type.disagrees_with_code = 3
|       balance       balance.unavailable = 3
|       mcc           confidence[HIGH] = 3
|       consistency   CODE_TYPE_MISMATCH = 3
|       pipeline      output_rows = 3
| [PERSISTENCE  ] upsert 3 -> cleaned_transactions       3 rows
+- 3 row(s) cleaned in 41.2s ------------------------
```

The `audit:` block is the record, and it is not optional: it is every step's
metrics over the finished frame — the same `CleaningReport` the batch run
writes as a sheet and the completion event carries — so nothing is coerced,
dropped or flagged without a line here saying so. It costs two Spark actions
for the whole batch, however many stages ran.

The stage lines above it are narration. `--trace` adds each stage's own
numbers and timings, evaluated at that point in the run; that costs a Spark
action *per stage* and holds a cached frame per stage on the driver, so it is
for watching one batch closely and not for a consumer left running. `--quiet`
suppresses narration and audit together, for a consumer whose output nobody
is reading.

Every fiftieth batch the consumer prints `recycling the Spark session ...` and
pauses for a few seconds. That is deliberate, and it is what keeps a driver
that never exits from ending its life unable to clean a single row — see
`renew_every` in `config/pipeline.yaml`, and ARCHITECTURE.md on why capping
Spark's retention was not enough on its own.

**9. Check what arrived.**

```bash
python -c "from src.db import raw, settings; import psycopg2; db = settings.load(); c = psycopg2.connect(db.dsn); cur = c.cursor(); cur.execute('SELECT txn_id_cleaned, merchant_name_cleaned, merchant_city_cleaned, txn_amount_cleaned, settle_date_cleaned FROM cleaned_transactions ORDER BY cleaned_at DESC LIMIT 5'); [print(r) for r in cur.fetchall()]"
```

For the same row before and after:

| `raw_transactions` | `cleaned_transactions` |
|---|---|
| `TRM:31659ZAATARWZEIT` | `ZAATAR W ZEIT` |
| `BEYRUT` | `BEIRUT` |
| `03-Jan-22` | `2022-01-03` |
| `""` (blank balance) | `NULL` |

And the raw row now records what happened to it:

```bash
python -c "from src.db import raw, settings; db = settings.load(); print(raw.fetch(db, [42])[0][-1])"
```

### What happens when something goes wrong

| Situation | What the consumer does |
|---|---|
| A row will not clean | Marks it `FAILED` with the reason in `last_error`, commits, carries on. One bad row must not stop the other thousand. |
| A *batch* fails | Retries the batch one row at a time, so the failure lands on the row that caused it and the others are still cleaned. |
| The id names no row | Reports it and moves on — in under a second, because the existence check runs before any Spark work. |
| A message is not one of ours | Refused by name, counted, and appended to `data/audit_trail/undecodable.jsonl` with its bytes and its offset — it has no id, so `raw_transactions.status` has no row to mark for it. |
| The consumer was down | Nothing is lost; the messages wait on the topic. It resumes at its committed offset on restart. |
| A row was missed entirely | `python -m scripts.dummy_producer --pending` re-announces everything still PENDING. |
| A row failed and the cause is fixed | Re-announce it by id: `--ids 42`. `--pending` will not pick it up — a FAILED row was attempted, and re-emitting every known-broken row on each recovery run would just fail them all again. `last_error` says which ids and why. |
| The same row arrives twice | Cleaned again, written again, same values under the same job id — a second pass changes nothing a reader can observe. |

Every *why* behind the above — the all-TEXT landing table, the id-only message,
the commit ordering, the failure policy, the thread count — is in
[The streaming path](ARCHITECTURE.md#the-streaming-path).

### The five balance columns

The `balance` stage publishes five columns rather than one, because a single
column cannot answer the questions a reader has: *what is the balance*, *how
much is that figure worth trusting*, *in what currency*, and — where the
source's own anchors disagree — *by how much*.

| Column | What it is |
| --- | --- |
| `running_balance_filled` | The reconstructed balance, in the denomination the ledger actually used. Null only where the status says `UNAVAILABLE` |
| `running_balance_status` | How that figure was arrived at, and how far it can be trusted |
| `running_balance_currency` | Which denomination that is |
| `running_balance_normalized` | The same balance valued in USD |
| `running_balance_discrepancy` | On a `CONTRADICTED` row, the signed gap between the forward and backward reconstructions. Null elsewhere |

`running_balance_status` is not optional reading. The balance column states a
figure wherever the arithmetic reaches an anchor — 265,195 of 265,195 rows on
this extract — so the status is the only thing separating a figure two
independent claims agree on (`OBSERVED`, `DERIVED`: 97.5% of rows) from one
reconstructed in a single direction (`FORWARD_DERIVED`, `BACKWARD_DERIVED`) and
from one standing inside a span the source contradicts (`CONTRADICTED`). The
full vocabulary and the reasoning are in [One balance column, and why it is no
longer two](ARCHITECTURE.md#one-balance-column-and-why-it-is-no-longer-two).

They are three columns because this source keeps its books two ways. For its
first 188,469 rows the stated balance moves by `TXN_AMOUNT_CLEANED`, in the
account's own currency; from row 188,470 on it moves by `BILLING_AMOUNT`, in
USD. So `running_balance_filled` is denominated in LBP on one row and USD on
another, and `running_balance_currency` is the only thing that says which --
`10,000,000 LBP` and `5,000 USD` are both true statements and neither is safe
to read without its unit.

`running_balance_normalized` is what downstream work aggregates: one currency,
so summing across accounts is meaningful. A balance already in USD normalizes
to itself at an effective rate of exactly `1.0`. That is asserted from the
currency, not read from `fx_rate` -- which is **not** 1 on most USD rows of
this source, and would restate 100 dollars as 100.44. The raw `fx_rate` column
is read and never written; the effective rate is a separate thing that exists
only inside the normalization.

The valuation is point-in-time. The rate belongs to the transaction on that
row, while the balance accumulated over many rows at many rates, so the column
answers *what is this balance worth now* rather than *what was each historical
movement worth when it happened*. On the USD accounts that is 73% of rows and
the distinction is immaterial; on ARS, which moved roughly fifteenfold across
the window, it is not, and a monthly close built on it should be read as a
valuation rather than as a reconstruction.

**Nothing configures where the convention changes.** The stage is given both
candidate movers and works out which is in force on each row from the stated
balances themselves -- see [Detecting the balance
regime](ARCHITECTURE.md#detecting-the-balance-regime). A row number in a config
file is a fact about one extract, and the next one would carry it silently
into a file it does not describe.

### Known limitation: the running balance

The `balance` stage chains a running balance per account in sequence order. A
single streamed row has no other transaction on its account to count from in
either direction, so the stage reaches no anchor, states no figure, and
`running_balance_filled` is `NULL` under the status `UNAVAILABLE` — which the
schema permits and which the log says out loud as `balance.unavailable`. A
zero would be a number nobody has any evidence for.

This is the one case where a batch run and a streamed run of the same row
disagree, and it is a property of the batch, not of the row: in a full load
that row's account brings its own history and the balance is reconstructed
normally. Seeding the anchor from `cleaned_transactions` is possible —
`idx_cleaned_account_seq` is in the schema for exactly that lookup — and is
deliberately not done yet.

### A note on speed

A batch takes a minute or two, most of which is fixed Spark overhead rather
than the data. The consumer therefore starts its session as `local[1]` with a
single shuffle partition, which is the opposite of what the batch run wants and
correct here: `local[*]` starts a Python worker per core, Windows process
creation is slow, and a two-row batch has nothing for the other fifteen threads
to do but be started. Measured on this machine, two rows through the eleven
stages:

| master | pass 1 | pass 2 | pass 3 |
|---|---|---|---|
| `local[*]` | 469.9s | 453.0s | 679.4s |
| `local[1]` | 105.3s | 71.0s | — |

`python consumer.py --workers '*'` if you ever want the other behaviour.

## Shape

Three ways in, one set of cleaning rules, two ways out. The stages are grouped
below by the layer they belong to — the same labels the consumer prints as it
runs, so what you read here and what you watch scroll past are the same
vocabulary.

```mermaid
flowchart TD
    subgraph input["Three sources, one pipeline"]
        RAW[("raw .xlsx<br/>transactions_v4 profile")]
        CSV[("forecast_balance_data.csv<br/>forecast_balance profile")]
        RAWT[("raw_transactions<br/><i>one row, via Kafka</i>")]
    end

    subgraph rules["rules/json - vocabularies, no logic"]
        R2["date_formats.json<br/>timestamp_formats.json"]
        R3["processing_codes.json"]
        R1["processors.json"]
        R6["merchants.json"]
        R4["city_aliases.json<br/>city_countries.json"]
        R5["mcc_rules.json"]
        R8["fx_rates.json"]
        R9["macro_series.json"]
    end

    subgraph pipe["Steps in dependency order - the profile chooses which run"]
        direction TB
        S1["<b>NORMALIZATION</b><br/>timestamps / dates"]
        S2["<b>ENRICHMENT</b><br/>macro"]
        S3["<b>DEDUPLICATION</b><br/>duplicates"]
        S4["<b>NORMALIZATION</b><br/>codes"]
        S5["<b>NORMALIZATION</b><br/>amounts"]
        S6["<b>DERIVATION</b><br/>balance"]
        S7["<b>NORMALIZATION</b><br/>missing"]
        S8["<b>ENRICHMENT</b><br/>merchant"]
        S9["<b>ENRICHMENT</b><br/>geo"]
        S10["<b>ENRICHMENT</b><br/>mcc"]
        S11["<b>VALIDATION</b><br/>consistency"]
    end

    REP[["CleaningReport<br/>shared audit trail"]]

    subgraph output["Two sinks, chosen by engine"]
        DB[("cleaned_transactions - spark<br/>Postgres, upserted on txn_id")]
        EV{{"pipeline.run.completed.v1<br/>counts, fingerprint, job id"}}
    end

    RAW --> S1
    CSV --> S1
    RAWT --> S1
    S1 --> S2 --> S3 --> S4 --> S5 --> S6 --> S7 --> S8 --> S9 --> S10 --> S11

    R2 -.-> S1
    R9 -.-> S2
    R3 -.-> S4
    R1 -.-> S8
    R6 -.-> S8
    R6 -.-> S10
    R4 -.-> S9
    R5 -.-> S10
    R8 -.-> S11

    S1 -.-> REP
    S5 -.-> REP
    S6 -.-> REP
    S8 -.-> REP
    S10 -.-> REP
    S11 -.-> REP

    S11 --> DB
    REP --> EV
    DB -.->|"after the write, never before"| EV
```

`balance` and `macro` belong to the `forecast_balance` profile only; `dates`
runs in place of `timestamps` for the v4 workbook. Which steps run is
`profiles:` in `config/pipeline.yaml`, detected from the source's own columns —
so the same eleven-stage chain serves a 265k-row file and a single row that
arrived over Kafka, and no stage can tell which it is looking at.

| # | Stage | Layer | What it does | Reasoning |
|---|---|---|---|---|
| 1 | `timestamps` / `dates` | Normalization | Parses the wall clock — five formats across two columns for v4, mixed conventions plus an explicit offset for the forecast extract | [Stage 1](ARCHITECTURE.md#stage-1-dates) |
| 2 | `macro` | Enrichment | Recovers the interest, inflation and holiday columns by month, as three broadcast joins | [`macro.py`](src/spark/cleaners/macro.py) |
| 3 | `duplicates` | Deduplication | Drops identical rows; sequences `TXN_ID` collisions | [Stage 2](ARCHITECTURE.md#stage-2-duplicates) |
| 4 | `codes` | Normalization | Restores leading zeros; regenerates labels from a lookup | [Stage 3](ARCHITECTURE.md#stage-3-codes) |
| 5 | `amounts` | Normalization | Parses text amounts to float and signs them by transaction type | [Stage 4](ARCHITECTURE.md#stage-4-amounts) |
| 6 | `balance` | Derivation | Reconstructs the running balance in both directions and states how far each figure is proven | [`balance.py`](src/spark/cleaners/balance.py) |
| 7 | `missing` | Normalization | Separates absent, unreadable and not-applicable | [Stage 5](ARCHITECTURE.md#stage-5-missing-values) |
| 8 | `merchant` | Enrichment | Cleans names, then resolves them against a curated master | [Stage 6](ARCHITECTURE.md#stage-6-merchants) |
| 9 | `geo` | Enrichment | Collapses city variants; checks the country each implies | [Stage 7](ARCHITECTURE.md#stage-7-cities) |
| 10 | `mcc` | Enrichment | Assigns a code and a confidence tier from five ranked signals | [Stage 8](ARCHITECTURE.md#stage-8-mcc) |
| 11 | `consistency` | Validation | Asserts the redundant encodings still agree | [Stage 9](ARCHITECTURE.md#stage-9-consistency) |

The layer names are the ones [`src/spark/stagelog.py`](src/spark/stagelog.py)
prints. They interleave rather than running in tidy blocks — `macro` is
enrichment and runs second, before three normalization stages — because that is
the real dependency order: the macro join needs the month `timestamps`
produces, and `codes` needs neither. A diagram that sorted them into blocks
would be describing a pipeline that does not exist.

Step order follows real dependencies rather than preference; [Execution order and why](ARCHITECTURE.md#execution-order-and-why) lists each edge and what breaks if it is reversed.

---

## Results on the source file

| Check | Result |
|---|---|
| Rows in / out | 2296 / 2296 |
| Dates unparsed | 0 of 4,592 values |
| Amounts unparsed | 0 (15 reformatted) |
| Amount signs restored | 13 (purchases whose text amount had no minus) |
| Duplicates | 0 exact, 0 ID collisions, 0 business-key repeats |
| Settlement unknown | 14 (3 blank, 9 `0000-00-00`, 2 `1970-01-01`) |
| Settlement anomalous | 6 (`SETTLE_DATE < TXN_DATE`) |
| Terminal sentinels | 1051 flagged, none erased |
| Auth codes invalid | 109 (71 sentinel + 38 improbable repeats) |
| Cities | 128 distinct → 107 |
| Merchants | 586 cleaned spellings → 270 merchants, 0 unrecognised |
| City/country mismatch | 148 (all from US/TR/GB/IE) |
| MCC conflicts | 137 curated, 11 in review |
| MCC ATM violations | 6 |
| FX reconciliation failures | 48 (23 of them a billing amount 1/100 too small) |
| FX rates off reference | 481, all LBP (100 still on the 1507.5 peg) |
| Rows flagged | 643 |

Every number here comes from `cleaning_report`.

---

## Rule files

Vocabularies live in `src/rules/json/` as data, never as code, so updating one never requires re-reviewing the logic that applies it.

| File | Contents |
|---|---|
| `date_formats.json` | Ordered format list and the tokens that mean null. |
| `processors.json` | Payment-processor prefixes that gate the `*` split. |
| `processing_codes.json` | Transaction-type codes and labels. |
| `fx_rates.json` | One reference rate per currency, as units per USD. |
| `city_aliases.json` | City variant spellings → canonical name. |
| `city_countries.json` | Canonical city → the country it sits in. |
| `merchants.json` | The merchant master: canonical names, aliases, curated MCCs. |
| `mcc_rules.json` | Deterministic MCC rules and the catch-all codes. |

---

## Testing

Two ways in. `make test` is the one that runs in CI and proves the rules are
right. The walkthrough below is the one that shows the system actually working:
a dirty row goes in one end and comes out clean at the other, and every command
is one line.

### The automated suite

```bash
make test
```

Over 600 tests, table-driven from the awkward values in the real file rather
than invented ones. They fall into six groups: the row-level parsing rules,
the policies those rules exist to serve, staleness guards on the JSON rule
files, the output contract, the streaming path's own units, and one
end-to-end test of the whole flow.
[What a test should assert](ARCHITECTURE.md#what-a-test-should-assert) explains
the distinction the suite is built on.

Three markers say what a test needs, and each **skips** rather than fails when
it is missing -- a machine without the stack up is a setup condition with its
own diagnostic, not a code defect.

| Marker | Needs | Deselect with |
|---|---|---|
| `spark` | a JVM | `-m "not spark"` |
| `db` | Postgres on the port `.env` names | `-m "not db"` |
| `kafka` | the broker at `KAFKA_BOOTSTRAP_SERVERS` | `-m "not kafka"` |

```bash
make test-fast    # everything except the tests marked `spark`
make verify       # does this machine run the Stage 2 stack at all
```

The streaming path is tested the same way it is built -- in pieces that need
nothing, plus one test of the seams that needs everything:

```bash
python -m pytest -q tests/test_db_raw.py tests/test_kafka_ingest_events.py tests/test_kafka_consumer.py tests/test_stagelog.py
python -m pytest -q tests/test_streaming.py
```

The first line runs in about a second and covers the landing table's contract
against the CSV header, the event payload and everything `decode` refuses, the
consumer's commit ordering and its failure handling against a fake broker, and
the stage log's output. The second takes a few minutes and proves the parts
fit: a dirty row inserted, announced, consumed, cleaned and upserted, then
cleaned a second time to show a redelivery changes nothing.

### Walking the whole thing by hand

What this demonstrates, in one sentence: a dirty transaction lands in Postgres,
Kafka announces its id, a Spark consumer cleans that row and upserts it, and
`main.py` does the same cleaning over the whole file -- one set of rules, two
ways of being asked.

```
docker (postgres + kafka)
        |
  seed-raw  -->  raw_transactions      22 TEXT columns, status = PENDING
        |
  emit      -->  transactions.raw.ingested.v1     {"id": 42}
        |
  consumer  -->  reads row 42, runs the cleaning stages
        |
  stage + merge  -->  cleaned_transactions        typed, constrained
        |
  status = CLEANED, completion event on pipeline.run.completed.v1
```

Anywhere below that shows a `psql` command, you can equally point pgAdmin,
DBeaver or a JetBrains data source at the same database and look at the tables
directly -- the queries are written out because they are reproducible in a
terminal, not because the terminal is the only way to see this.

**Two terminals.** The consumer blocks, so it wants one to itself; everything
else runs in the other.

#### 1. Bring the stack up

```bash
docker compose up -d
```

Starts Postgres and Kafka. Spark is deliberately not in here -- it runs in the
project venv on the host, so the driver stays debuggable.

```bash
docker compose ps
```

Both containers must read **healthy**, not merely running: Postgres accepts a
TCP connection several seconds before it accepts a query.

```bash
make verify
```

Checks Java, Spark, Postgres and Kafka in one pass; every failure names its own
fix.

#### 2. Start from a known state

```bash
make db-reset
```

Drops all three tables and rebuilds them from the schema files -- destructive,
which is why it is separate from the writer's own idempotent migrate.

```bash
make kafka-topic
```

Creates both topics. Auto-create is off on purpose, so a producer aimed at a
typo'd topic fails loudly instead of inventing one nobody consumes.

#### 3. Put some rows in

```bash
make seed-raw N=3 OFFSET=0
```

Cuts 3 rows from the extract and inserts them verbatim, then prints their ids.
`OFFSET` skips that many source rows first; the scan always starts at the top
of the file, so without it every run seeds the same transactions. Between `N`
and `OFFSET` you choose how wide and how arbitrary the sample is -- some rows
carry a blank settlement date, a missing balance, an amount written
`5.727.580,00` or a merchant name still wearing its terminal prefix, and some
are already fine. Which is which is the pipeline's answer to give.

```bash
docker exec -it cleaning-postgres psql -U pipeline -d transactions -c "select id, txn_id, txn_amount, txn_date_time, settle_date, merchant_name, status from raw_transactions order by id desc limit 3;"
```

Shows what actually landed: mixed date formats, European decimals, ragged
merchant names, `status = PENDING`. Every column is `TEXT`, so the database
coerced nothing on the way in.

#### 4. Start the consumer (second terminal)

```bash
make consumer
```

Subscribes to `transactions.raw.ingested.v1` and blocks until something
arrives. Leave it visible.

#### 5. Announce the row

```bash
make emit ID=42
```

Publishes `{"id": 42}` -- the id and nothing else, because the row is already
durable in Postgres and the database stays the single source of truth. Watch
the second terminal wake up and clean it.

#### 6. Look at what came out

```bash
docker exec -it cleaning-postgres psql -U pipeline -d transactions -c "select txn_id_cleaned, txn_ts, txn_amount_cleaned, txn_ccy, merchant_name_cleaned, running_balance_filled, running_balance_currency, running_balance_normalized, sync_job_id, cleaned_at from cleaned_transactions order by cleaned_at desc limit 3;"
```

The same transactions, typed and constrained: timestamps parsed, amounts
numeric, merchants normalised. `running_balance_filled` may be null -- the
balance stage states a figure only where the arithmetic proves it, rather than
repeating an unverified number.

```bash
docker exec -it cleaning-postgres psql -U pipeline -d transactions -c "select id, status, cleaned_at, last_error from raw_transactions order by id desc limit 3;"
```

The landing row is now `CLEANED`. That flag is what makes recovery possible in
step 8.

#### 7. Show that redelivery is safe

The claim worth proving properly: at-least-once delivery is fine because the
write is idempotent.

```bash
make fingerprint
```

Prints one line describing the whole table: row count, distinct keys, the
amount total, how many loads contributed, the last write time and a digest over
every key and amount. Everything but `last write` is expected to survive a
replay.

```bash
make emit ID=42
```

Replays the identical event. Run it two or three times: the consumer re-runs
the full cleaning each time, so nothing is being skipped by a duplicate check.

```bash
make fingerprint
```

The same command again. `rows`, `keys`, `total`, `loads` and `digest` are all
unchanged -- only `last write` has moved, which is the proof the pipeline
really did write again rather than skipping the row.

The mechanism is the
`INSERT ... SELECT DISTINCT ON (txn_id_cleaned) ... ON CONFLICT DO UPDATE` in
`src/db/contract.py` -- `DISTINCT ON` because `ON CONFLICT` raises if one
statement touches a key twice, and `cleaned_at = now()` sits outside `EXCLUDED`
so an updated row does not keep the timestamp of the load that first inserted
it.

#### 8. Recover from an outage

Stop the consumer with a single Ctrl-C: it finishes its batch, commits, and
leaves the consumer group deliberately, so a restart rejoins immediately rather
than waiting for the broker to time the old member out.

```bash
make seed-raw N=2 OFFSET=9000
```

Inserts two more rows while nothing is listening -- they sit at `PENDING`.

```bash
make emit PENDING=1
```

Asks the database which rows are still `PENDING` and publishes an event for
each. You do not have to remember which ids were stranded, or how they were
stranded -- seeded during downtime, emitted into a dead consumer, or abandoned
by a crash mid-batch all look the same from here.

```bash
make consumer
```

Restart it and watch it drain the backlog.

#### 9. The same rules over the whole file

```bash
python main.py --dry-run
```

Reads the full extract, runs every cleaning stage, prints the report, writes
nothing -- the state you want when the question is whether the cleaning is
right rather than whether the write is.

```bash
make run
```

The same run, writing: project the frame, bulk-load it into the unlogged
staging table, then one `ON CONFLICT` statement merges it into
`cleaned_transactions`. Spark's JDBC writer has no upsert mode, which is why
the staging table exists.

#### 10. Tear down

```bash
docker compose down
```

Stops both containers, keeping the Postgres volume. Add `-v` to discard the
data too.

---

# Task 3 - Feature Table

## What this task is

Stage 2 produces one row per transaction. A forecasting model needs one row per
**user per month**, with every value on it computable from information that
existed *before* that month began.

Task 3 is that transformation, and nothing more: it builds the feature table,
it does not model. The target is the balance a user holds at the end of a
month; the features are its history, the flows into and out of it, how active
the user was, and where they spent. Which of those actually predicts anything
is Stage 4's question.

Three properties define the build:

- **Point-in-time correctness.** Every feature on month M is derived from
  months strictly before M. The one exception is the target, which reads M by
  definition and is never an input.
- **One grain, one destination.** One row per `(user_id, month)`, upserted into
  `feature_store_monthly` in Postgres. There is no file copy - a second
  artifact is a second thing to keep in step.
- **PySpark throughout.** The frame that leaves the reader is the frame that
  reaches the upsert. Nothing in `src/features/` imports pandas, and a test
  enforces that rather than trusting it.

```bash
make db-rules
```

Seeds the spending and direction vocabularies into the rule tables.

```bash
make features
```

Reads `cleaned_transactions`, builds the table, upserts it, and writes the run
report to `data/features/feature_store_monthly.manifest.json`.

```bash
make features-scale
```

The same build over a source replicated to five times the users, so the timings
can be compared rather than read on their own.

## The feature table

28 columns: two keys, 25 features, one target. Every monetary column is USD.

`prev_1m_` means the calendar month immediately before this row's month -
guaranteed by a dense spine, so a user who was silent in April still has an
April row and May's `prev_1m` is April rather than March.

### Grain

| Column | What it is |
| --- | --- |
| `user_id` | The user. Account-level figures are rolled up to here |
| `month` | First day of the month this row describes |

### Balance history

| Column | What it is |
| --- | --- |
| `prev_1m_closing_balance_usd` | Closing balance one month before this row's month |
| `prev_2m_closing_balance_usd` | Two months before |
| `prev_3m_closing_balance_usd` | Three months before |
| `roll3_mean_closing_balance_usd` | Mean closing balance over the three preceding months |
| `roll3_std_closing_balance_usd` | Sample standard deviation of the same three; null under three observations |
| `delta_prev_1m_2m_closing_balance_usd` | `prev_1m` minus `prev_2m` - the latest month-on-month change that is still in the past |

A user's closing balance is the sum of their accounts' month-end balances. An
account that was quiet keeps its last known figure; nothing is ever filled
backwards.

### Money in and out

| Column | What it is |
| --- | --- |
| `prev_1m_total_credited_usd` | Money in during the preceding month, as a positive magnitude |
| `prev_1m_total_debited_usd` | Money out during the preceding month, as a positive magnitude |
| `prev_1m_net_flow_usd` | Credited minus debited |
| `roll3_mean_total_credited_usd` | Mean monthly credit over the three preceding months |
| `roll3_mean_total_debited_usd` | Mean monthly debit over the same three |
| `roll3_mean_net_flow_usd` | Mean monthly net flow over the same three |

Direction comes from the processing code's declared direction, never from the
sign the source wrote on the amount. A code that declares no direction enters
neither total and is counted in the run report instead.

### Activity

| Column | What it is |
| --- | --- |
| `prev_1m_txn_count` | Transactions in the preceding month |
| `prev_1m_distinct_merchants` | Distinct counterparties in the preceding month; internal descriptors excluded |
| `accounts_held` | Accounts whose first transaction is strictly before this row's month |

`accounts_held` is a point-in-time count, not a count over all time - the
latter would tell a model in month 3 that the user opens a fourth account in
month 40.

### Calendar

| Column | What it is |
| --- | --- |
| `month_of_year` | 1-12 for this row's month |
| `days_in_month` | 28-31 for this row's month |

Both are properties of the Gregorian calendar and fixed before the month
begins, which is why they are not lagged.

### Spending

| Column | What it is |
| --- | --- |
| `prev_1m_total_spend_usd` | Spend-eligible debits in the preceding month |
| `prev_1m_spend_groceries_usd` | Of that, groceries |
| `prev_1m_spend_dining_usd` | Dining |
| `prev_1m_spend_retail_usd` | Retail |
| `prev_1m_spend_cash_atm_usd` | Cash and ATM |
| `prev_1m_spend_transport_usd` | Transport |
| `prev_1m_spend_bills_usd` | Bills |
| `prev_1m_spend_other_usd` | The residual - anything whose MCC maps to no category above |

Amounts only. A category's share of the month is its amount over
`prev_1m_total_spend_usd`, and both are on the row, so publishing the quotient
as well would duplicate information at one column per category. Stage 4 divides
if Stage 4 wants a share.

Spending is narrower than debits: a transfer between the customer's own
accounts is a real outflow and belongs in `total_debited`, but it is not
consumption and enters no category. The categories therefore sum to
`prev_1m_total_spend_usd`, not to `prev_1m_total_debited_usd`.

### Target

| Column | What it is |
| --- | --- |
| `target_closing_balance_usd` | The user's closing balance at the end of *this* row's month |

The label. It reads month M - the one column allowed to - and must never be
used as an input. It comes from the same underlying figure the lags do, so the
label and its own history are on one scale by construction.

## The run report

Pipeline diagnostics are deliberately **not** feature columns. How many
accounts contributed a balance, whether a figure was carried forward, how many
transactions could not be classified - these describe the build, not the
customer, and a model handed them has to be told to ignore them.

They are still computed, and they go here:

```
data/features/feature_store_monthly.manifest.json
```

Every metric in it is self-describing - the value, what it counts, and what a
high or low reading means - so a number never has to be looked up elsewhere:

```json
"account_months_carried_forward": {
  "value": 18432,
  "of": 204800,
  "pct": 9.0,
  "what": "Account-months with no observation of their own, filled from an earlier month.",
  "means": "Real figures, but stale. A high rate means the balance series leans on persistence rather than on fresh statements."
}
```

The report covers, in sections:

| Section | What it answers |
| --- | --- |
| `coverage` | How much table came out of how much input, over what window |
| `balance_quality` | Rows excluded as `CONTRADICTED` or `UNAVAILABLE`, account-months observed vs. carried forward vs. never reachable, accounts that never supplied a balance, partial rollups, the longest carry-forward run |
| `direction_quality` | Credits, debits, transactions with undeclared direction and the USD they represent, which codes they were, and sign disagreements |
| `spending_quality` | What was spend-eligible, what was excluded as transfer, unmapped and null MCCs, and the residual's share of all spend |
| `activity_quality` | Inactive user-months, dormancy percentiles, internal descriptors, unparseable timestamps |
| `point_in_time` | Every column's `known_at`, and which one is the target |
| `performance` | Per-phase wall clock, the slowest phase, and JVM peak memory |

## Where the reasoning lives

This section says *what* each column is. *Why* it is computed that way - the
dense spine, the balance eligibility cutoff, why the lag is taken in exactly
one module, why the diagnostics are lagged and then dropped rather than never
computed - is written where the code is, one module docstring per concern:

| Module | The decision it argues for |
| --- | --- |
| [`src/features/contract.py`](src/features/contract.py) | The column list, and why the diagnostics and shares are not on it |
| [`src/features/spine.py`](src/features/spine.py) | Why the account timeline is dense to the end of the window |
| [`src/features/balances.py`](src/features/balances.py) | Carry-forward, and why nothing is ever filled backwards |
| [`src/features/windows.py`](src/features/windows.py) | The point-in-time rule as a window frame |
| [`src/features/diagnostics.py`](src/features/diagnostics.py) | Why observability is counted once at the end rather than per metric |
| [`src/features/writer.py`](src/features/writer.py) | Why the upsert goes through a staging table |

Stage 2's reasoning is in [`ARCHITECTURE.md`](ARCHITECTURE.md), one section
per cleaning stage.

---

# Future work

**Extend the MCC reference.** Car rental (`7512` in ISO 18245) is absent, so `AVIS` and `HRTZ` are recorded under `7538` Automotive Service Shops by convention rather than correctness.

**Improve merchant grouping.** `THE BODY SHOP` currently cleans to `THE BODY` because branch-number stripping removes the trailing token, which is harmless here but shows the rule is blunter than it should be.

**Add a merchant alias layer.** Abbreviated names (`USJ`, `RPSL`, `STRBCKS`) are entity-knowledge problems that no text model solves, so they need a lookup that grows as names are identified.
