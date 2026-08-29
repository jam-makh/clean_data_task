# Architecture

How each cleaning stage works, one stage per section. Every section follows the
same shape: **input → steps → output → the hard case**.

[`README.md`](README.md) says what the pipeline is, how to run it and what it
produced. This file says why each stage decides what it decides. A run's
outcome — how many rows each rule caught — lives there, not here, so the two
never drift.

---

## Table of contents

- [The shared contract](#the-shared-contract)
- [Execution order and why](#execution-order-and-why)
- [Stage 1 — Dates](#stage-1--dates)
- [Stage 2 — Duplicates](#stage-2--duplicates)
- [Stage 3 — Codes](#stage-3--codes)
- [Stage 4 — Amounts](#stage-4--amounts)
- [Stage 5 — Missing values](#stage-5--missing-values)
- [Stage 6 — Merchants](#stage-6--merchants)
- [Stage 7 — Cities](#stage-7--cities)
- [Stage 8 — MCC](#stage-8--mcc)
- [Stage 9 — Consistency](#stage-9--consistency)
- [Five rules that apply everywhere](#five-rules-that-apply-everywhere)
- [Adding a stage](#adding-a-stage)
- [Adding a rule file](#adding-a-rule-file)
- [The parity harness](#the-parity-harness)

---

## The shared contract

Every stage is a class with two methods, and the split between them is the
whole contract.

```python
class BaseCleaner(ABC):
    def apply(self, df: pd.DataFrame) -> pd.DataFrame: ...      # marks
    def metrics(self, df: pd.DataFrame) -> Iterator[...]: ...   # counts
```

| Rule | Why |
|---|---|
| Takes a frame, returns a **new** frame | The input is never mutated, so a stage can be rerun |
| Same shape for every stage | The pipeline holds them in a list and runs them without knowing what any does |
| `apply` marks rows and counts nothing | See below — this is what makes the stage portable to Spark |
| `metrics` derives every total from columns | A step that quietly nulls 400 rows looks identical to one that changed nothing |
| Steps are constructor arguments, not hardcoded | You can run one stage alone while developing it |

```python
TransactionCleaner(steps=[DateNormalizer]).run(df)   # one stage
TransactionCleaner().run(df)                         # all nine
```

### Mark, don't count

A stage never computes a total while it is touching rows. It writes a
**diagnostic column** saying what it did to each row — which date format was
read, whether an amount had to be reformatted, whether a balance reconciled —
and the run's totals are derived from those columns afterwards, in one pass,
by `TransactionCleaner.run`.

The reason is that a running total has to live somewhere outside the rows, and
the only places to put one are a closure or an accumulating call like `.sum()`.
Both are single-process constructs. This is what the code used to look like:

```python
matched: dict[str, int] = {}
parsed = df[source].map(lambda v: self._parse(v, compiled, null_tokens, matched))
for fmt, count in sorted(matched.items(), key=lambda kv: -kv[1]):
    self.log(f"{source}.format[{fmt}]", count)
```

`matched` is filled in as a side effect, once per row, and works only because
`.map` runs every row in one process. Distributed, each executor gets its own
copy, fills it in, and discards it; the driver reads back the original, still
empty, and reports zero for every format. No error, no warning — just a report
that lies. Writing the format onto the row instead is the operation that
survives the move, because the mark travels with the row it describes.

It also makes the audit trail a property of the data rather than of the run.
"How many amounts were reformatted" and "was *this* amount reformatted" stop
being two questions answered in two places.

| Written by | Column | Says |
|---|---|---|
| dates / timestamps | `*_FORMAT` | Which rule read the value, or `NULL_TOKEN` / `UNRECOGNISED` / `UNPARSEABLE` |
| timestamps | `TXN_TS_SLASH_RESOLUTION` | Which of the three passes settled a `NN/NN` date |
| amounts | `TXN_AMOUNT_COERCION` | `PARSED` / `REFORMATTED` / `UNPARSEABLE` / `ABSENT` |
| amounts | `TXN_AMOUNT_SIGN`, `PROCESSING_CODE_DIRECTION` | Where the sign came from, and on whose authority |
| duplicates | `EXACT_DUPLICATE_COPIES`, `TXN_ID_COLLISION` | How many source rows this one stands for; whether its key was shared |
| missing | `AUTH_CODE_REPEATED` | The code recurs across the file, so it was planted |
| balance | `RUNNING_BALANCE_CHAIN_BREAK` | This published balance does not lead to the next one |
| mcc | `MCC_SIGNAL` | Which rule chose the code |

`EXACT_DUPLICATE_COPIES` is the one that needs the trick: the dropped rows are
gone by the end of the run, so the survivor carries the size of the group it
absorbed, and `sum(copies) - len(df)` is what was dropped. In Spark that is one
`groupBy(*columns).count()`.

These are on the frame, not on the cleaned sheet — the sheet states the cleaned
row, and a column saying how a value was arrived at is a second reading of the
column beside it. `AUDIT_COLUMNS` in `utils/columns.py` is the list that
answers "why does this row look like this", which is
the question actually being asked.

---

## Execution order and why

Order is set by real data dependencies, not preference.

```mermaid
flowchart LR
    D[1 Dates] --> U[2 Duplicates] --> C[3 Codes] --> A[4 Amounts]
    A --> M[5 Missing] --> ME[6 Merchants] --> CI[7 Cities]
    CI --> MC[8 MCC] --> CO[9 Consistency]
```

| Edge | Reason it cannot be reversed |
|---|---|
| Dates → Duplicates | Collisions are ordered by date; sorting mixed-format date *strings* orders garbage |
| Codes → Amounts | The sign is taken from `PROCESSING_TYPE_CLEANED`, which stage 3 resolves |
| Codes → Missing | Types must settle first, so "unreadable" is distinguishable from "absent" |
| Dates → Missing | `SETTLE_DATE_STATUS` needs a parsed date to call it anomalous |
| Merchants → MCC | MCC layers 3–5 group by the cleaned merchant name |
| Cities → Consistency | The geo check reads `MERCHANT_COUNTRY_EXPECTED` |
| everything → Consistency | It asserts across every derived column, so it runs last |

---

## Stage 1: Dates

**In:** `TXN_DATE_TIME`, `SETTLE_DATE` — 5 formats across 2 columns
**Out:** `TXN_DATE_TIME_CLEANED`, `SETTLE_DATE_CLEANED` as `datetime64`

### Steps

1. Strip the value. If it matches a null token, return `None`.
2. Try each format **in a fixed order**; first regex match wins.
3. Parse with that format's `strptime`.
4. No match → `NaT`, and count it.

### The format table

Order matters. ISO comes first because `YYYY-MM-DD` and `MM-DD-YYYY` both use dashes.

| Format | Example | Rows |
|---|---|---|
| `iso_datetime` | `2022-03-10 00:51:36` | 1214 |
| `slash_day_first_datetime` | `09/07/2022 14:22` | 466 |
| `dash_month_first_datetime` | `04-22-2022 09:15` | 328 |
| `day_month_abbrev_datetime` | `10-Mar-22 00:51` | 175 |
| `epoch_millis` | `1646873496000` | 113 |

### The hard case: 604 ambiguous rows

`09/07/2022` — is that 9 July or 7 September?

**The separator decides.** It is a property of the exporter, verified before use:

```
'/' is day-first    630 unambiguous rows, 0 counterexamples
'-' is month-first  371 unambiguous rows, 0 counterexamples
```

> "Unambiguous" means one component is > 12, so only one reading is possible.

Never fall back to a guessing parser. An unparsed date means **a format we do not
handle yet** — a bug signal. Imputing it hides the bug, and next quarter's file
silently loses rows to a format nobody noticed.

---

## Stage 2: Duplicates

**In:** the full frame
**Out:** same rows, plus `TXN_ID_SEQ`

### Two different defects, treated differently

| Case | Meaning | Action |
|---|---|---|
| Byte-identical row | double-load | **drop** |
| Same `TXN_ID`, different rows | upstream key fault | **keep both**, sequence them |

The second is never dropped — two rows sharing an ID may be two real
transactions with real amounts.

### Why a separate column

```python
df["TXN_ID_SEQ"] = 0        # 0 normally, 1..n on collision, ordered by date
```

Suffixing `TXN_ID` to `"4774694-2"` would flip the column's dtype from int to
string — **and only on files that happen to contain a collision.** Code that
works all year breaks in the one quarter that has a duplicate.

This stage is a no-op on the current file and exists for the next one.

---

## Stage 3: Codes

**In:** `PROCESSING_CODE` (int), `MCC_CODE`
**Out:** `PROCESSING_CODE_CLEANED`, `PROCESSING_TYPE_CLEANED`, `MCC_CODE_CLEANED`, `MCC_CATEGORY`

### The principle

> A code spelled with digits is not a number. Arithmetic on it is meaningless
> and its leading zeros carry meaning, so its canonical form is a **string**.

An integer column destroyed the leading zeros. Padding restores them:

| Stored as int | ISO 8583 field 3 | Label | Rows |
|---|---|---|---|
| `0` | `00` | Purchase | 2112 |
| `1` | `01` | ATM Cash Withdrawal | 71 |
| `20` | `20` | Purchase Return/Refund | 113 |

> The full ISO field is 6 digits (type, from-account, to-account). This export
> carries only the leading transaction-type pair, hence width 2 and not 6.

### Labels are regenerated, never trusted

```python
labels = df["PROCESSING_CODE_CLEANED"].map(codes.get)
```

The incoming `PROCESSING_TYPE` text is not copied — it is **recomputed from the
code** and the old value compared against it. A future file spelling it `"ATM
WITHDRAWAL"` still lands on one canonical value, and the disagreement is counted.

> **Honest limit.** `PROCESSING_CODE` is 1:1 with `PROCESSING_TYPE` across all
> 2296 rows, zero disagreements. It is a join key with nothing to join to and a
> validation layer that never fires. Kept because a real switch keys on it.

---

## Stage 4: Amounts

**In:** `TXN_AMOUNT` stored as **text**
**Out:** `TXN_AMOUNT_CLEANED` as float, signed by transaction type

### Three conventions in one column

| Input | Convention | Output |
|---|---|---|
| `(808.41)` | accounting negative | `-808.41` |
| `1,193.50` | comma thousands | `1193.50` |
| `5.727.580,00` | European decimal | `5727580.00` |
| `172,22` | **ambiguous** | depends on currency |

### Steps

1. Drop everything that is not a digit, separator, parenthesis or minus.
2. Parentheses or a leading `-` → negative.
3. **Both** separators present → the **last one** is the decimal point.
4. **One** separator → see below.

### The hard case: a lone comma

`172,22` could be 172.22 or 17222. Resolution order:

```
more than one occurrence          -> thousands       1,193,500
followed by exactly 3 digits      -> ask the currency
anything else                     -> decimal         172,22 -> 172.22
```

The currency is the tiebreaker because **a zero-decimal currency cannot have
minor units**:

```python
ZERO_DECIMAL = {"LBP", "JPY", "KRW", "VND", "IQD"}
```

So `5.727.580,00` under LBP is 5,727,580 — not 5.72.

### The sign the source dropped

Parsing recovers a negative only when the cell carries one to recover — a
leading `-`, or accounting parentheses. 13 rows carry neither:

| Stored as | Example | Sign survived? |
|---|---|---|
| number | `-104.39` | yes |
| text, parenthesised | `(808.41)` | yes |
| **text, bare** | `409.34` | **no — 13 rows** |

All 13 are purchases, and every purchase stored as a number in this file is
negative, so the convention is not in doubt: the sign was lost by whatever
wrote those cells as strings.

So the sign is not read from the cell at all. It is taken from the transaction
type, which states the direction of the money independently of how the amount
happened to be formatted:

```python
refund = df["PROCESSING_TYPE_CLEANED"].astype(str) == REFUND_LABEL
signed = df[target].abs().where(refund, -df[target].abs())
```

Only the sign moves; the digits are what the source got right. Corrected rows
are counted as `TXN_AMOUNT_CLEANED.sign_restored` **and** flagged
`AMOUNT_SIGN_RESTORED` per row, because the value now differs from the one in
`raw_transactions` and that has to be traceable to the transaction.

---

## Stage 5: Missing values

**In:** `TERMINAL_ID`, `AUTH_CODE`, `SETTLE_DATE_CLEANED`
**Out:** `HAS_TERMINAL`, `AUTH_CODE_VALID`, `SETTLE_DATE_STATUS`

### The whole policy follows from one split

| Kind | Meaning | Treatment |
|---|---|---|
| **Absent** | never recorded | leave null |
| **Unreadable** | present but unparseable | leave null, **always count** |
| **Not applicable** | legitimately has no value | keep the value, add a flag |

Collapsing "unreadable" into "absent" is the costly mistake — it turns a bug
signal into a silent gap.

### Per column

| Column | Count | Kind | Action |
|---|---|---|---|
| `TERMINAL_ID` = `00000000` | 1051 | not applicable | `HAS_TERMINAL = False` |
| `AUTH_CODE` sentinel/repeat | 109 | absent / invalid | `AUTH_CODE_VALID = False` |
| `SETTLE_DATE` | 14 | absent | `SETTLE_DATE_STATUS = MISSING` |
| `MERCHANT_CITY` | 67 | absent | leave blank, no imputation |

### `TERMINAL_ID` at 46% is signal, not dirt

**0 of 71 ATM rows carry the sentinel** — an ATM is itself a terminal. So the
sentinel marks *card-not-present*, and imputing or dropping it would destroy the
card-present distinction.

### The hard case: nulls wearing three disguises

`SETTLE_DATE`'s 14 gaps arrive as:

```
blank        3    fails isna()  ✓
0000-00-00   9    MySQL zero-date   -- reads as an ordinary value
1970-01-01   2    epoch zero        -- reads as an ordinary value
```

Only the blanks fail an `isna()` check. The other 11 would have **parsed into
real 1970 dates**. So every disguise collapses to `NaT` *before* any other
decision, and "is it missing" then has exactly one answer in one place.

### Why the date stays null

The lag distribution looks safe to impute (0–4 days, mode 1, median 2). It is not:

- A +2 day estimate is right only **31.9%** of the time; the mode (+1) only 42%.
- The affected accounts settle at observably different speeds.
- `TXN_ID 4774694` is `Unmatched` — settlement is not even established, so an
  estimated date would assert an unevidenced event.

`SETTLE_DATE_STATUS` carries the state instead, as `MISSING`.

The sheet does print `UNKNOWN` in `settle_date_cleaned` for those rows, because
a blank cell reads as "nothing to say here" as easily as "not settled yet". But
it is printed by `render_dates` on the way out, in the same pass that applies
the display format, and nowhere earlier. Holding the string in the column would
be the **same defect as `0000-00-00`** — a text placeholder in a date field,
forcing the column to text and breaking every sort and date calculation after
it. The status column is the machine-readable half and says `MISSING`, not
`UNKNOWN`, so it states what the source did rather than repeating the word in
the cell beside it. The same treatment is applied to `running_balance` and to
`running_balance_filled`, whose blanks go out as `UNKNOWN` for the same reason:
a withheld balance is a decision the pipeline took, not a cell nobody filled in,
and `running_balance_status` beside it names which decision — `CONTRADICTED`,
`UNVERIFIED` or `UNKNOWN`. Both stay real nulls inside the pipeline; only the
rendered copy carries the word.

### Detecting the balance regime

The stage is never told which column moves the balance. It is given two
candidates -- `TXN_AMOUNT_CLEANED` and `BILLING_AMOUNT` -- and works out which
is in force on each row from the stated balances themselves.

Every pair of consecutive rows in an account where the source states a balance
on both is a fact about one row's mover:

```text
delta            = balance[i] - balance[i-1]
explains_native  = |delta - TXN_AMOUNT_CLEANED[i]| <= tolerance
explains_billing = |delta - BILLING_AMOUNT[i]|     <= tolerance
```

On this extract that yields 126,015 pairs. A regime is a *run* of sequence
numbers over which one mover keeps winning, so the answer is a segmentation of
that evidence in sequence order rather than a majority vote over the file. It
is computed as a two-state dynamic program: a row costs 1 if the mover it is
labelled with fails to explain its step, changing label costs
`regime_switch_penalty`, and the cheapest labelling wins.

Run on this source, that finds the change at `TXN_SEQ` **188,470** — which is
where it is, and which appears nowhere in the code or the config. Before the
seam the native mover explains 99.6% of pairs and the billing mover 0.2%;
after it, the billing mover explains 100.0%.

The formulation is what makes the stage work on files this one says nothing
about:

- **No seam.** A single-convention file pays a switch penalty it can never
  recoup and comes back one label throughout.
- **Several seams.** Nothing in the cost function knows how many change points
  to expect, so three come back as three.
- **Coincidence.** An isolated row that only the other mover happens to
  explain cannot open a regime of its own: a two-switch excursion has to save
  more than twice the penalty to be worth taking. The stage is biased towards
  reporting no change.

**The seam needs no special case downstream, and that is not luck.** The
running total is accumulated from whichever mover governs each row, so the
difference between two cumulative totals is exactly the sum of the movers
actually in force between them. Two stated balances either side of the change
therefore still carry equal offsets and still reconcile. Before this was
detected, the entire second half of the extract came back `CONTRADICTED`;
after, that count is **2 rows**.

The one part of this that is irreducibly sequential is the dynamic program --
each row's cheapest label depends on the row before it. So `segment` lives in
the pandas module and the Spark port *imports and calls the same function*,
collecting the same small evidence projection to the driver and broadcasting
the resulting change points back as a `when` chain on `TXN_SEQ`. Two
implementations of a dynamic program would be two chances to disagree on a
file where the seam is marginal, and the parity harness would end up comparing
them rather than comparing the pipeline.

### A tolerance of 0.02, and why not 0.01

A running balance is reached by repeated addition of two-decimal figures, and
the residue that leaves is not bounded by half a cent once a chain is long
enough. The billing-regime chains reconcile on **87.8%** of adjacent pairs at
0.01 and on **100.0%** at 0.02 — and the figure does not move again at 0.05,
1.0 or 5.0.

That plateau is the evidence. A real discrepancy would not sit in a band one
cent wide; the rows between the two thresholds are rounding. The native regime
holds flat at 99.6% across the whole sweep, so the looser tolerance buys
nothing there and costs nothing either. Tightening it back to 0.01 would
falsely reject roughly an eighth of a regime that is, in fact, exact.

### One balance column, and why it is no longer two

`running_balance_filled` states a figure on **every row the arithmetic can
reach an anchor from, in either direction** — 265,195 of 265,195 on this
extract. It used to state one only where the arithmetic could *prove* it,
which left 6,625 rows null, and a second column `running_balance_adjusted`
answered the weaker question beside it. The two have been merged.

The merge is not a relaxation of the standard. The old design was right that a
projected figure must never be readable as a proven one; it was wrong about
where to enforce that. Withholding the value does not stop a consumer being
wrong about it — it hands them a null, which a feature build must either drop
or impute, and both of those are decisions taken further from the evidence
than this pipeline is. Worse, the column that *did* carry the answer was
marked internal and never reached Postgres, so the only consumer that mattered
saw nulls and no explanation of them.

So the figure is stated and the confidence travels with it, in
`running_balance_status`, which is now a published column and not a diagnostic.
Two balance columns invited a total computed from the wrong one. One balance
column and a mandatory status does not.

Each row counts from the **nearest trusted anchor**, not one anchor per
account: anchoring each account at its first verified row reproduces the
surviving later balances 43.6% of the time, and re-anchoring at the nearest one
raises that to 67.9%. Rows before an account's first trusted balance count
backwards from it — the same arithmetic with the sign reversed. Neither
direction crosses an unreadable amount, because a running total with a hole in
it is short by an amount nobody can name.

An anchor is a stated balance the arithmetic has **not disproved**. A stated
balance two reachable neighbours refute is excluded from the anchor set
entirely, not merely flagged: projecting from it would spread a known error
across every row that counts from it, and those rows would look derived. A
stated balance with no reachable neighbour is kept — untested is not
disproved, and an account whose only balance is unconfirmed still has better
evidence for that figure than for any alternative.

### The status vocabulary

Declared in `src/cleaners/balance.py` and constrained in `sql/schema.sql`
against the same list, strongest evidence first — so a consumer's eligibility
rule is a *prefix* of this table rather than a set of names to remember.

| Status | Meaning | Rows |
|---|---|---:|
| `OBSERVED` | the source stated it and a neighbouring stated balance confirms the arithmetic between them | 172,338 |
| `DERIVED` | the source left it blank and the trusted anchors either side agree on what it must be | 86,232 |
| `FORWARD_DERIVED` | counted forward from the last trusted anchor; nothing after it to check against | 205 |
| `BACKWARD_DERIVED` | counted back from the first trusted anchor; nothing before it to check against | 3,268 |
| `UNVERIFIED` | stated by the source, with no reachable neighbour to test it | 0 |
| `CONTRADICTED` | the trusted anchors bracketing this row **disagree** — the file is missing money that moved | 3,152 |
| `UNAVAILABLE` | no reachable anchor in either direction; the only status with a null balance | 0 |

`PROVEN` — `OBSERVED` and `DERIVED` — is the pair the arithmetic actually
established, 258,570 rows or **97.5%**. It is a named constant rather than a
condition spelled out at each use, because the invariant test, the report and
every downstream consumer must ask the question identically.

### `CONTRADICTED`, and why it still carries a number

It is not a defect in the reconstruction. It is the reconstruction working
correctly and reporting that the transaction list is incomplete: two anchors
each check out against their own neighbours, and the movements between them do
not account for the step from one to the other. Money moved that the file does
not record as a transaction.

Both answers are kept. `running_balance_filled` carries the forward one — or
the source's own figure, where the source stated one, because a stated balance
is an observation even when the rows around it refuse to agree with it. The
signed `running_balance_discrepancy` carries the rest: **add it to the
published figure and you have the backward answer exactly.** One column rather
than two more balance columns, and signed rather than absolute, for precisely
that reason.

The spread is the reason no single number summarises this and the report does
not try to. Across the 3,152 rows on 190 accounts, the disagreement runs from
0.02 — one tolerance-width, i.e. rounding — to **9.16 billion**, with a median
of 2,985. A file can hold both a cent of noise and a nine-figure hole, and an
aggregate over the two answers nothing.

### The invariant

```
Every cleaned transaction carries either
  a numeric running balance and a status explaining its provenance,
or
  the status UNAVAILABLE and no number.
Never one without the other.
```

Enforced in three places, deliberately: `CONSTRAINT
balance_stated_iff_available` in the table, a test in `tests/test_balance.py`,
and the `balance.stated` / `balance.unavailable` / `balance.proven` counts in
every run's own report. Each is stated as an **equivalence** and not an
implication — a null balance beside a status claiming provenance and a figure
stamped `UNAVAILABLE` are both incoherent, and checking one direction would
catch only one of them.

On this extract `UNAVAILABLE` is 0, so the persisted balance column has **no
nulls at all**. That is a property of the data, not a guarantee of the design:
every one of the 355 accounts happens to state at least 25 balances, and an
account that stated none would be `UNAVAILABLE` throughout rather than
silently zero.

### Auth codes: why any repeat is suspicious

```
6 alphanumeric chars  ->  ~2.2 billion combinations
across ~2300 rows     ->  ~0.001 expected genuine collisions
```

So a repeat is a planted value, not chance. Threshold is 2. **4 values repeat.**

---

## Stage 6: Merchants

**In:** `MERCHANT_NAME` — the dirtiest column
**Out:** `MERCHANT_NAME_CLEANED`, `MERCHANT_PROCESSOR`, `MERCHANT_KIND`,
`MERCHANT_TYPE`, `MATCHES_STATUS_CLEANED`, `merchant_review` sheet

This stage has **two halves that must not be confused**: string cleaning, then a
lookup. Cleaning cannot finish the job, and no amount of extra regex changes that.

```mermaid
flowchart TD
    A["SQ *AWS CS  22883"] --> B[1 split on the gated star]
    B --> C[2 strip reference codes, URLs, branches, legal suffixes]
    C --> D["AWS CS"]
    D --> N{3 an internal descriptor?}
    N -- yes --> P["kind = Internal<br/>named by the movement<br/>never queued"]
    N -- no --> E{4 in merchants.json?}
    E -- yes --> F["AWS CLOUD SERVICES<br/>kind = Merchant"]
    E -- no --> G["keep as cleaned<br/>kind = Unidentified<br/>-> merchant_review"]
```

### Three kinds, because the column holds three kinds of thing

`MERCHANT_NAME` is not always a merchant. 41293 rows of the workbook and 176064
of the forecast extract describe money moving **inside the bank** — settlement,
standing orders, sweeps between a customer's own accounts. Counted as merchants,
`CARD SETTLEMENT` is the largest one in the file by a factor of seven.

| Kind | What it is | Status | Queued? |
|---|---|---|---|
| `Merchant` | a counterparty the master names | `Confirmed` | no |
| `Internal` | money moving inside the bank | `Not a merchant` | no — already decided |
| `Unidentified` | a counterparty nobody has named yet | `Pending` | **yes** |

`MERCHANT_TYPE` collapses these to the two a reader is choosing between —
`Merchant` or `Internal`. A name nobody has resolved yet is still a
counterparty, and how far resolving it got is `MATCHES_STATUS`'s question.

Both `MERCHANT_TYPE` and `LOCATION_TYPE` are working columns: they are on the
frame and in the audit trail, not on the cleaned sheet, which carries one
column per source column and no verdicts. `MATCHES_STATUS` is the one that
goes out, because it is the column the source itself had.

An internal row is named by its movement, spelled in words: `CARD SETTLEMENT`,
`STANDING ORDER`, `INTERNAL TRANSFER`. The kind token behind it (`STANDING_ORDER`)
is a key in `internal_descriptors.json` and stays there — a column that otherwise
holds `CARREFOUR` should not hold an identifier. Naming the movement rather than
the descriptor is deliberate: the descriptor is truncated to eleven lengths and
eight of them identify the kind without identifying whether it was
`TRANSFER TO CURRENT` or `TRANSFER TO SAVINGS`, and the direction is already on
the row in the amount's sign.

`internal_descriptors.json` holds the descriptors, deliberately outside the
merchant master. What separates them from the employers they resemble
(`INDEVCO`, `MUREX`, `AUB PAYROLL`) is **`PROCESSING_TYPE`, not spelling**: every
descriptor row is settlement or transfer with zero purchases, while every
employer row is `SALARY_CREDIT` at 6012 and is a real counterparty.

Internal rows are named by the **kind** of movement, not by the descriptor. The
descriptor is truncated to eleven different lengths, and eight of them identify
the kind without saying which of `TRANSFER TO CURRENT` or `TRANSFER TO SAVINGS`
it was. Naming the kind states exactly what is known; the direction is already
on the row in the amount's sign.

### Truncation: one merchant as nine lengths of itself

The forecast source cuts `MERCHANT_NAME` to a field width, so a name survives at
every length it was cut to — and so does the `-CARD PMT-` suffix behind it. The
two are told apart by the dash:

```
H AND M -C  -CA  -CAR  -CARD PMT-   ->  a dash there can only be the suffix; any length goes
H AND M CARD  CARD P  CARD PM       ->  no dash: nothing shorter than the whole word CARD
METRO CASH CAR                      ->  ...because this is METRO CASH CARRY, not METRO CASH
```

Reading a dashless tail as the suffix deleted the real last word and left seven
merchants too short to recover. What survives the strip is settled by the
master, where a prefix identifies a name **only when exactly one merchant
extends it**. `AMERICAN UNIVERSITY BEIR` resolves; `CARREFOUR` and `PHARMACIE`
do not, and go to review rather than to whichever candidate sorted first. That
ambiguity check is also the enforcement behind `trap_pairs.json`: a never-merge
group holds two entries by definition, so no prefix can ever reach both.

### Half 1 — string cleaning

#### The gated `*` split

The merchant sits on **either side** of the star:

```
SQ *TAKEALOT           -> processor left,  merchant right
COURSERA.COM *W2PA     -> merchant left,   ref code right
```

A blind split would corrupt **114 merchants**. So the split only fires when the
left token is on a whitelist:

```json
"prefixes": ["SQ", "IZ", "TST", "WPY", "SP", "PP", "PAYPAL", "GOOGLE"]
```

The whitelist was derived by frequency — a genuine processor precedes many
unrelated merchants, a merchant appears once:

```
SQ 83 · IZ 72 · TST 55 · WPY 45 · SP 42 · PP 39 · PAYPAL 36 · GOOGLE 35
114 tokens appearing exactly once (MALIK BKSHP, WAYFAIR.COM, FEDEX.COM …)
```

#### Then, in order

| Step | Removes | Example |
|---|---|---|
| `STAR_REF` | a **second** `*` + code | `DEUTSCHE BAHN *TNAS` → `DEUTSCHE BAHN` |
| `REF_SUFFIX` | `/xyz` | `AUTOZONE /lzx` → `AUTOZONE` |
| `URL_SUFFIX` | `.COM`, `.LB`, … | `COURSERA.COM` → `COURSERA` |
| `BRANCH` | numbered branch | `CREPAWAY BR 306` → `CREPAWAY` |
| `LEGAL` | `LTD`, `SARL`, … | `PARKING SERVICES LTD` → `PARKING SERVICES` |
| token filter | reference codes, store numbers | `REPSOL #70 *4GK9` → `REPSOL` |
| `TRAILING_BRANCH` | bare `BR` at the end | `AMERICAN UNIVERSITY BR` → `AMERICAN UNIVERSITY` |

The token filter is the subtle one:

```
mixes letters and digits  -> always a code       K0SA, 4GK9   drop
pure digits, not first    -> store number        #70, 3382    drop
pure digits, first        -> part of the name    7 ELEVEN     keep
```

### Half 2: the merchant master

String cleaning gets variants *close* but never *equal*. One merchant arrives in
up to five forms:

| Form | Example |
|---|---|
| full | `WATERSTONES` |
| vowel-dropped | `WTRSTNS` |
| truncated | `YOUTUBE P` |
| space-stripped | `GOOGLEADS` |
| abbreviated | `CVS PHCY` |

So the final step is a **lookup, not a transformation**. `merchants.json` holds
one entry per merchant:

```json
"AWS CLOUD SERVICES": {
  "aliases": ["AWS CLOUD SERVI", "AWS CLOUD SVCS", "AWS CS", "AWSCLOUDSERVICES"],
  "note": "kept separate from AMAZON MARKETPLACE: same parent, different
           business (7372 vs 5999). MCC describes what was sold",
  "added_by": "Joseph Am-Makhlouf", "added": "2026-08-17"
}
```

An unrecognised name is never guessed: it keeps its cleaned form, its kind is
`Unidentified`, and it enters `merchant_review` with its raw spellings,
countries and observed MCCs. Internal movements never enter that queue — they
are unrecognised as merchants because they are not merchants, which is a
decision already taken rather than a question for a reviewer.

### The hard case: similarity is a hint, never the decision

Two traps that string distance gets wrong:

| Pair | Looks like | Actually |
|---|---|---|
| `WTRS` / `WTRSTNS` | one merchant | **Waitrose** 5411 grocery vs **Waterstones** 5942 books |
| `MEZYAN` / `MEZYANE` | one merchant | 5812 restaurant vs 5691 clothing, both LB |

Every merge was confirmed against **MCC + country**, not spelling. That is also
how `ADBL`→Audible (5815/US), `ATZN`→AutoZone (5533/US) and `CRM`→Careem
(4121/AE, *not* Crepaway 5812/LB) were settled.

Where the evidence disagreed, **nothing was asserted**: `TSC S` is 5411 in LB
while `SULTAN CENTER` is 5411 in KW, so it stays in review.

### Two naming rules

| Rule | Applied to |
|---|---|
| Brand relationship ≠ merchant identity | `AWS CLOUD SERVICES` stays separate from `AMAZON MARKETPLACE` |
| Country qualifiers are dropped | `CARREFOUR EGYPT`/`UAE`/`FRANCE` → `CARREFOUR`, since `MERCHANT_COUNTRY` carries geography |

One documented exception: `TOTAL LEBANON` keeps its suffix, because bare `TOTAL`
would collide with the unrelated `TOTAL WINE` and `TOTAL FITNESS CTR`.

---

## Stage 7: Cities

**In:** `MERCHANT_CITY`, `MERCHANT_COUNTRY`
**Out:** `MERCHANT_CITY_CLEANED`, `IS_ECOMMERCE`, `MERCHANT_COUNTRY_EXPECTED`

### Steps

1. Uppercase, then map through `city_aliases.json`.
2. Mark `IS_ECOMMERCE` from the marker tokens **or** a missing terminal.
3. Look up the expected country in `city_countries.json`.

### Two kinds of variant collapse in one map

| Kind | Example | Rows |
|---|---|---|
| transliteration | `BEIRUT` / `BEIRUT LB` / `BEYROUTH` / `BEYRUT` | 329 |
| card-not-present marker | `INTERNET` / `ECOM` / `E-COMMERCE` | 1051 |

**128 distinct → 107.** Three merges were found only after the geo check below
made them visible, and each was confirmed by **shared merchants**, not spelling:

```
ASHRAFIEH  = ACHRAFIEH   (only ASHRAFIYA was listed)
JBEIL      = BYBLOS      Cave de Byblos, Gift Corner Jbeil appear under both
AL HAMRA   = HAMRA       Beauty Lounge Hamra, Mezyan appear under both
```

### The check: city implies country

A city sits in exactly one country, so the pair is verifiable. Before building
it, two things had to be established.

**1. Which field is wrong.** `CARREFOUR` settles it — it legitimately spans four
countries and its pairing is correct on every row:

```
ABU DHABI -> AE     CAIRO -> EG     PARIS -> FR     BEIRUT -> LB (7)
                                                    BEIRUT -> US (2)  <- noise
```

However, one limitation here could arise when facing a city that shares its name with other cities in different countries (e.g., Tripoli -> Lebanon/Libya).

> **The merchant's own modal country is not a usable witness.** `SNCF CONNECT`
> carries `PARIS`+GB five times against `PARIS`+FR once, so the mode reports GB
> for a French railway. The noise outvotes the truth.

**2. That it is noise, not unmodelled geography.** All 148 bad values come from a
set of four:

```
US 79 · TR 34 · GB 24 · IE 10
```

Real geography does not concentrate like that.

### Why the reference is asserted, not computed

`city_countries.json` states 105 city→country pairs rather than taking the modal
country at runtime. **Deriving the rule from the data it polices is circular**,
and it fails immediately: `DELFT` has one row, an IKEA transaction tagged SE, so
the mode would place a Dutch city in Sweden.

All 105 were hand-checked; that was the only one the mode got wrong.

### `INTERNET` + `LB` is not an error

The marker sits in the *city* field; the country still describes where the
merchant is registered. So `INTERNET`+`LB` reads "online purchase from a
Lebanon-registered merchant" and nothing conflicts. Those rows get a **blank**
expectation and are never flagged.

> Not knowing where a merchant is differs from placing it in the wrong country.
> Only the second is a defect.

### Blank cities are not imputed

Country does not determine city, so there is nothing to derive from. Only 18 of
the 67 are card-not-present, so "it's e-commerce" does not explain the rest —
and for those 18, `IS_ECOMMERCE` already carries the fact. Writing `INTERNET`
into a city column would encode the same thing twice, in a fake location.

---

## Stage 8: MCC

**In:** `MCC_CODE_CLEANED`, `MERCHANT_NAME_CLEANED`, `PROCESSING_TYPE`
**Out:** `MCC_CODE_SUGGESTED`, `MCC_CONFIDENCE`, `mcc_review` sheet

The workbook's second sheet is a 41-entry `MCC_CODE → CATEGORY` reference. That
turns MCC into an independently verifiable field.

### Signals, in strict priority order

```mermaid
flowchart TD
    S{merchant in merchants.json with an mcc?} -- yes --> H1[HIGH · curated]
    S -- no --> O{one code across all its rows?}
    O -- yes --> H2[HIGH · consistent]
    O -- no --> D{ATM rule applies?}
    D -- yes --> H3[HIGH · deterministic]
    D -- no --> CA{catch-all is winning?}
    CA -- yes --> M1[MEDIUM · specific code wins]
    CA -- no --> T{tie?}
    T -- yes --> M2[MEDIUM if one non-suspect<br/>else PENDING]
    T -- no --> B[binomial tail on the majority]
    B --> R["p<=0.05 HIGH · p<=0.20 MEDIUM · else PENDING"]
```

| # | Signal | Basis |
|---|---|---|
| 0 | **Curated** | A human `mcc` in `merchants.json`. Beats everything |
| 1 | **Consistent** | One code across every row. Nothing disagrees |
| 2 | **Deterministic** | The reference labels `6011` *"ATM Cash Withdrawal"* — the exact string `PROCESSING_TYPE` uses |
| 3 | **Catch-all override** | `5999` is a bucket with no meaning, so a specific code beats it *regardless of count* |
| 4 | **Suspect tiebreak** | On a tie, a specific code beats `5812`/`5817` |
| 5 | **Majority** | Scored by a binomial tail, giving a real p-value |

### Signal 2 is the strongest, because it needs no merchant cleaning

```
71 ATM Cash Withdrawal rows  ->  65 carry MCC 6011
                                  5 carry 5999
                                  1 carries 5812     = 6 violations
```

### Signal 3 is the one that matters most

Majority vote returns **flatly wrong** answers on this data:

```
USJ BEIRUT   5999:7  8220:3   ->  vote says "Miscellaneous Retail" for a university
```

The rule flips it to `8220 Colleges, Universities`. Rationale:

> **`5999` is a bucket, not a category.** It is what an acquirer assigns when it
> does not know. It carries no positive information, so it can never win.

Applies to 30 merchants:

```
BOOTS PHARMACY 5999:5 5912:3 -> 5912 Drug Stores
OGERO          5999:4 4814:3 -> 4814 Telecommunication Services
DHL EXPRESS    5999:3 4215:2 -> 4215 Courier Services
ZARA           5999:4 5651:2 -> 5651 Family Clothing
```

### Not every suspect code is the same kind

| Code | Nature | Treatment |
|---|---|---|
| `5999` Misc Retail | pure bucket | never wins |
| `5812` Eating Places | **real category** also used as noise | only loses head-to-head |
| `5817` Digital Goods | **rail artifact** — see below | only loses head-to-head |

`PRET A MANGER` is `5812:8 / 5999:3` and genuinely *is* a restaurant. Demoting
5812 by identity would punish a correct answer.

**`5817` has a mechanical explanation.** All 26 rows carrying it are
Google-Pay-prefixed — **26 of 26**. The acquirer stamps that rail as *Digital
Goods, Applications* regardless of merchant (Ryanair, Carrefour, AXA, Avis). It
is not a category judgement at all.

### Confidence: three states

Three, because three is what a reader can act on.

| State | Meaning | Rows |
|---|---|---|
| `HIGH` | settled — curated, consistent, deterministic, or p ≤ 0.05 | 2230 |
| `MEDIUM` | inferred — catch-all override, tiebreak, or p ≤ 0.20 | 58 |
| `PENDING` | a human still has to decide | 8 |

Provenance is not lost to the collapse — it is logged:

```
signal[curated]     1222      signal[unresolved_tie]        8
signal[consistent]   973      signal[catch_all_override]    7
signal[majority]      80      signal[deterministic]         6
```

### Only PENDING reaches the review queue

**1 merchant** on this file: `ABC VERDUN` (`5812:4 / 5999:4`), a Beirut mall
where food court versus mixed retail is a genuine judgement call.

> A queue listing resolved items trains reviewers to skim it, which is how the
> one row that needs a decision gets missed.

### The known failure mode

The catch-all rule assumes the specific code is true. That breaks when the
merchant genuinely *is* miscellaneous retail:

```
NOON COM   5999:6  5812:4   ->  rule would wrongly suggest "Eating Places"
```

`NOON COM` is a general marketplace, so it is **pinned** to 5999 in
`merchants.json`. This is exactly why signal output is a review queue and never
an auto-applied repair.

### Why not a model

| Objection | Detail |
|---|---|
| It would not solve it | `USJ` fails because identifying it needs *entity knowledge* — that USJ is Université Saint-Joseph. Embeddings of "USJ BEIRUT" and "Colleges, Universities" are not close |
| It breaks auditability | "Why did this merchant get this MCC" stops having a stable answer |

The better answer is **reference data, not inference**: an internal merchant
master is authoritative and needs no guessing.

---

## Stage 9: Consistency

**In:** every derived column
**Out:** `VALIDATION_FLAGS` (changes no data)

### The idea

The dataset states some facts more than once. Redundancy becomes the validation
layer rather than something to collapse.

| Flag | Rule |
|---|---|
| `REQUIRED_NULL[…]` | 5 columns that make a row unusable if null |
| `SETTLE_BEFORE_TXN` | `SETTLE_DATE < TXN_DATE` |
| `GEO_CITY_COUNTRY_MISMATCH` | city implies a different country |
| `REFUND_NEGATIVE` | refund with a negative billing amount |
| `PURCHASE_POSITIVE` | purchase with a positive billing amount |
| `FX_RECONCILE_MISMATCH` | the two amounts do not reconcile at the stated rate |
| `FX_RATE_OFF_REFERENCE` | the stated rate is not plausible for its currency |
| `CODE_TYPE_MISMATCH` | label disagrees with its code |
| `MCC_ATM_MISMATCH` | raised in stage 8 |
| `AMOUNT_SIGN_RESTORED` | raised in stage 4 |

How many rows each one caught on the current file is in
[README](README.md#results-on-the-source-file), which is where a run's outcome
is recorded. Repeating it here would mean two copies drifting apart.

### The amount is stated twice

`TXN_AMOUNT` in local currency and `BILLING_AMOUNT` in USD, with `FX_RATE`
between them, so the two have to reconcile:

```python
expected = df["TXN_AMOUNT_CLEANED"].abs() * rate
drift = (expected - billed.abs()).abs() / expected
```

Magnitudes, not signed values: the local amount is signed by transaction type
and the billing amount by its own convention, so comparing them signed would
flag every refund. The direction is what the sign checks above are for.

The rate multiplies rather than divides. That is worth stating because the
wrong direction hides in plain sight — on the 1090 USD rows the rate is 1 and
both directions agree. Across the file, multiplying reconciles 2244 of 2296
rows; dividing reconciles only those 1090.

`FX_TOLERANCE` is relative, at 1%. `FX_RATE` is stored to six decimals, so a
large transaction reconciles to within a rounding error rather than exactly,
and a fixed tolerance would flag every large row or miss every small one across
the four orders of magnitude this file spans.

**48 rows do not reconcile**, and none of them are among the 13 signed rows —
these are a separate defect. 23 have a billing amount exactly 1/100 of what the
rate implies; the rest are off by 5–35%, consistent with a stale rate.

### A row can be perfectly consistent and still wrong

Reconciliation only proves a row agrees with itself. A transaction that states
a dead exchange rate **and** a billing amount computed from that dead rate
reconciles exactly, and is wrong by a factor of twenty. Nothing inside the row
can reveal that, so the check needs an outside reference — `fx_rates.json`,
one rate per currency, expressed as units per USD.

The reference is deliberately **not** used to recompute `BILLING_AMOUNT`. The
file covers March–September 2022 and rates move over seven months: EUR ranges
1.0631–1.1098, GBP 1.2393–1.2907. Applying one fixed rate across that window
flags 820 rows at 1% tolerance and 393 even at 10%, nearly all of them
perfectly correct transactions priced on a different day. So the reference
answers only the narrow question it can answer: *is this row's own rate
plausible for this currency at all?*

`FX_REFERENCE_TOLERANCE` is 15% for the same reason — wide enough that
ordinary movement never trips it. Every floating currency in the file stays
within 4.3% of its own median, so the check is silent on fifteen of the
sixteen currencies.

**It fires on 481 rows, all of them LBP.** 100 of those still state the
1507.5 official peg, abandoned in practice years before these transactions;
the rest range from 26,000 to 38,000 to the dollar. The reference is set at
89,500, so every LBP row is flagged rather than only the stale-peg ones — see
`_lbp_note` in the rule file, which is one number away from the alternative.

> **Why this one is flagged and not repaired.** Three values and one equation:
> the amount, the rate, or the billing figure could each be the wrong one, and
> the arithmetic cannot say which. Repairing would mean inventing a number.
> Stage 4's sign restoration is repaired precisely because it does not have
> this problem — the transaction type says unambiguously which value is wrong.

### Flags accumulate, they do not overwrite

```python
flags = existing.map(lambda v: [f for f in str(v).split(";") if f])
```

Stage 8 already wrote `MCC_ATM_MISMATCH`. Starting from a blank column here
would silently erase it.

### Why flag and not fix

The 6 impossible settlement dates settle **one day before** the transaction. All
are `Match`/`Settled` with times spread across the day, so a timezone boundary
does not explain them.

> We cannot know **which of the two dates is wrong**, so correcting either would
> be a guess.

### The one legitimate reason to drop a row

A null in the required set — `TXN_ID`, `ACCOUNT_ID`, `TXN_DATE_TIME`,
`TXN_AMOUNT`, `TXN_CCY`. This file has none. That is a hard failure, not a
cleaning task.

No row is ever dropped for a null anywhere else. Deleting 67 complete
transactions to fix one descriptive field would destroy more than it repairs.

---

## Five rules that apply everywhere

### 1. Never overwrite a source column

Every stage adds a `*_CLEANED` column beside the original rather than writing
over it. Applied without exception:

| Field | Original | Derived |
|---|---|---|
| Amount | `TXN_AMOUNT` (text) | `TXN_AMOUNT_CLEANED` (float) |
| MCC | `MCC_CODE` | `MCC_CODE_CLEANED` + `MCC_CONFIDENCE` |
| Country | `MERCHANT_COUNTRY` | `MERCHANT_COUNTRY_CLEANED` |
| Settlement | `SETTLE_DATE` | `SETTLE_DATE_CLEANED` + `SETTLE_DATE_STATUS` |

The suffix comes off on the way out — sheets are written lowercase and
unsuffixed — but it exists inside the pipeline so both versions can sit side by
side while a stage decides.

`raw_transactions` ships all 19 source columns byte-for-byte, joinable on
`TXN_ID`, so the workbook is self-auditing. The cleaned sheets then show **only**
the cleaned columns — a text `TXN_AMOUNT` beside a float one invites a total
computed from the wrong column.

### 1b. The cleaned sheet is one column per source column

Exactly one: the cleaned counterpart where a stage produced one, the original
where none did, and nothing else. Every column on that sheet answers *what
does this transaction say*.

Everything a stage derives that is not a cleaned counterpart — statuses,
confidences, precisions, coverage flags, the projected balance — answers a
different question, *how do you know*, and is answered in two other places
instead: the totals in `cleaning_report`, and the per-row detail in
`AUDIT_COLUMNS`. Nothing is discarded; `INTERNAL` in `utils/columns.py` is a list of what the writer
hides, not of what the pipeline forgets.

One exception, and it is not really one: `TXN_TS_PRECISION` passes through to
the writer and is dropped by it. The writer needs it to know whether a row may
be written with a time of day at all. Hiding it a step earlier would put
`00:00:00` on all 18,592 date-only rows — a reading the source never gave, and
indistinguishable on the sheet from the 51 transactions that really did happen
at midnight.

### 2. Rules in JSON, logic in Python

| File | Holds |
|---|---|
| `processors.json` | processor prefixes |
| `date_formats.json` | format patterns + null tokens |
| `processing_codes.json` | ISO codes → labels |
| `fx_rates.json` | Currency → units per USD |
| `city_aliases.json` | city spellings + e-commerce markers |
| `city_countries.json` | 105 city → country |
| `mcc_rules.json` | catch-all, suspect codes, thresholds |
| `merchants.json` | 270 merchants, 336 aliases, 137 asserted MCCs |

Vocabularies change as new data arrives; the code applying them does not.
Updating a vocabulary needs no logic review, and non-engineers can read it.

They live **inside** the package, because `pip install` would leave them behind
otherwise and every import would break in production.

### 3. Dates are datetimes until the moment of writing

Held as `datetime64` in the frame; `dd-mm-yyyy hh:mm:ss` is applied only in
`write_workbook`. As text, `"09-07-2022"` sorts before `"10-01-2021"` because
strings compare character by character, and every sort, filter and date
calculation silently breaks.

---

### 4. A value is repaired in place only when the data says which column is wrong

Two things meet that bar. An MCC the merchant's own transaction history
contradicts, and the sign a purchase lost when its amount was written as text:
in both, a second field settles which value is the wrong one.

Everything else is flagged and left alone. Where the amount and the billing
amount fail to reconcile, any of the three values involved could be the liar
and the arithmetic cannot say which, so the pipeline says so and stops. The
distinction is not caution for its own sake — it is whether the data contains
the answer.

### 5. Every step reports, and none of them counts

`CleaningReport` is passed to every stage and accumulates across the run.
Without it, a step that quietly nulls 400 rows is indistinguishable from one
that changed nothing. It is also the only place a run's outcome is recorded,
which is what lets this document stay free of counts.

What it no longer is, is something a stage writes to while it runs. Every
entry is derived after the last stage finishes, from the diagnostic columns
described in [the shared contract](#mark-dont-count). Collecting in step order
is what keeps the report reading the same as it always did.

---

## Adding a stage

1. Subclass `BaseCleaner`, implement `apply`, set `name`.
2. Have `apply` write a diagnostic column for anything you want counted, and
   implement `metrics` to derive those counts from it. Do not reduce inside
   `apply` — see [Mark, don't count](#mark-dont-count).
3. Put any vocabulary in `rules/json/` and add a `loader` function.
4. Insert it in `DEFAULT_STEPS` at the point its dependencies allow.
5. If it produces a review queue, expose `review_queue()` and add the sheet in
   `build_sheets`.
6. Add derived columns to `PRESENTATION_ORDER` in `utils/columns.py`, and any
   diagnostic column to `INTERNAL` and `AUDIT_COLUMNS`.

The orchestrator needs no edit — it holds steps in a list and runs them.

## Adding a rule file

1. Put the JSON in `src/rules/json/`, with a `_comment` recording where the
   values came from and what would make them wrong.
2. Add a loader function in `rules/loader.py` returning the shape the caller
   actually needs, not the shape the file happens to have — `city_aliases`
   inverts canonical→variants into variant→canonical for exactly this reason.
3. Add a staleness test. A rule file that has quietly stopped matching
   anything is the failure mode that never announces itself.

### What a test should assert

| Assert | Not |
|---|---|
| the awkward real values from this file | invented round numbers |
| a planted violation is caught | that the count is currently 0 |
| every rule-file key still matches a live row | that the file parses |

The staleness guards matter most: `merchants.json` keys match *cleaned* names,
which shift as `MerchantCleaner` improves, so a dead entry must fail a test
rather than silently do nothing.

---

## The parity harness

The cleaning stages are being ported to Spark. The harness is what makes that
survivable: after every stage, a machine checks that the two engines give the
same answer, on a sample chosen for the cases the stages actually get wrong.

Four modules under `src/spark/`, none of which cleans anything:

| Module | Answers |
|---|---|
| `session.py` | how a `SparkSession` is configured, everywhere |
| `source.py` | how a delimited file is read, deciding nothing |
| `sample.py` | which rows the harness runs on |
| `parity.py` | whether two frames say the same thing |
| `pipeline.py` | which stages are ported, and in what order they run |

### The session states its semantics rather than inheriting them

Four settings change *answers* rather than speed, and all four are written
down even where the written value equals today's default:

| Setting | Value | Because |
|---|---|---|
| `spark.sql.session.timeZone` | `UTC` | every parsed timestamp is read in it, so an unset one makes the output a property of the laptop |
| `spark.sql.ansi.enabled` | `false` | ANSI is on by default in Spark 4 and makes a bad cast *raise*; this pipeline counts bad casts |
| `spark.sql.legacy.timeParserPolicy` | `CORRECTED` | the strict parser yields null instead of coaxing a date out of `2022-13-45` |
| `spark.sql.shuffle.partitions` | `8` | 200 tasks over 265k rows is scheduling overhead with a job attached |

Inheriting a default is a bet that it will not change between versions, and
two of these have already changed once.

### The reader decides nothing

Same discipline as `utils/io.py`, for the same reason: `inferSchema` would
sample the file, guess a type, and coerce on the way in — which pre-empts the
coercion the pipeline is required to perform explicitly and count. Every
column arrives as a string, and every type in the output is a cast some stage
made on purpose.

The schema is derived from the file's own header rather than written out as 22
literal names, and the file is then read with `enforceSchema=false` — so Spark
compares the header it finds against the schema it was given and refuses a
file whose columns moved. Under the default it would apply the schema by
position and hand every stage the column to the left of the one it asked for.

### The sample is chosen, not taken

The first 500 rows are not a sample, they are January. Two properties decide
the design:

**It samples accounts, not rows.** `BalanceReconstructor` works over
`ACCOUNT_ID` ordered by `TXN_SEQ`. Take every third row and every chain
breaks, every gap becomes unclosable, and the stage reports `UNVERIFIED` where
the real run reports `DERIVED` — in *both* engines, so the comparison passes
having proved nothing. A selected account brings all of its rows.

**The accounts are chosen for what they contain:** the widest `TXN_SEQ` spans
(the accounts that cross the balance seam), the narrowest (the ones that live
on one side of it), accounts naming a trap-pair merchant, a greedy cover of
the distinct `TXN_DATE_TIME` shapes, and the accounts withholding the most
balances — then a deterministic fill.

Deterministic rules out `hash()`, which is salted per process, and
`DataFrame.sample`, whose seeding is a numpy detail. The fill orders accounts
by a SHA-256 of the account id. The same source file produces the same 16
accounts and the same 11,417 rows on every machine — the same requirement the
upsert key carries, for the same reason: a sample that drifts turns one
failing stage into a bug report nobody can reproduce.

### What counts as a difference

The whole design problem is the line between a real divergence and a
difference in representation, and it is drawn once here so no stage's test
has to draw it again.

| Not a finding | Is a finding |
|---|---|
| row order — Spark's is a function of partitioning | a changed value |
| column order — imposed by `presented()` at the end anyway | a missing column |
| dtype — `float64` or `double` or the text of the number | a null against a value |
| `Categorical` storage — the labels are the answer | **a null against an empty string** |
| float noise below tolerance — Spark sums in partition order | a float beyond it |

The last row of the left column and the last of the right are the same
distinction seen twice. Money is rounded to the cent by the cleaners
themselves, so anything the tolerance forgives is representation; and
null-versus-empty-string is the distinction this pipeline exists to preserve,
so it is never forgiven.

Rows are aligned on a key — `TXN_SEQ`, then `TXN_ID_CLEANED` — by an index
join rather than by sorting both sides, because sorting silently mis-pairs
every row after the first missing one where a join reports it as missing.

The output is a report, not a boolean: *"`BALANCE_STATUS` differs on 41 rows,
here are five with their keys"* is actionable where *"not equal"* is not.

### How it grows

`SPARK_STEP_REGISTRY` in `src/spark/pipeline.py` is the ledger of what has
been ported. `ported()` returns the longest *prefix* of a profile's steps that
is registered — a prefix and not a filter, because the stages are not
independent: `amounts` signs by what `codes` resolved, `balance` moves by what
`amounts` parsed.

`tests/test_parity.py` runs that prefix through both engines and compares. It
is never edited as the port proceeds: a stage becomes tested by being
registered, and registering one that is not finished turns its parity test red
immediately, which is the intended direction.

With the registry empty it compares the two readers — which is not a vacuous
assertion. It says that 11,417 rows and 22 columns of a genuinely dirty
extract arrive identically through two entirely different readers, including
which cells are null and which are the empty string.

---

## The streaming path

The batch pipeline cleans a file. The streaming path cleans a *row*, on being
told one arrived, and it exists to answer a different question: not "clean this
extract" but "keep this table clean as things land in it".

Everything about it is arranged so that it is the same pipeline. `run_rows`
sits beside `run` in `src/runner.py` and differs in where the rows come from
and nothing else — same profile detection, same steps, same policy, same
contract, same upsert. Writing a second pipeline for the streaming case would
have made "the cleaning is identical" a claim rather than a fact.

### Why the landing table is all TEXT

`raw_transactions` declares its twenty-two source columns as `TEXT`, which
looks like laziness and is the opposite. It is the same contract
`spark_setup.read_csv` enforces with `inferSchema` off and `src/utils/io.py`
with `dtype=object`: **the reader must not decide** what `""` or `"NA"` or
`"5.727.580,00"` mean.

A `NUMERIC` column here would reject an unparseable amount at the `INSERT` —
before any stage could mark it, and therefore before the cleaning report could
count it. Requirement 2 asks for unreadable values to be *counted*, not
guessed and not refused, and a landing table that validates makes that
impossible in the one place it matters most. Typing happens exactly once, on
the way out, in `src/db/contract.py`.

The one place the frame and the table deliberately disagree is the empty
string. `read_csv` sets `nullValue=""`, so a blank field reaches the stages as
null — and the `missing` stage counts nulls. A blank read back from the table
as `""` would not be null, would not be counted, and the same transaction
cleaned through the file and through the table would report differently. So
`raw.read` applies `NULLIF(col, '')`. The table keeps the two apart, because a
person querying it should see what actually arrived; the frame does not,
because the batch path cannot either.

### Why the id, and only the id, travels

The Kafka message carries a row id and no transaction data. That is the same
rule the completion event follows — Kafka carries the *event*, the rows go to
Postgres — and it has a specific payoff here: the message cannot go stale.
A payload holding the transaction would be a second copy of a row that the
database also holds, and the two could disagree the moment anybody corrected
one. An id can only be right or absent.

It is a JSON object rather than a bare integer for three reasons, all of which
are about the second year of the system's life rather than the first: a
consumer holding `42` cannot tell what it is holding, there is nowhere to put
a version, and a topic full of naked integers cannot be debugged with
`kafka-console-consumer`.

### The commit is the design

Offsets are committed after Postgres has committed, never before, and
`enable.auto.commit` is off precisely so that this is possible. Auto-commit
commits on a timer whether or not the row was dealt with, so a consumer that
dies mid-clean has already told Kafka it finished and the row is cleaned by
nobody.

Committing last inverts the failure: a crash redelivers a row rather than
skipping one. That is only safe because redelivery is a no-op, which is
`src/jobs.py`'s doing — the job id is derived from the ids rather than
generated, so a second pass writes the same rows with the same values under
the same identity. At-least-once delivery plus an idempotent write is the
whole guarantee, and neither half is sufficient alone.

### Why a failed row is marked and skipped

Three options exist when a row will not clean: halt, retry, or record and move
on. Halting lets one unparseable row stop the other thousand. Retrying is
worse than it sounds — the same row through the same code fails the same way,
so a retry loop is a consumer that has stopped consuming.

So the row is marked `FAILED` with its reason in `last_error`, the offset is
committed, and the consumer carries on. Nothing is lost: the row is still in
`raw_transactions`, and re-emitting its id is how it is retried once the cause
is fixed. `status` is a *record* and not a work queue, for the same reason —
Kafka is the queue, and a consumer polling this column instead would be a
second, competing source of truth about what is outstanding.

A batch that fails is retried one row at a time. That is slow, and it only
happens on a path that was already going wrong; what it buys is that the
failure lands on the row that caused it instead of on the forty-nine that
travelled with it.

### Why a message that will not decode goes to a file

The section above works because a failed row *has an id*. `FAILED` is written
against a primary key, `last_error` explains it, and `--ids 42` retries it.
Every part of that recovery hangs off the id.

A message that will not decode has none. That is what is wrong with it: the
bytes are not JSON, or are JSON of the wrong shape, or carry an `id` that is
not a whole number — so there is no row in `raw_transactions` to mark and
nothing to re-emit. The status column cannot express this failure, not as a
matter of preference but of structure, and the third value it would need
(`UNDECODABLE`, belonging to no row) would be a column describing something
that is not a row.

So it goes to `data/audit_trail/undecodable.jsonl`, configured at
`kafka.consumer.audit_trail`. Before this it was printed and stepped over and
the offset committed, which meant the sole evidence that a message ever
arrived was a line on stdout that a restart erased.

The record holds the bytes (base64 — a message that failed to decode is by
definition not guaranteed to be UTF-8), the topic, partition and offset, and
the error. The coordinates are what make the file a pointer rather than only a
copy: the original can be re-read from the log while it is still within
retention.

JSON Lines rather than a JSON array, because an array must be read, parsed and
rewritten whole on every bad message — slower, and it turns a crash mid-write
into a corrupted file instead of a torn last line.

Two properties are deliberate and both are constraints rather than features.
The write cannot raise: a full disk must not turn one malformed message into a
stopped consumer, which would be strictly worse than the skipping it replaced,
so `record` returns False and the loop says `NOWHERE` on the line it was
already printing. And it happens **before** the commit, for the same reason
the Postgres write does — the offset is the promise the message was dealt
with, so quarantining after it would let a crash in between lose the only
copy. Erring the other way appends a duplicate record, which the offset in it
makes obvious; the file has no upsert to make a redelivery a no-op the way
Postgres does, and a duplicate line is the cheaper failure.

What it is not is durable in the way Postgres is. The file is local to the
process that wrote it: it does not survive a container rebuild unless the
configured path is on a mounted volume, and two consumer replicas would keep
two files nobody joins. Acceptable for a single-consumer pipeline, and the
reason the path is configuration rather than a constant. A dead-letter *topic*
is the textbook alternative and was not built, because it would mean another
topic to create and something to consume it, added to demonstrate a pattern
rather than to solve a problem this pipeline has.

### Why the consumer runs on one thread

`local[*]` is right when there is data to divide across cores. A batch of two
rows has none, and every additional local thread is another Python worker
process for Spark to start — which `spark_setup` already documents as slow
enough on Windows to need the worker socket timeout raised to 120 seconds.

Measured on the development machine, two rows through the eleven stages:

| master | pass 1 | pass 2 | pass 3 |
|---|---|---|---|
| `local[*]` | 469.9s | 453.0s | 679.4s |
| `local[1]` | 105.3s | 71.0s | — |

Not warm-up — the third pass is as slow as the first. It is fixed per-job
overhead, and on a batch this small the overhead *is* the run. So the consumer
builds its session as `local[1]` with one shuffle partition. The batch path is
untouched: `session()` takes a master because the right thread count is a
property of the work and not of the project.

### Why the stage log costs an action, and caches

`report()` says what a run did once it is over, which is right for a batch job
and useless for watching one row. The stage log is the other view — each stage
announced as it finishes, with its own metrics evaluated at that point.

Those metrics are real, which means each one is a Spark action, which means a
counted run is one pass per stage rather than one pass. That is affordable on a
two-row batch and ruinous on 265k, which is why `counts` is a switch and why
the batch path leaves it off.

It also means each measured frame must be cached. Measuring after stage *n*
runs stages 1..n; measuring after *n+1* runs 1..n+1 **from the source again**,
because nothing kept the intermediate. Uncached, two rows through eleven
counted stages did not finish in ten minutes and had reached Spark job 54. The
log caches each frame it measures and releases the previous one once the next
has been computed, so peak memory is two stages' worth of one batch.

### Why the consumer stops its own Spark session every fifty batches

Every other Spark decision in this project was made against the question "what
does this cost on 265k rows". A batch run answers it once and exits, taking
the driver with it. The consumer asks a different question — what does this
cost after the four hundredth batch — and nothing had been designed against
it.

It runs out of memory. Not from a leak in this code: cached blocks and
broadcast relations stay reachable until the plans referencing them are
collected, the status listeners keep a history whose entries are query plans,
and the Python worker pool holds processes. Every one of those is small,
bounded, and defensible on its own; the sum over hundreds of batches is a
driver whose heap is mostly the record of work that finished an hour ago.

Observed rather than predicted. One consumer session reached Spark stage 1777
with 673 live broadcasts, spent 872 seconds on a batch of **one row**, and
then died — first "not enough memory to build and broadcast the table" while
joining a single row against a lookup table, then `OutOfMemoryError` on every
action, then six more batches "failing" in five seconds each without doing any
work, marking six perfectly good rows FAILED on the way past.

Three changes, and only one of them is a fix:

| change | what it does |
|---|---|
| `spark.ui.retained*` and `spark.sql.ui.retainedExecutions` capped at 20 | slows the growth |
| `autoBroadcastJoinThreshold = -1` on the consumer session | slows the growth |
| stop and rebuild the session every `renew_every` batches | **bounds** it |

The first two are worth having and neither is sufficient, because both reduce
the slope and leave the driver's footprint a function of uptime. Only the
rebuild resets that function. Stopping does not restart the JVM — the py4j
gateway outlives it and the heap is the same heap — but it makes the entire
`SparkContext` object graph unreachable, which is the weaker and truer claim:
the collector is given somewhere to go.

It happens after the offset is committed, where nothing is outstanding. Before
the commit there would be a several-second window in which a crash redelivers
a batch that Postgres already has — harmless, because the write upserts, but
harmless by luck rather than by arrangement.

The separate half of the same problem is the first failure rather than the
hundredth. `spark_setup.is_fatal` recognises an `OutOfMemoryError` anywhere in
a cause chain, the stage log re-raises instead of reporting it as a missing
metric, and `handle` raises `SessionLost` rather than writing a Java stack
trace into the `last_error` column of a row it never read. The offset is not
committed, so the batch is redelivered to a consumer that can actually clean
it.

### What is deliberately not done

**The balance anchor.** The `balance` stage chains a running balance per
account in sequence order. A single row has no earlier transaction to count
from, so the stage states no figure and the column is null — which the schema
permits and the log reports as `adjusted[NO_ANCHOR]`. Seeding the anchor from
`cleaned_transactions` is the correct fix and `idx_cleaned_account_seq` exists
for exactly that lookup. It is left undone rather than half-done: a balance
computed from a partial chain would be a number nobody can defend, and a null
is at least honest about what was not known.

**A real emitter.** `scripts/dummy_producer.py` publishes because a person ran
it. In a deployment the event would come from whatever writes the row — a
trigger, an outbox poller, a CDC reader on the write-ahead log — and all three
produce exactly the message this script produces. That is the point of the
message being an id and a version rather than a payload shaped around this
script: replacing the producer changes nothing downstream.
