# Variables
# Overridable, so a specific interpreter can be used without editing this file:
# `make test PYTHON=.venv/Scripts/python.exe`. Plain `python` when the virtual
# environment is already activated.
PYTHON ?= python

.PHONY: help fingerprint run run-dry test test-fast sample verify kafka-topic db-migrate db-reset db-rules seed-raw emit consumer features features-reset features-scale features-test features-check clean clean-pyc clean-build

help:
	@$(PYTHON) -c "print('Usage:\n  make run         Clean the extract on Spark and upsert it (main.py)\n  make test        Run the test suite\n  make test-fast   Run it without the tests that start a JVM\n  make verify      Check Java, Spark, Postgres and Kafka are up\n  make sample      Cut (or re-cut) the test sample\n  make seed-raw    Insert raw rows, print their ids (N=3 DIRTY=1 OFFSET=5000)\n  make emit        Announce a row to Kafka (ID=42 or PENDING=1)\n  make fingerprint One line describing cleaned_transactions\n  make consumer    Listen and clean arriving rows (ONCE=1)\n  make db-rules    Seed the Stage 3 rule tables from src/rules/json/\n  make features    Build the Stage 3 feature table (upserts Postgres)\n  make features-reset  Drop the feature table and build it again\n  make features-scale  Rebuild it on the 5x source and report the timings\n  make features-check  The Stage 3 guards that need no JVM\n  make clean       Remove cache and build artifacts\n  make clean-pyc   Remove Python bytecode caches\n  make clean-build Remove pytest/mypy caches')"

# Run the pipeline on the configured engine, : read the extract,
# clean it, upsert to Postgres. Needs the stack up, `make verify` first.
run:
	$(PYTHON) main.py

# Read, clean and report without writing anything. The state you want when the
# question is whether the cleaning is right rather than whether the write is.
run-dry:
	$(PYTHON) main.py --dry-run

# Run the test suite
test:
	$(PYTHON) -m pytest -q

# The same suite without the tests that start a JVM. Several seconds of Spark
# startup is worth paying when the port is what you are working on and not
# otherwise, and the marker is what makes that a choice rather than a habit.
test-fast:
	$(PYTHON) -m pytest -q -m "not spark"

# Cut the test sample from the full extract. Not normally needed -- the
# harness cuts it on first use and rebuilds it when the source changes -- but
# useful after editing the sampling strategy, which the manifest's version
# check would otherwise catch only on the next test run.
sample:
	$(PYTHON) -c "from tests.harness.sample import build; print(build())"

# Does this machine run the Stage 2 stack? Every failure names its own fix.
verify:
	$(PYTHON) -m scripts.verify_env

# Create both topics if the broker does not have them. Auto-create is off on
# purpose - a producer aimed at a typo'd topic should fail rather than quietly
# invent one - so the topics have to be made deliberately. The runner and the
# dummy producer each do this before publishing too; this target is for setting
# a fresh broker up without running anything.
kafka-topic:
	$(PYTHON) -c "from src.kafka import producer, settings; b = settings.load(); [print('created' if producer.ensure_topic(b, t) else 'already there', t) for t in (b.topic, b.raw_topic)]"

# Announce that a row arrived, so the consumer picks it up and cleans it.
# `make emit ID=42`, or `make emit PENDING=1` to re-announce every row the
# consumer has not reported on -- the recovery path after it was down.
emit:
	$(PYTHON) -m scripts.dummy_producer $(if $(PENDING),--pending,--id $(ID))

# Listen for arriving rows and clean each one. `make consumer ONCE=1`
# cleans the first batch and exits, which is the version to run when you want
# to see the flow rather than leave it running.
consumer:
	$(PYTHON) consumer.py $(if $(ONCE),--once,)

# One line describing the whole cleaned table: counts, total, and a digest over
# the key and the amount. Run it, replay an event, run it again -- everything
# but `last write` is identical if the upsert was idempotent, which is the
# claim, and `last write` moving is the proof it really did write again rather
# than skipping the row.
fingerprint:
	$(PYTHON) -m scripts.fingerprint

# Create cleaned_transactions, its indexes and the staging table. Idempotent --
# every statement is IF NOT EXISTS -- so running it against a database that is
# already set up does nothing. The writer calls it before each run too; this
# target is for setting a fresh container up without running the pipeline.
db-migrate:
	$(PYTHON) -c "from src.db import migrate, settings; migrate.migrate(settings.load()); print('schema applied')"

# Insert rows from the extract into raw_transactions and print their ids --
# the manual "a transaction arrived" step of the streaming path, made
# repeatable. `make seed-raw N=3` for three rows; DIRTY=1 picks rows a stage
# will visibly change, which is what you want when the point is to watch the
# cleaning happen rather than to watch it find nothing.
#
# The scan starts at the top of the extract every time, so the same N rows come
# back on every run -- resetting the database does not change which rows the
# file offers first. OFFSET=5000 skips that many source rows before taking any,
# which is how you seed different transactions on a second run.
seed-raw:
	$(PYTHON) -m scripts.seed_raw --count $(or $(N),1) --offset $(or $(OFFSET),0) $(if $(DIRTY),--dirty,)

# Drop all three tables and rebuild them. This is how a column added to
# sql/schema.sql reaches a database that already exists, because IF NOT EXISTS
# will not alter a table it finds. Destructive, and separate from db-migrate
# for that reason -- note that it takes raw_transactions with it, so rows
# seeded or typed in by hand are gone and their ids mean nothing afterwards.
db-reset:
	$(PYTHON) -c "from src.db import migrate, settings; migrate.recreate(settings.load()); print('schema rebuilt')"

# Create the Stage 3 rule tables and load them from src/rules/json/. Idempotent
# in the sense that matters: the seed replaces the contents, so a rule the JSON
# no longer declares does not survive in the table where a build would read it.
db-rules:
	$(PYTHON) features_main.py --seed-rules

# Build the feature table. Reads cleaned_transactions and the rule tables from
# Postgres, runs the whole build on Spark, and upserts the result into
# feature_store_monthly. `make features RULES=json` still reads the vocabularies from src/rules/json/
# rather than the rule tables, which is the one remaining escape hatch and is
# about the rules, not the data.
features:
	$(PYTHON) features_main.py --rules $(or $(RULES),db) $(if $(NODB),--no-database,)

# Drop the feature table and build it again. This is how a column added to or
# removed from src/features/contract.py reaches a database that already has the
# table, because CREATE TABLE IF NOT EXISTS will not alter one it finds -- a
# removed column would otherwise survive holding the previous build's values,
# which looks like data and is not. Destructive, and separate from `features`
# for that reason.
features-reset:
	$(PYTHON) -c "from src.db.settings import load, connect; from src.features import settings as fs; t = fs.load().database.table; c = connect(load()); c.cursor().execute(f'DROP TABLE IF EXISTS {t}'); c.cursor().execute(f'DROP TABLE IF EXISTS staging_{t}'); c.commit(); print(f'dropped {t}')"
	$(MAKE) features

# The scaling run deliverable 4 asks for: the same build over a source
# replicated to five times the users, so the timings can be compared against
# the ordinary run above rather than read on their own.
features-scale:
	$(PYTHON) features_main.py --rules $(or $(RULES),db) --scale $(or $(FACTOR),5) --no-database

# The Stage 3 tests on their own: the point-in-time rule, the aggregation, the
# unit decision and the rule vocabularies. These start a JVM -- the build is
# PySpark, so testing it means running Spark -- which is why the structural
# guards below are split out for the fast loop.
features-test:
	$(PYTHON) -m pytest -q tests/test_features_point_in_time.py tests/test_features_aggregation.py tests/test_features_artifacts.py tests/test_features_units.py tests/test_features_rules.py

# The guards that need no JVM: the contract, the units decision, and the scans
# that keep the build PySpark and the lag in one module. Seconds rather than
# minutes, so this is the loop to run while changing the feature build.
features-check:
	$(PYTHON) -m pytest -q tests/test_features_rules.py 	  tests/test_features_units.py::test_the_whole_build_is_pyspark 	  tests/test_features_units.py::test_nothing_in_the_build_collects_the_dataset 	  tests/test_features_units.py::test_no_module_but_windows_takes_a_lag 	  tests/test_features_units.py::test_no_window_in_the_build_reaches_forwards 	  tests/test_features_units.py::test_the_forbidden_columns_appear_nowhere_in_the_build 	  tests/test_features_units.py::test_the_source_reader_selects_only_usd_columns

# Remove Python bytecode caches, skipping the virtualenv
clean-pyc:
	$(PYTHON) -c "import pathlib, shutil; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').rglob('__pycache__') if '.venv' not in p.parts]"
	$(PYTHON) -c "import pathlib; [p.unlink() for pat in ('*.pyc', '*.pyo') for p in pathlib.Path('.').rglob(pat) if '.venv' not in p.parts]"

# Remove test and type-checker caches
clean-build:
	$(PYTHON) -c "import shutil; [shutil.rmtree(d, ignore_errors=True) for d in ('.pytest_cache', '.mypy_cache')]"

# Remove everything regenerable
clean: clean-pyc clean-build
