# Variables
# Overridable, so a specific interpreter can be used without editing this file:
# `make test PYTHON=.venv/Scripts/python.exe`. Plain `python` when the virtual
# environment is already activated.
PYTHON ?= python

# Recipes below never chain with `&&`. Make runs each recipe line in its own
# shell, and on Windows that shell may be cmd.exe or PowerShell, neither of
# which accepts `&&` the way sh does. One command per line instead.
#
# Deletions go through Python rather than `find`/`rm -rf` for the same reason:
# those are sh-only, and Python is guaranteed present in a Python project.

.PHONY: help run run-pandas run-dry test test-fast parity sample verify kafka-topic db-migrate db-reset seed-raw clean clean-pyc clean-build

# `echo` is not portable either: cmd.exe prints the surrounding quotes that sh
# strips, so the help text goes through Python too.
help:
	@$(PYTHON) -c "print('Usage:\n  make run         Run the cleaning pipeline (main.py)\n  make test        Run the test suite\n  make test-fast   Run it without the tests that start a JVM\n  make verify      Check Java, Spark, Postgres and Kafka are up\n  make sample      Cut (or re-cut) the parity sample\n  make parity      Run the pandas-vs-Spark parity tests only\n  make seed-raw    Insert raw rows, print their ids (N=3 DIRTY=1)\n  make clean       Remove cache and build artifacts\n  make clean-pyc   Remove Python bytecode caches\n  make clean-build Remove pytest/mypy caches')"

# Run the pipeline on the configured engine, which is spark: read the extract,
# clean it, upsert to Postgres. Needs the stack up -- `make verify` first.
run:
	$(PYTHON) main.py

# The same source through the Stage 1 pandas path, producing the multi-sheet
# workbook instead of database rows. Needs no JVM and no containers, which is
# what makes it the way to check a cleaning change quickly.
run-pandas:
	$(PYTHON) main.py --engine pandas

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

# The parity harness on its own: the pandas pipeline and the Spark pipeline
# over the same sample, compared column for column.
parity:
	$(PYTHON) -m pytest -q tests/test_parity.py

# Cut the parity sample from the full extract. Not normally needed -- the
# harness cuts it on first use and rebuilds it when the source changes -- but
# useful after editing the sampling strategy, which the manifest's version
# check would otherwise catch only on the next test run.
sample:
	$(PYTHON) -c "from tests.harness.sample import build; print(build())"

# Does this machine run the Stage 2 stack? Every failure names its own fix.
verify:
	$(PYTHON) -m scripts.verify_env

# Create the completion-event topic if the broker does not have it. Auto-create
# is off on purpose -- a producer aimed at a typo'd topic should fail rather
# than quietly invent one -- so the topic has to be made deliberately. The
# runner does this before every emit too; this target is for setting a fresh
# broker up without running the pipeline.
kafka-topic:
	$(PYTHON) -c "from src.kafka import producer, settings; b = settings.load(); print('created' if producer.ensure_topic(b) else 'already there', b.topic)"

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
seed-raw:
	$(PYTHON) -m scripts.seed_raw --count $(or $(N),1) $(if $(DIRTY),--dirty,)

# Drop all three tables and rebuild them. This is how a column added to
# sql/schema.sql reaches a database that already exists, because IF NOT EXISTS
# will not alter a table it finds. Destructive, and separate from db-migrate
# for that reason -- note that it takes raw_transactions with it, so rows
# seeded or typed in by hand are gone and their ids mean nothing afterwards.
db-reset:
	$(PYTHON) -c "from src.db import migrate, settings; migrate.recreate(settings.load()); print('schema rebuilt')"

# Remove Python bytecode caches, skipping the virtualenv
clean-pyc:
	$(PYTHON) -c "import pathlib, shutil; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').rglob('__pycache__') if '.venv' not in p.parts]"
	$(PYTHON) -c "import pathlib; [p.unlink() for pat in ('*.pyc', '*.pyo') for p in pathlib.Path('.').rglob(pat) if '.venv' not in p.parts]"

# Remove test and type-checker caches
clean-build:
	$(PYTHON) -c "import shutil; [shutil.rmtree(d, ignore_errors=True) for d in ('.pytest_cache', '.mypy_cache')]"

# Remove everything regenerable
clean: clean-pyc clean-build
