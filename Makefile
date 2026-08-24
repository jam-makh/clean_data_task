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

.PHONY: help run test test-fast parity sample verify clean clean-pyc clean-build

# `echo` is not portable either: cmd.exe prints the surrounding quotes that sh
# strips, so the help text goes through Python too.
help:
	@$(PYTHON) -c "print('Usage:\n  make run         Run the cleaning pipeline (main.py)\n  make test        Run the test suite\n  make test-fast   Run it without the tests that start a JVM\n  make verify      Check Java, Spark, Postgres and Kafka are up\n  make sample      Cut (or re-cut) the parity sample\n  make parity      Run the pandas-vs-Spark parity tests only\n  make clean       Remove cache and build artifacts\n  make clean-pyc   Remove Python bytecode caches\n  make clean-build Remove pytest/mypy caches')"

# Run the pipeline
run:
	$(PYTHON) main.py

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
	$(PYTHON) -c "from src.spark.sample import build; print(build())"

# Does this machine run the Stage 2 stack? Every failure names its own fix.
verify:
	$(PYTHON) -m scripts.verify_env

# Remove Python bytecode caches, skipping the virtualenv
clean-pyc:
	$(PYTHON) -c "import pathlib, shutil; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').rglob('__pycache__') if '.venv' not in p.parts]"
	$(PYTHON) -c "import pathlib; [p.unlink() for pat in ('*.pyc', '*.pyo') for p in pathlib.Path('.').rglob(pat) if '.venv' not in p.parts]"

# Remove test and type-checker caches
clean-build:
	$(PYTHON) -c "import shutil; [shutil.rmtree(d, ignore_errors=True) for d in ('.pytest_cache', '.mypy_cache')]"

# Remove everything regenerable
clean: clean-pyc clean-build
