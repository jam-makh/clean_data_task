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

.PHONY: help run test clean clean-pyc clean-build

# `echo` is not portable either: cmd.exe prints the surrounding quotes that sh
# strips, so the help text goes through Python too.
help:
	@$(PYTHON) -c "print('Usage:\n  make run         Run the cleaning pipeline (main.py)\n  make test        Run the test suite\n  make clean       Remove cache and build artifacts\n  make clean-pyc   Remove Python bytecode caches\n  make clean-build Remove pytest/mypy caches')"

# Run the pipeline
run:
	$(PYTHON) main.py

# Run the test suite
test:
	$(PYTHON) -m pytest -q

# Remove Python bytecode caches, skipping the virtualenv
clean-pyc:
	$(PYTHON) -c "import pathlib, shutil; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').rglob('__pycache__') if '.venv' not in p.parts]"
	$(PYTHON) -c "import pathlib; [p.unlink() for pat in ('*.pyc', '*.pyo') for p in pathlib.Path('.').rglob(pat) if '.venv' not in p.parts]"

# Remove test and type-checker caches
clean-build:
	$(PYTHON) -c "import shutil; [shutil.rmtree(d, ignore_errors=True) for d in ('.pytest_cache', '.mypy_cache')]"

# Remove everything regenerable
clean: clean-pyc clean-build
