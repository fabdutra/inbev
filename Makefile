PYTHON ?= python3

.PHONY: install install-dev format lint test run-bootstrap run-bronze run-silver run-gold

install:
	$(PYTHON) -m pip install -r requirements.txt

install-dev:
	$(PYTHON) -m pip install -r requirements-dev.txt

format:
	PYTHONPATH=src $(PYTHON) -m black src tests

lint:
	PYTHONPATH=src $(PYTHON) -m ruff check src tests

test:
	PYTHONPATH=src $(PYTHON) -m pytest

run-bootstrap:
	PYTHONPATH=src $(PYTHON) -m bees_breweries

run-bronze:
	PYTHONPATH=src $(PYTHON) -m bees_breweries --command bronze-ingest --max-pages 1

run-silver:
	PYTHONPATH=src $(PYTHON) -m bees_breweries --command silver-transform

run-gold:
	PYTHONPATH=src $(PYTHON) -m bees_breweries --command gold-transform
