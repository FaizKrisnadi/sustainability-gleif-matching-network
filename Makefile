PYTHON ?= python

setup:
	$(PYTHON) -m pip install -e .[dev]

inspect:
	$(PYTHON) -m src.cli inspect-inputs

preprocess-level1:
	$(PYTHON) -m src.cli preprocess-gleif-level1

preprocess-level2:
	$(PYTHON) -m src.cli preprocess-gleif-level2 --parse-repex

build-source:
	$(PYTHON) -m src.cli build-sustainability-source

network:
	$(PYTHON) -m src.cli build-network

match:
	$(PYTHON) -m src.cli match-sustainability

sample-eval:
	$(PYTHON) -m src.cli sample-matching-eval

eval-report:
	$(PYTHON) -m src.cli matching-eval-report

network-sanity:
	$(PYTHON) -m src.cli network-sanity

test:
	$(PYTHON) -m pytest -q

run-all:
	$(PYTHON) -m src.cli run-all
