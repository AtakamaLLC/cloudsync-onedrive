SHELL := /bin/bash
ifeq ($(OS),Windows_NT)
	ENVBIN="scripts"
else
	ENVBIN="bin"
endif

BASE := $(shell git merge-base HEAD origin/master)

env:
	virtualenv env

requirements: env
	. env/$(ENVBIN)/activate && pip install -r requirements.txt

lint: requirements
	. env/$(ENVBIN)/activate && pylint *.py
	. env/$(ENVBIN)/activate && mypy .

test: requirements
	. env/$(ENVBIN)/activate && flit install
	. env/$(ENVBIN)/activate && pytest -v --cov=. --cov-report=xml --durations=1 -n=2 --provider=onedrive,testodbiz tests

format:
	autopep8 --in-place -r -j 8 cloudsync/

coverage:
	diff-cover coverage.xml --compare-branch=$(BASE)
