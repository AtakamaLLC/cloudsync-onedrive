SHELL := /bin/bash
ifeq ($(OS),Windows_NT)
	ENVBIN="scripts"
else
	ENVBIN="bin"
endif

env:
	virtualenv env

requirements: env
	. env/$(ENVBIN)/activate && pip install -r requirements.txt

lint:
	pylint .
	mypy .

test:
	. env/$(ENVBIN)/activate && pytest -v --cov=. --cov-report=xml --durations=1 -n=8 --full-trace --timeout=600 --provider=onedrive,testodbiz tests

format:
	autopep8 --in-place -r -j 8 cloudsync/
