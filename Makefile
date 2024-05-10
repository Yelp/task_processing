ifeq ($(findstring .yelpcorp.com, $(shell hostname -f)), .yelpcorp.com)
	BUILD_ENV?=YELP
	export PIP_INDEX_URL?=https://pypi.yelpcorp.com/simple
else
	BUILD_ENV?=$(shell hostname -f)
endif

.PHONY: venv
venv:
	tox -e venv

.PHONY: test
test:
	tox

.PHONY: tox_%
tox_%:
	tox -e $*

.PHONY: docs
docs:
	tox -e docs

.PHONY: pypi
pypi:
	tox -e pypi

.PHONY: clean
clean:
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf .tox .taskproc
	rm -rf dist build
	rm -rf task_processing.egg-info
	rm -rf venv
