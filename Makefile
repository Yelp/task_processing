ifeq ($(findstring .yelpcorp.com, $(shell hostname -f)), .yelpcorp.com)
	BUILD_ENV?=YELP
	export PIP_INDEX_URL?=https://pypi.yelpcorp.com/simple
else
	BUILD_ENV?=$(shell hostname -f)
endif

venv:
	tox -e venv

test:
	tox

tox_%:
	tox -e $*

itest:
	tox -e integration

docs:
	tox -e docs

pypi:
	tox -e pypi

clean:
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf .tox .taskproc
	rm -rf dist build
	rm -rf task_processing.egg-info
	rm -rf venv
