.PHONY: all test dev_env docs

TOX=".tox/dev/bin/tox"

ifeq ($(findstring .yelpcorp.com, $(shell hostname -f)), .yelpcorp.com)
	BUILD_ENV?=YELP
	export PIP_INDEX_URL?=https://pypi.yelpcorp.com/simple
else
	BUILD_ENV?=$(shell hostname -f)
endif

test: dev_env
	${TOX}

dev_env:
	mkdir -p .tox
	test -f .tox/dev/bin/activate || virtualenv -p python3.6 .tox/dev
	.tox/dev/bin/pip install -U tox

tox_%: dev_env
	${TOX} -e $*

docs: dev_env
	${TOX} -e docs

pypi: dev_env
	${TOX} -e pypi

clean:
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf .tox .taskproc
	rm -rf dist build
	rm -rf task_processing.egg-info
