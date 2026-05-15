SHELL := /bin/bash

# 1. Bump VERSION below
# 2. `make release`
VERSION = 1.4.0

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

# To cut a release:
#   1. Merge all changes to master
#   2. Bump VERSION at the top of this file
#   3. Run `make release`
release:
	@if [[ "$$(git status --porcelain --untracked-files=no :^./Makefile)" != '' ]]; then echo "Error: working directory is not clean; only changes to Makefile are allowed when cutting a release."; exit 1; fi
	@if [[ "$$(git rev-parse --abbrev-ref HEAD)" != "master" ]]; then echo "Error: must be on master branch."; exit 1; fi
	@read upstream_master junk <<<"$$(git ls-remote -h origin master)" && if ! git merge-base --is-ancestor $$upstream_master HEAD; then echo "Error: HEAD is missing commits from origin/master ($$upstream_master)."; exit 1; fi
	@if existing_sha=$$(git rev-parse --verify --quiet v$(VERSION)); then echo "Error: tag v$(VERSION) already exists and points at $$existing_sha"; exit 1; fi
	@if curl -sf "https://pypi.yelpcorp.com/simple/task-processing/index.html" | grep -q "task.processing-$(VERSION)"; then echo "Error: $(VERSION) already exists on PyPI"; exit 1; fi
	sed -i -e 's/__version__ = .*/__version__ = "$(VERSION)"/' ./task_processing/__init__.py
	git add ./Makefile ./task_processing/__init__.py
	git commit -m "Release v$(VERSION)"
	git tag v$(VERSION)
	git push --atomic origin master v$(VERSION)
	@echo "Tagged and pushed v$(VERSION) — watch Jenkins at y/jenkins for task-processing build"

clean:
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf .tox .taskproc
	rm -rf dist build
	rm -rf task_processing.egg-info
	rm -rf venv
