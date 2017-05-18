.PHONY: all test dev_env

TOX=".tox/dev/bin/tox"

test: dev_env
	${TOX}

dev_env:
	mkdir -p .tox
	test -f .tox/dev/bin/activate || virtualenv -p python3.6 .tox/dev
	.tox/dev/bin/pip install -U tox

tox_%: dev_env
	${TOX} -e $*

clean:
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf .tox .taskproc
