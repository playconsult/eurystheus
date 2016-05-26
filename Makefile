PROJ=queue_processor
PYTHON=python
GIT=git
TOX=tox
ICONV=iconv
FLAKE8=flake8
FLAKEPLUS=flakeplus
SPHINX_APIDOC=sphinx-apidoc
MAKE=make

SPHINX_DIR=docs/
SPHINX_BUILDDIR="${SPHINX_DIR}/_build"
SPHINX_GENDIR="${SPHINX_DIR}/_gen"
SPHINX_HTMLDIR="${SPHINX_BUILDDIR}/html"
DOCUMENTATION=Documentation
FLAKEPLUSTARGET=3.4

clean: clean-docs clean-pyc clean-build

clean-dist: clean clean-git-force

doc-gen:
	$(SPHINX_APIDOC) -f -o "$(SPHINX_GENDIR)" "$(PROJ)"

Documentation:
	(cd "$(SPHINX_DIR)"; $(MAKE) html)
	mv "$(SPHINX_HTMLDIR)" $(DOCUMENTATION)

docs: clean-docs Documentation

clean-docs:
	-rm -rf "$(SPHINX_BUILDDIR)"
	-rm -rf "$(SPHINX_GENDIR)"
	-rm -rf "$(DOCUMENTATION)"

lint: flakecheck

flakecheck:
	$(FLAKE8) "$(PROJ)"

flakediag:
	-$(MAKE) flakecheck

flakepluscheck:
	$(FLAKEPLUS) --$(FLAKEPLUSTARGET) "$(PROJ)"

flakeplusdiag:
	-$(MAKE) flakepluscheck

flakes: flakediag flakeplusdiag

clean-pyc:
	-find . -type f -a \( -name "*.pyc" -o -name "*$$py.class" \) | xargs rm
	-find . -type d -name "__pycache__" | xargs rm -r

removepyc: clean-pyc

clean-build:
	rm -rf build/ dist/ .eggs/ *.egg-info/ .tox/ .coverage reports/

clean-git:
	$(GIT) clean -xdn -e .env -e .pycharmrc

clean-git-force:
	$(GIT) clean -xdf -e .env -e .pycharmrc

test-all: clean-pyc
	$(TOX)

test:
	mkdir -p reports
	$(PYTHON) setup.py nosetests

build:
	$(PYTHON) setup.py sdist bdist_wheel

distcheck: lint test clean

dist: clean-dist build
