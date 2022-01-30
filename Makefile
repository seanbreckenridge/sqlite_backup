.DEFAULT_GOAL := install

install:
	python3 -m pip install .

docs: install
	python3 -m pip install pdoc3
	rm -rf ./docs
	pdoc3 -o ./docs sqlite_backup
