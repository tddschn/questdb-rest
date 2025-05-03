GITINGEST_OUTPUT_FILE := qdb-cli.txt
patch-p: patch install publish push
minor-p: minor install publish push
major-p: major install publish push
p: install publish push

install:
	poetry install

publish:
	poetry publish --build

patch:
	bump2version patch

minor:
	bump2version minor

major:
	bump2version major

push:
	git push origin master

yapf:
	poetry run yapf -i -vv **/*.py

gitingest:
	gitingest questdb_rest -o $(GITINGEST_OUTPUT_FILE) && rpp_ $(GITINGEST_OUTPUT_FILE)

ging_:
	gitingest_api_cli.py tddschn/questdb-rest

update-python-functions-from-copied-markdown:
	update-python-functions-from-copied-markdown.sh questdb_rest/cli.py questdb_rest/__init__.py

update-python-functions-from-copied-markdown-include:
	update-python-functions-from-copied-markdown-include-new-defs.sh questdb_rest/cli.py questdb_rest/__init__.py

.PHONE: *
