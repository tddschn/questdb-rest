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

.PHONE: *
