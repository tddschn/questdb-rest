GITINGEST_OUTPUT_FILE := qdb-cli.txt
PYPI_PACKAGE_NAME := questdb-rest[mcp]
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

gitingest-python-module-only:
# 	gitingest questdb_rest -o $(GITINGEST_OUTPUT_FILE) && rpp_ $(GITINGEST_OUTPUT_FILE)
	gitingest questdb_rest -o $(GITINGEST_OUTPUT_FILE) && copy_paths_as_files_objc.py $(GITINGEST_OUTPUT_FILE)

ging-full-repo:
	gitingest_api_cli.py tddschn/questdb-rest && copy_paths_as_files_objc.py tddschn-questdb-rest.txt
	rg __version__ tddschn-questdb-rest.txt

# update-python-functions-from-copied-markdown:
# 	update-python-functions-from-copied-markdown.sh questdb_rest/cli.py questdb_rest/__init__.py

# update-python-functions-from-copied-markdown-include:
# 	update-python-functions-from-copied-markdown-include-new-defs.sh questdb_rest/cli.py questdb_rest/__init__.py

# update-python-functions-from-copied-markdown-include-table-names:
# 	update-python-functions-from-copied-markdown-include-new-defs.sh questdb_rest/questdb_table_names_pypika.py

# update-python-functions-from-copied-markdown-include-rnd:
# 	update-python-functions-from-copied-markdown-include-new-defs.sh questdb_rest/qdb_gen_random_data.py

# --------------------
# uv
# --------------------
uv-tool-install:
	uv tool install $(PYPI_PACKAGE_NAME)

uv-tool-upgrade:
	uv tool upgrade $(PYPI_PACKAGE_NAME)


.PHONE: *
