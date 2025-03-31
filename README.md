# QuestDB REST API Python Client, CLI and REPL Shell

> QuestDB comes with a very nice web console, but there's no CLI, so I wrote one (can't live without the terminal!).

The REST API is very well defined: https://questdb.com/docs/reference/api/rest/, only 3 documented endpoints. One undocumented endpoints I also implemented are `/chk` to check for if a table exists, I found the route when trying to ingest CSV via the web console.

- [QuestDB REST API Python Client, CLI and REPL Shell](#questdb-rest-api-python-client-cli-and-repl-shell)
  - [Features beyond what the vanilla REST API provides](#features-beyond-what-the-vanilla-rest-api-provides)
    - [Docs, screenshots and video demos](#docs-screenshots-and-video-demos)
    - [`imp` programmatically derives table name from filename when uploading CSVs](#imp-programmatically-derives-table-name-from-filename-when-uploading-csvs)
    - [`exec` supports multiple queries in one go](#exec-supports-multiple-queries-in-one-go)
    - [Query output parsing and formatting](#query-output-parsing-and-formatting)
    - [Global options to fine tune log levels](#global-options-to-fine-tune-log-levels)
    - [`schema`](#schema)
    - [`chk`](#chk)
  - [PyPI packages and installation](#pypi-packages-and-installation)
  - [The Python API](#the-python-api)


## Features beyond what the vanilla REST API provides


### Docs, screenshots and video demos

Originally I just wrote the CLI (`cli.py`), then it becomes really complicated that I had to split the code and put the REST API interfacing part into a module (`__init__.py`).

- https://teddysc.me/blog/questdb-rest
- https://teddysc.me/blog/rlwrap-questdb-shell
- GitHub: https://github.com/tddschn/questdb-rest
- PyPI: https://pypi.org/project/questdb-rest/

### `imp` programmatically derives table name from filename when uploading CSVs

`questdb-cli imp` options that are not part of the REST API spec:
```
  --name-func {stem,add_prefix}
                        Function to generate table name from filename (ignored if --name set). Available: stem, add_prefix (default: None)
  --name-func-prefix NAME_FUNC_PREFIX
                        Prefix string for 'add_prefix' name function. (default: )
  -D, --dash-to-underscore
                        If table name is derived from filename (i.e., --name not set), convert dashes (-) to underscores (_). Compatible with --name-func. (default: False)
```

Global flag `--stop-on-error` controls if it should stop talking to the API on first CSV import error or not.

### `exec` supports multiple queries in one go

The API and web console will only take your last query if you attempt to give it more than 1, while this project uses `sqlparser` to split the queries and send them one by one for you for convenience. Global flag `--stop-on-error` controls if it should stop talking to the API on first error or not. Since the API doesn't always return a status code other than 200 on error, I dived in to the Dev Tools to see what exactly tells me if a request is successful or not.

The queries can be piped in from stdin, or read from a file, or you can supply it from the command line.



### Query output parsing and formatting

The `/exec` endpoints only speaks JSON, this tool gives you options to format the output table to as markdown with `--markdown` or a psql-style ASCII table with `--psql` (default is JSON).

For CSV output, use `questdb-cli exp` instead.

### Global options to fine tune log levels

```
qdb-cli -h

usage: questdb-cli [-h] [-H HOST] [--port PORT] [-u USER] [-p PASSWORD]
                   [--timeout TIMEOUT] [--scheme {http,https}] [-i | -D] [-R]
                   [--config CONFIG] [--stop-on-error | --no-stop-on-error]
                   {imp,exec,exp,chk,schema,gen-config} ...
QuestDB REST API Command Line Interface.
Logs to stderr, outputs data to stdout.
Uses QuestDB REST API via questdb_rest library.
positional arguments:
  {imp,exec,exp,chk,schema,gen-config}
                        Available sub-commands
    imp                 Import data from file(s) using /imp.
    exec                Execute SQL statement(s) using /exec (returns JSON).
                        Reads SQL from --query, --file, --get-query-from-python-module, or stdin.
    exp                 Export data using /exp (returns CSV to stdout or file).
    chk                 Check if a table exists using /chk (returns JSON). Exit code 0 if exists, 3 if not.
    schema              Fetch CREATE TABLE statement(s) for one or more tables.
    gen-config          Generate a default config file at ~/.questdb-rest/config.json
options:
  -h, --help            Show this help message and exit.
  -H HOST, --host HOST  QuestDB server host.
  --port PORT           QuestDB REST API port.
  -u USER, --user USER  Username for basic authentication.
  -p PASSWORD, --password PASSWORD
                        Password for basic authentication. If -u is given but -p is not, will prompt securely unless password is in config.
  --timeout TIMEOUT     Request timeout in seconds.
  --scheme {http,https}
                        Connection scheme (http or https).
  -i, --info            Use info level logging (default is WARNING).
  -D, --debug           Enable debug level logging to stderr.
  -R, --dry-run         Simulate API calls without sending them. Logs intended actions.
  --config CONFIG       Path to a specific config JSON file (overrides default ~/.questdb-rest/config.json).
  --stop-on-error, --no-stop-on-error
                        Stop execution immediately if any item (file/statement/table) fails.
```

### `schema`

Convenience command to fetch schema for 1 or more tables. Hard to do without reading good chunk of the QuestDB doc. The web console supports copying schemas from the tables list.

### `chk`

The `chk` command to talk to `/chk` endpoint, which is used by the web console's CSV upload UI.

## PyPI packages and installation

`questdb-cli`, `questdb-rest` and `questdb-api` are the same package (just aliases), with `questdb-rest` guaranteed to be the most updated.

Installing any of them will give you the `questdb-cli` and `qdb-cli` commands (same thing).

Install (Python >=3.11 required):

```bash
uv tool install questdb-rest
```

```bash
pipx install questdb-rest
```

```bash
# not recommended, but if you really want to:
pip install questdb-rest
```


## The Python API

These classes are provided with extensive methods to interact with the REST API (it's all in `__init__.py`).

```plain
QuestDBError
QuestDBConnectionError
QuestDBAPIError
QuestDBClient
```