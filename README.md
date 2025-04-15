# QuestDB REST API Python Client, CLI and REPL Shell

> QuestDB comes with a very nice web console, but there's no CLI, so I wrote one (can't live without the terminal!).

The REST API is very well defined: https://questdb.com/docs/reference/api/rest/, only 3 documented endpoints. One undocumented endpoints I also implemented are `/chk` to check for if a table exists, I found the route when trying to ingest CSV via the web console.

```
# << a short tour of questdb-cli >>

# querying the public demo instance, print the data in psql table format
$ qdb-cli --port 443 --host https://demo.questdb.io exec --psql -q 'trades limit 20'

+----------+--------+----------+------------+-----------------------------+
| symbol   | side   |    price |     amount | timestamp                   |
|----------+--------+----------+------------+-----------------------------|
| ETH-USD  | sell   |  2615.54 | 0.00044    | 2022-03-08T18:03:57.609765Z |
| BTC-USD  | sell   | 39270    | 0.001      | 2022-03-08T18:03:57.710419Z |
| ETH-USD  | buy    |  2615.4  | 0.002      | 2022-03-08T18:03:57.764098Z |
| ETH-USD  | buy    |  2615.4  | 0.001      | 2022-03-08T18:03:57.764098Z |
| ETH-USD  | buy    |  2615.4  | 0.00042698 | 2022-03-08T18:03:57.764098Z |
| ETH-USD  | buy    |  2615.36 | 0.025936   | 2022-03-08T18:03:58.194582Z |
| ETH-USD  | buy    |  2615.37 | 0.0350084  | 2022-03-08T18:03:58.194582Z |
| ETH-USD  | buy    |  2615.46 | 0.172602   | 2022-03-08T18:03:58.194582Z |
| ETH-USD  | buy    |  2615.47 | 0.14811    | 2022-03-08T18:03:58.194582Z |
| BTC-USD  | sell   | 39265.3  | 0.000127   | 2022-03-08T18:03:58.357448Z |
| BTC-USD  | sell   | 39265.3  | 0.000245   | 2022-03-08T18:03:58.357448Z |
| BTC-USD  | sell   | 39265.3  | 7.3e-05    | 2022-03-08T18:03:58.357448Z |
| BTC-USD  | sell   | 39263.3  | 0.00392897 | 2022-03-08T18:03:58.357448Z |
| ETH-USD  | buy    |  2615.35 | 0.0224587  | 2022-03-08T18:03:58.612275Z |
| ETH-USD  | buy    |  2615.36 | 0.0324461  | 2022-03-08T18:03:58.612275Z |
| BTC-USD  | sell   | 39265.3  | 6.847e-05  | 2022-03-08T18:03:58.660121Z |
| BTC-USD  | sell   | 39262.4  | 0.00046562 | 2022-03-08T18:03:58.660121Z |
| ETH-USD  | buy    |  2615.62 | 0.00044    | 2022-03-08T18:03:58.682070Z |
| ETH-USD  | buy    |  2615.62 | 0.00044    | 2022-03-08T18:03:58.682070Z |
| ETH-USD  | buy    |  2615.62 | 0.00044    | 2022-03-08T18:03:58.682070Z |
+----------+--------+----------+------------+-----------------------------+


# export the whole table (180 MB, be careful)
$ qdb-cli --port 443 --host https://demo.questdb.io exp 'trips' > trips.csv

# import the copy in your local instance
# let's configure the CLI to use your local instance first
$ qdb-cli gen-config
# edit the config file to set your local instance

# lightning fast import!
$ qdb-cli imp --name trips trips.csv
+-----------------------------------------------------------------------------------------------------------------+
|      Location:  |                                             trips  |        Pattern  | Locale  |      Errors  |
|   Partition by  |                                              NONE  |                 |         |              |
|      Timestamp  |                                              NONE  |                 |         |              |
+-----------------------------------------------------------------------------------------------------------------+
|   Rows handled  |                                           1000000  |                 |         |              |
|  Rows imported  |                                           1000000  |                 |         |              |
+-----------------------------------------------------------------------------------------------------------------+
|              0  |                                          cab_type  |                  VARCHAR  |           0  |
|              1  |                                         vendor_id  |                  VARCHAR  |           0  |
|              2  |                                   pickup_datetime  |                TIMESTAMP  |           0  |
|              3  |                                  dropoff_datetime  |                TIMESTAMP  |           0  |
|              4  |                                      rate_code_id  |                  VARCHAR  |           0  |
|              5  |                                   pickup_latitude  |                   DOUBLE  |           0  |
|              6  |                                  pickup_longitude  |                   DOUBLE  |           0  |
|              7  |                                  dropoff_latitude  |                   DOUBLE  |           0  |
|              8  |                                 dropoff_longitude  |                   DOUBLE  |           0  |
|              9  |                                   passenger_count  |                      INT  |           0  |
|             10  |                                     trip_distance  |                   DOUBLE  |           0  |
|             11  |                                       fare_amount  |                   DOUBLE  |           0  |
|             12  |                                             extra  |                   DOUBLE  |           0  |
|             13  |                                           mta_tax  |                   DOUBLE  |           0  |
|             14  |                                        tip_amount  |                   DOUBLE  |           0  |
|             15  |                                      tolls_amount  |                   DOUBLE  |           0  |
|             16  |                                         ehail_fee  |                   DOUBLE  |           0  |
|             17  |                             improvement_surcharge  |                   DOUBLE  |           0  |
|             18  |                              congestion_surcharge  |                   DOUBLE  |           0  |
|             19  |                                      total_amount  |                   DOUBLE  |           0  |
|             20  |                                      payment_type  |                  VARCHAR  |           0  |
|             21  |                                         trip_type  |                  VARCHAR  |           0  |
|             22  |                                pickup_location_id  |                      INT  |           0  |
|             23  |                               dropoff_location_id  |                      INT  |           0  |
+-----------------------------------------------------------------------------------------------------------------+
```

- [QuestDB REST API Python Client, CLI and REPL Shell](#questdb-rest-api-python-client-cli-and-repl-shell)
  - [How's this different from the official `py-questdb-client` and `py-questdb-query` packages?](#hows-this-different-from-the-official-py-questdb-client-and-py-questdb-query-packages)
  - [Features beyond what the vanilla REST API provides](#features-beyond-what-the-vanilla-rest-api-provides)
    - [Docs, screenshots and video demos](#docs-screenshots-and-video-demos)
    - [`imp` programmatically derives table name from filename when uploading CSVs](#imp-programmatically-derives-table-name-from-filename-when-uploading-csvs)
    - [`exec` supports multiple queries in one go](#exec-supports-multiple-queries-in-one-go)
    - [Query output parsing and formatting](#query-output-parsing-and-formatting)
    - [`schema`](#schema)
    - [`chk`](#chk)
  - [Usage](#usage)
    - [Global options to fine tune log levels](#global-options-to-fine-tune-log-levels)
    - [Configuring CLI - DB connection options](#configuring-cli---db-connection-options)
  - [PyPI packages and installation](#pypi-packages-and-installation)
  - [The Python API](#the-python-api)
  - [Screenshots](#screenshots)

## How's this different from the official `py-questdb-client` and `py-questdb-query` packages?

- `py-questdb-client`: Focuses on ingestion from Python data structures and / or DataFrames, I don't think it does anything else
- `py-questdb-query`: Cython based library to get numpy arrays or dataframes from the REST API
- This python client: Gets raw JSON from REST API, doesn't depend on numpy or pandas, making the CLI lightweight and fast to start

## Features beyond what the vanilla REST API provides


### Docs, screenshots and video demos

Originally I just wrote the CLI (`cli.py`), then it becomes really complicated that I had to split the code and put the REST API interfacing part into a module (`__init__.py`).

- Write up and demo: https://teddysc.me/blog/questdb-rest
- 6 min demo: https://www.youtube.com/watch?v=l_1HBbAHeBM
- https://teddysc.me/blog/rlwrap-questdb-shell
- GitHub: https://github.com/tddschn/questdb-rest
- PyPI: https://pypi.org/project/questdb-rest/
- QuestDB-Shell: https://github.com/tddschn/questdb-shell

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

### `schema`

Convenience command to fetch schema for 1 or more tables. Hard to do without reading good chunk of the QuestDB doc. The web console supports copying schemas from the tables list.

```
qdb-cli schema equities_1d

CREATE TABLE 'equities_1d' ( 
	timestamp TIMESTAMP,
	open DOUBLE,
	high DOUBLE,
	low DOUBLE,
	close DOUBLE,
	volume LONG,
	ticker SYMBOL CAPACITY 1024 CACHE
) timestamp(timestamp) PARTITION BY YEAR WAL
WITH maxUncommittedRows=500000, o3MaxLag=600000000us
DEDUP UPSERT KEYS(timestamp,ticker);
```
### `chk`

The `chk` command to talk to `/chk` endpoint, which is used by the web console's CSV upload UI.

## Usage

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

### Configuring CLI - DB connection options

Run `qdb-cli gen-config` and edit the generated config file to specify your DB's port, host, and auth info.

All options are optional and will use the default `localhost:9000` if not specified.

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

## Screenshots

![CleanShot-2025-03-30-16.25.07](https://g.teddysc.me/tddschn/16651cccc351b1d2742a4bddaee1c62d/CleanShot-2025-03-30-16.25.07@2x_base64.txt?b)
![CleanShot-2025-03-30-16.32.44](https://g.teddysc.me/tddschn/16651cccc351b1d2742a4bddaee1c62d/CleanShot-2025-03-30-16.32.44_base64.txt?b)
![CleanShot-2025-03-30-16.33.18](https://g.teddysc.me/tddschn/16651cccc351b1d2742a4bddaee1c62d/CleanShot-2025-03-30-16.33.18_base64.txt?b)
![CleanShot-2025-03-30-16.33.36](https://g.teddysc.me/tddschn/16651cccc351b1d2742a4bddaee1c62d/CleanShot-2025-03-30-16.33.36_base64.txt?b)