# QuestDB REST API Python Client, CLI and REPL Shell

> QuestDB comes with a very nice web console, but there's no CLI, so I wrote one (can't live without the terminal!).

The REST API is very well defined: https://questdb.com/docs/reference/api/rest/, only 3 documented endpoints. One undocumented endpoints I also implemented are `/chk` to check for if a table exists, I found the route when trying to ingest CSV via the web console.

## A short tour

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

# lightning fast local import!
# the imp command can infer table name using different rules, install it and run --help to see
$ qdb-cli imp --name trips trips.csv --partitionBy WEEK --timestamp pickup_datetime

# you can also pipe data directly from stdin using the qdb-imp-from-stdin helper script
$ cat trips.csv | qdb-imp-from-stdin --name trips --partitionBy WEEK --timestamp pickup_datetime
+-----------------------------------------------------------------------------------------------------------------+
|      Location:  |                                             trips  |        Pattern  | Locale  |      Errors  |
|   Partition by  |                                              WEEK  |                 |         |              |
|      Timestamp  |                                   pickup_datetime  |                 |         |              |
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

# check schema to confirm the import
$ qdb-cli schema trips
CREATE TABLE 'trips' ( 
	cab_type VARCHAR,
	vendor_id VARCHAR,
	pickup_datetime TIMESTAMP,
	dropoff_datetime TIMESTAMP,
	rate_code_id VARCHAR,
	pickup_latitude DOUBLE,
	pickup_longitude DOUBLE,
	dropoff_latitude DOUBLE,
	dropoff_longitude DOUBLE,
	passenger_count INT,
	trip_distance DOUBLE,
	fare_amount DOUBLE,
	extra DOUBLE,
	mta_tax DOUBLE,
	tip_amount DOUBLE,
	tolls_amount DOUBLE,
	ehail_fee DOUBLE,
	improvement_surcharge DOUBLE,
	congestion_surcharge DOUBLE,
	total_amount DOUBLE,
	payment_type VARCHAR,
	trip_type VARCHAR,
	pickup_location_id INT,
	dropoff_location_id INT
) timestamp(pickup_datetime) PARTITION BY WEEK WAL
WITH maxUncommittedRows=500000, o3MaxLag=600000000us;

# rename commands for your convenience (run something like `RENAME TABLE 'test.csv' TO 'myTable'`; under the hood)
$ qdb-cli rename trips taxi_trips_feb_2018
{
  "status": "OK",
  "message": "Table 'trips' renamed to 'taxi_trips_feb_2018'"
}
```

## Table of Contents

- [QuestDB REST API Python Client, CLI and REPL Shell](#questdb-rest-api-python-client-cli-and-repl-shell)
  - [A short tour](#a-short-tour)
  - [Table of Contents](#table-of-contents)
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
    - [Accompanying Bash Scripts](#accompanying-bash-scripts)
  - [Subcommands that run complex workflows](#subcommands-that-run-complex-workflows)
    - [`create-or-replace-table-from-query` or `cor`](#create-or-replace-table-from-query-or-cor)
    - [`rename` with table exists checks](#rename-with-table-exists-checks)
    - [`dedupe` check, enable, disable](#dedupe-check-enable-disable)
  - [Examples](#examples)
    - [Advanced Scripting](#advanced-scripting)
    - [Drop all backup tables with UUID4 in the name](#drop-all-backup-tables-with-uuid4-in-the-name)
    - [Piping query or table names from stdin](#piping-query-or-table-names-from-stdin)
    - [Change partitioning strategy to YEAR for existing table](#change-partitioning-strategy-to-year-for-existing-table)
    - [Batch change partitioning strategy and enable deduplication with `xargs`](#batch-change-partitioning-strategy-and-enable-deduplication-with-xargs)
  - [PyPI packages and installation](#pypi-packages-and-installation)
  - [The Python API](#the-python-api)
  - [Screenshots](#screenshots)
  - [Code Stats](#code-stats)
    - [LOC by file](#loc-by-file)
    - [Token count by function](#token-count-by-function)
    - [Function LOC Sunburst Chart](#function-loc-sunburst-chart)

## How's this different from the official `py-questdb-client` and `py-questdb-query` packages?

- `py-questdb-client`: Focuses on ingestion from Python data structures and / or DataFrames, I don't think it does anything else
- `py-questdb-query`: Cython based library to get numpy arrays or dataframes from the REST API
- This python client: Gets raw JSON from REST API, doesn't depend on numpy or pandas, making the CLI lightweight and fast to start

## Features beyond what the vanilla REST API provides


### Docs, screenshots and video demos

Originally I just wrote the CLI (`cli.py`), then it becomes really complicated that I had to split the code and put the REST API interfacing part into a module (`__init__.py`).

- Write-up and demo: https://teddysc.me/blog/questdb-rest
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
                   {imp,exec,exp,chk,schema,rename,create-or-replace-table-from-query,cor,drop,drop-table,dedupe,gen-config}
                   ...

QuestDB REST API Command Line Interface.
Logs to stderr, outputs data to stdout.

Uses QuestDB REST API via questdb_rest library.

positional arguments:
  {imp,exec,exp,chk,schema,rename,create-or-replace-table-from-query,cor,drop,drop-table,dedupe,gen-config}
                        Available sub-commands
    imp                 Import data from file(s) using /imp.
    exec                Execute SQL statement(s) using /exec (returns JSON).
                        Reads SQL from --query, --file, --get-query-from-python-module, or stdin.
    exp                 Export data using /exp (returns CSV to stdout or file).
    chk                 Check if a table exists using /chk (returns JSON). Exit code 0 if exists, 3 if not.
    schema              Fetch CREATE TABLE statement(s) for one or more tables.
    rename              Rename a table using RENAME TABLE. Backs up target name by default if it exists.
    create-or-replace-table-from-query (cor)
                        Atomically replace a table with the result of a query, with optional backup.
    drop (drop-table)   Drop one or more tables using DROP TABLE.
    dedupe              Enable, disable, or check data deduplication settings for a WAL table.
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
                        Stop execution immediately if any item (file/statement/table) fails (where applicable).

This CLI can also be used as a Python library.
```

### Configuring CLI - DB connection options

Run `qdb-cli gen-config` and edit the generated config file to specify your DB's port, host, and auth info.

All options are optional and will use the default `localhost:9000` if not specified.

### Accompanying Bash Scripts

```plain
# check next section too
$ qdb-drop-tables-by-regex 

Usage: ~/.local/bin/qdb-drop-tables-by-regex [-n] [-c] -p PATTERN

Options:
  -p PATTERN   Regex pattern to match table names (required)
  -n           Dry run; show what would be dropped but do not execute
  -c           Confirm each drop interactively
  -h           Show this help message
```

## Subcommands that run complex workflows

### `create-or-replace-table-from-query` or `cor`

https://stackoverflow.com/a/79601299/11133602

QuestDB doesn't have `DELETE FROM` to delete rows, you can only create a new table and drop the old one. This command does that for you, and optionally backs up the old table.

It does complex checks to ensure the queries are correctly constructed and are run in the correct order.

One of the query that will be executed is `CREATE TABLE IF NOT EXISTS <table> AS <query>`.


```plain
qdb-cli cor --help

usage: questdb-cli create-or-replace-table-from-query [-h] [-q QUERY | -f FILE | -G GET_QUERY_FROM_PYTHON_MODULE] [-B BACKUP_TABLE_NAME | --no-backup-original-table] [-P {NONE,YEAR,MONTH,DAY,HOUR,WEEK}] [-t TIMESTAMP]
                                                      [--statement-timeout STATEMENT_TIMEOUT]
                                                      table

positional arguments:
  table                 Name of the target table to create or replace.

options:
  -h, --help            Show this help message and exit.
  -q QUERY, --query QUERY
                        SQL query string defining the new table content.
  -f FILE, --file FILE  Path to file containing the SQL query.
  -G GET_QUERY_FROM_PYTHON_MODULE, --get-query-from-python-module GET_QUERY_FROM_PYTHON_MODULE
                        Get query from a Python module (format 'module_path:variable_name').
  --statement-timeout STATEMENT_TIMEOUT
                        Query timeout in milliseconds for underlying operations.

Backup Options (if target table exists):
  -B BACKUP_TABLE_NAME, --backup-table-name BACKUP_TABLE_NAME, --rename-original-table-to BACKUP_TABLE_NAME
                        Specify a name for the backup table (if target exists). Default: generated name.
  --no-backup-original-table
                        DROP the original table directly instead of renaming it to a backup.

New Table Creation Options:
  -P {NONE,YEAR,MONTH,DAY,HOUR,WEEK}, --partitionBy {NONE,YEAR,MONTH,DAY,HOUR,WEEK}
                        Partitioning strategy for the new table.
  -t TIMESTAMP, --timestamp TIMESTAMP
                        Designated timestamp column name for the new table.
  -k COLUMN [COLUMN ...], --upsert-keys COLUMN [COLUMN ...]
                        List of column names to use as UPSERT KEYS when creating the new table. Must include the designated timestamp (if specified via -t). Requires WAL.
```

```plain
# oh snap! I inserted wrong PLTR data to the equities_1 table, the timestamp col is messed up
# let's fix it by creating a new table with the correct data

qdb-cli --info cor equities_1 -q "equities_1 where ticker != 'PLTR'" -t timestamp -P WEEK
INFO: Log level set to INFO
INFO: Connecting to http://localhost:9000
INFO: Starting create-or-replace operation for table 'equities_1' using temp table '__qdb_cli_temp_equities_1_26b1ac1a_5853_4215_b9b0_aa9b872c1f7b'...
WARNING: Input query from query string does not start with SELECT. Assuming it's valid QuestDB shorthand.
WARNING: Query: equities_1 where ticker != 'PLTR'
INFO: Using query from query string for table creation.
INFO: Creating temporary table '__qdb_cli_temp_equities_1_26b1ac1a_5853_4215_b9b0_aa9b872c1f7b' from query...
INFO: Successfully created temporary table '__qdb_cli_temp_equities_1_26b1ac1a_5853_4215_b9b0_aa9b872c1f7b'.
INFO: Checking if target table 'equities_1' exists...
INFO: Generated backup name: 'qdb_cli_backup_equities_1_bc345051_9157_4e3c_83ec_70e8430a3f64'
INFO: Checking if backup table 'qdb_cli_backup_equities_1_bc345051_9157_4e3c_83ec_70e8430a3f64' exists...
INFO: Backup table 'qdb_cli_backup_equities_1_bc345051_9157_4e3c_83ec_70e8430a3f64' does not exist. Proceeding with rename.
INFO: Renaming original table 'equities_1' to backup table 'qdb_cli_backup_equities_1_bc345051_9157_4e3c_83ec_70e8430a3f64'...
INFO: Successfully renamed 'equities_1' to 'qdb_cli_backup_equities_1_bc345051_9157_4e3c_83ec_70e8430a3f64'.
INFO: Renaming temporary table '__qdb_cli_temp_equities_1_26b1ac1a_5853_4215_b9b0_aa9b872c1f7b' to target table 'equities_1'...
INFO: Successfully renamed temporary table '__qdb_cli_temp_equities_1_26b1ac1a_5853_4215_b9b0_aa9b872c1f7b' to 'equities_1'.
{
  "status": "OK",
  "message": "Successfully created/replaced table 'equities_1'. Original table backed up as 'qdb_cli_backup_equities_1_bc345051_9157_4e3c_83ec_70e8430a3f64'.",
  "target_table": "equities_1",
  "backup_table": "qdb_cli_backup_equities_1_bc345051_9157_4e3c_83ec_70e8430a3f64",
  "original_dropped_no_backup": false
}
```

### `rename` with table exists checks

```plain
qdb-cli rename --help

usage: questdb-cli rename [-h] [--no-backup-if-new-table-exists] [--statement-timeout STATEMENT_TIMEOUT] old_table_name new_table_name

positional arguments:
  old_table_name        Current name of the table.
  new_table_name        New name for the table.

options:
  -h, --help            Show this help message and exit.
  --no-backup-if-new-table-exists
                        If the new table name already exists, do not back it up first. Rename might fail. (default: False)
  --statement-timeout STATEMENT_TIMEOUT
                        Query timeout in milliseconds (per RENAME statement). (default: None)
```

Example:

```plain
qdb chk trades2
{
  "tableName": "trades2",
  "status": "Exists"
}
❯ qdb chk trades3
{
  "tableName": "trades3",
  "status": "Exists"
}
❯ qdb rename trades2 trades3
WARNING: Target table name 'trades3' already exists.
{
  "status": "OK",
  "message": "Table 'trades2' successfully renamed to 'trades3'. Existing table at 'trades3' was backed up as 'qdb_cli_backup_trades3_f652d5ac_b9dd_4561_a835_eae947866e4f'.",
  "old_name": "trades2",
  "new_name": "trades3",
  "backup_of_new_name": "qdb_cli_backup_trades3_f652d5ac_b9dd_4561_a835_eae947866e4f"
}

# ok let's drop it now
qdb drop qdb_cli_backup_trades3_f652d5ac_b9dd_4561_a835_eae947866e4f
{
  "status": "OK",
  "table_dropped": "qdb_cli_backup_trades3_f652d5ac_b9dd_4561_a835_eae947866e4f",
  "message": "Table 'qdb_cli_backup_trades3_f652d5ac_b9dd_4561_a835_eae947866e4f' dropped successfully.",
  "ddl_response": "OK"
}

qdb chk qdb_cli_backup_trades3_f652d5ac_b9dd_4561_a835_eae947866e4f
{
  "tableName": "qdb_cli_backup_trades3_f652d5ac_b9dd_4561_a835_eae947866e4f",
  "status": "Does not exist"
}
```

### `dedupe` check, enable, disable


Usage:

Default is `--check`.

This command parses the `CREATE TABLE` statement to get the `UPSERT KEYS` and `DESIGNATED TIMESTAMP` columns for you.

```plain
❯ qdb-cli dedupe trades --help
usage: questdb-cli dedupe [-h] [--enable | --disable | --check] [-k COLUMN [COLUMN ...]] [--statement-timeout STATEMENT_TIMEOUT] table_name

positional arguments:
  table_name            Name of the target WAL table.

options:
  -h, --help            Show this help message and exit.
  --enable              Enable deduplication. Requires --upsert-keys. (default: False)
  --disable             Disable deduplication. (default: False)
  --check               Check current deduplication status and keys (default action). (default: False)
  -k COLUMN [COLUMN ...], --upsert-keys COLUMN [COLUMN ...]
                        List of column names to use as UPSERT KEYS when enabling. Must include the designated timestamp. (default: None)
  --statement-timeout STATEMENT_TIMEOUT
                        Query timeout in milliseconds for the ALTER TABLE statement. (default: None)
```

Example:

```plain
# trades table is the same as the one in the demo instance
qdb-cli dedupe trades
{
  "status": "OK",
  "table_name": "trades",
  "action": "check",
  "deduplication_enabled": true,
  "designated_timestamp": "timestamp",
  "upsert_keys": [
    "timestamp",
    "symbol"
  ]
}
❯ qdb-cli dedupe trades --disable
{
  "status": "OK",
  "table_name": "trades",
  "action": "disable",
  "deduplication_enabled": false,
  "ddl": "OK"
}
❯ qdb-cli dedupe trades --enable -k timestamp,symbol
ERROR: Error: Designated timestamp column 'timestamp' must be included in --upsert-keys.
{
  "status": "Error",
  "table_name": "trades",
  "action": "enable",
  "message": "Designated timestamp column 'timestamp' must be included in upsert keys.",
  "provided_keys": [
    "timestamp,symbol"
  ]
}
[1]    47734 exit 1     questdb-cli dedupe trades --enable -k timestamp,symbol
❯ qdb-cli dedupe trades --enable -k timestamp symbol
{
  "status": "OK",
  "table_name": "trades",
  "action": "enable",
  "deduplication_enabled": true,
  "upsert_keys": [
    "timestamp",
    "symbol"
  ],
  "ddl": "OK"
}
```

## Examples

Check the `Short Tour` section above for a quick overview of the CLI.

### Advanced Scripting

```bash
# drop all tables with name regex matching 'test_table_'
# exp exports as CSV, so we use tail to skip the header
qdb-cli exp "select table_name from tables where table_name ~ 'test_table_'" | tail -n +2 | xargs -I{} bash -c 'echo Dropping table {}; qdb-cli exec -q "drop table {}"'
```

For convenience, I included a bash script `qdb-drop-tables-by-regex` and `qdb-imp-from-stdin` (see below) that does exactly this - it will be installed if you install the `questdb-rest` PyPI package.

```bash
curl 'https://raw.githubusercontent.com/your/test.csv' | qdb-imp-from-stdin -n your_table_name
```

Or use the more general purpose version:

```bash
qdb-table-names test_table_ | qdb-cli drop
```

### Drop all backup tables with UUID4 in the name


```plain
# dry run first:
qdb-table-names backup --uuid | qdb-cli --dry-run drop

{
  "dry_run": true,
  "table_dropped": "qdb_cli_backup_cme_liq_ba_LE_0ae696bb_076e_4c0e_b7ba_3999e8939c89",
  "ddl": "OK (Simulated)"
}
{
  "dry_run": true,
  "table_dropped": "qdb_cli_backup_cme_liq_ba_LE_96042ea7_d2eb_4455_a8d3_250ab75f347a",
  "ddl": "OK (Simulated)"
}

# destructive command, be careful!
qdb-table-names backup --uuid | qdb-cli drop

{
  "status": "OK",
  "table_dropped": "qdb_cli_backup_cme_liq_ba_LE_0ae696bb_076e_4c0e_b7ba_3999e8939c89",
  "message": "Table 'qdb_cli_backup_cme_liq_ba_LE_0ae696bb_076e_4c0e_b7ba_3999e8939c89' dropped successfully.",
  "ddl_response": "OK"
}

{
  "status": "OK",
  "table_dropped": "qdb_cli_backup_cme_liq_ba_LE_96042ea7_d2eb_4455_a8d3_250ab75f347a",
  "message": "Table 'qdb_cli_backup_cme_liq_ba_LE_96042ea7_d2eb_4455_a8d3_250ab75f347a' dropped successfully.",
  "ddl_response": "OK"
}

```

```plain
# yes, this command is installed if you install the Python package
$ qdb-table-names --help

Usage: qdb-table-names [-u|--uuid] [-U|--no-uuid] [regex]

Get a list of table names from QuestDB.
If you provide a regex, only tables whose name matches will be returned.
-u, --uuid      Only tables containing a UUID-4 in their name
-U, --no-uuid   Only tables NOT containing a UUID-4 in their name
You may combine either UUID flag with an additional regex, but -u and -U are mutually exclusive.

OPTIONS:
  -h, --help     Show this help message and exit
  -u, --uuid     Match only tables containing a UUID-4 in their name
  -U, --no-uuid  Match only tables NOT containing a UUID-4 in their name

EXAMPLES:
  # list all table names
  qdb-table-names

  # list only tables containing a UUID-4
  qdb-table-names -u

  # list only tables NOT containing a UUID-4
  qdb-table-names -U

  # list only tables starting with "equities_"
  qdb-table-names equities_

  # combine regex and UUID-flag
  qdb-table-names -u equities_
  qdb-table-names -U equities_
```

### Piping query or table names from stdin

`qdb-cli exec` supports reading multiple queries (delimited by `;`) from stdin, or from a file.

Besides `qdb-cli drop` (see example right above), these subcommands also support reading table names (1 per line) from stdin: `chk`, `dedupe`, `schema`.

Examples:

```plain
qdb-table-names cme_liq | qdb-cli chk 
{
  "tableName": "cme_liq_ba_LE",
  "status": "Exists"
}
{
  "tableName": "cme_liq_ba_HG",
  "status": "Exists"
}
{
  "tableName": "cme_liq_ba_SI",
  "status": "Exists"
}
{
  "tableName": "cme_liq_ba_GC",
  "status": "Exists"
}
```

```sql
-- run this:
-- qdb-table-names cme_liq | qdb-cli schema
CREATE TABLE 'cme_liq_ba_LE' ( 
        CT VARCHAR,
        MP DOUBLE,
        LVL1A DOUBLE,
        LVL2A DOUBLE,
        LVL3A DOUBLE,
        LVL4A DOUBLE,
        LVL5A DOUBLE,
        WT LONG,
        timestamp TIMESTAMP
) timestamp(timestamp) PARTITION BY YEAR WAL
WITH maxUncommittedRows=500000, o3MaxLag=600000000us
DEDUP UPSERT KEYS(timestamp);


CREATE TABLE 'cme_liq_ba_HG' ( 
        MP DOUBLE,
        LVL1B DOUBLE,
        LVL1A DOUBLE,
        LVL2B DOUBLE,
        LVL2A DOUBLE,
        LVL3B DOUBLE,
        LVL3A DOUBLE,
        LVL4B DOUBLE,
        LVL10B DOUBLE,
        LVL10A DOUBLE,
        CT VARCHAR,
        LVL4A DOUBLE,
        LVL5B DOUBLE,
        LVL5A DOUBLE,
        LVL6B DOUBLE,
        LVL6A DOUBLE,
        LVL7B DOUBLE,
        LVL7A DOUBLE,
        LVL8B DOUBLE,
        LVL8A DOUBLE,
        LVL9B DOUBLE,
        WT LONG,
        LVL9A DOUBLE,
        timestamp TIMESTAMP
) timestamp(timestamp) PARTITION BY DAY WAL
WITH maxUncommittedRows=500000, o3MaxLag=600000000us
DEDUP UPSERT KEYS(timestamp);

-- ...
```


### Change partitioning strategy to YEAR for existing table

```plain
# let check original schema before we make big changes

qdb-cli schema cme_liq_ba_6S
CREATE TABLE 'cme_liq_ba_6S' ( 
	MP DOUBLE,
	LVL1B DOUBLE,
	LVL1A DOUBLE,
	LVL2B DOUBLE,
	LVL2A DOUBLE,
	LVL3B DOUBLE,
	LVL3A DOUBLE,
	LVL4B DOUBLE,
	LVL10B DOUBLE,
	LVL10A DOUBLE,
	CT VARCHAR,
	LVL4A DOUBLE,
	LVL5B DOUBLE,
	LVL5A DOUBLE,
	LVL6B DOUBLE,
	LVL6A DOUBLE,
	LVL7B DOUBLE,
	LVL7A DOUBLE,
	LVL8B DOUBLE,
	LVL8A DOUBLE,
	LVL9B DOUBLE,
	WT LONG,
	LVL9A DOUBLE,
	timestamp TIMESTAMP
) timestamp(timestamp) PARTITION BY DAY WAL
WITH maxUncommittedRows=500000, o3MaxLag=600000000us
DEDUP UPSERT KEYS(timestamp);

# forgot to specify the designated timestamp column

❯ qdb-cli cor cme_liq_ba_6S -q cme_liq_ba_6S -k timestamp -P YEAR
WARNING: Input query from query string does not start with SELECT. Assuming it's valid QuestDB shorthand.
WARNING: Query: cme_liq_ba_6S
WARNING: QuestDB API Error: HTTP 400: partitioning is possible only on tables with designated timestamps
WARNING: Response Body: {"query": "CREATE TABLE __qdb_cli_temp_cme_liq_ba_6S_683ce6ae_9c45_4bd1_836a_b1184075dea2 AS (cme_liq_ba_6S) PARTITION BY YEAR DEDUP UPSERT KEYS(timestamp);", "error": "partitioning is possible only on tables with designated timestamps", "position": 111}
ERROR: Error creating temporary table '__qdb_cli_temp_cme_liq_ba_6S_683ce6ae_9c45_4bd1_836a_b1184075dea2': HTTP 400: HTTP 400: partitioning is possible only on tables with designated timestamps
[1]    64741 exit 1     questdb-cli cor cme_liq_ba_6S -q cme_liq_ba_6S -k timestamp -P YEAR

❯ qdb-cli cor cme_liq_ba_6S -q cme_liq_ba_6S -k timestamp -P YEAR -t timestamp
WARNING: Input query from query string does not start with SELECT. Assuming it's valid QuestDB shorthand.
WARNING: Query: cme_liq_ba_6S
{
  "status": "OK",
  "message": "Successfully created/replaced table 'cme_liq_ba_6S'. DEDUP enabled with keys: ['timestamp']. Original table backed up as 'qdb_cli_backup_cme_liq_ba_6S_dd70f217_4931_428f_8d84_3fa6003fbe4c'.",
  "target_table": "cme_liq_ba_6S",
  "upsert_keys_set": [
    "timestamp"
  ],
  "backup_table": "qdb_cli_backup_cme_liq_ba_6S_dd70f217_4931_428f_8d84_3fa6003fbe4c",
  "original_dropped_no_backup": false
}

# check the schema again
❯ qdb-cli schema cme_liq_ba_6S
CREATE TABLE 'cme_liq_ba_6S' ( 
	MP DOUBLE,
	LVL1B DOUBLE,
	LVL1A DOUBLE,
	LVL2B DOUBLE,
	LVL2A DOUBLE,
	LVL3B DOUBLE,
	LVL3A DOUBLE,
	LVL4B DOUBLE,
	LVL10B DOUBLE,
	LVL10A DOUBLE,
	CT VARCHAR,
	LVL4A DOUBLE,
	LVL5B DOUBLE,
	LVL5A DOUBLE,
	LVL6B DOUBLE,
	LVL6A DOUBLE,
	LVL7B DOUBLE,
	LVL7A DOUBLE,
	LVL8B DOUBLE,
	LVL8A DOUBLE,
	LVL9B DOUBLE,
	WT LONG,
	LVL9A DOUBLE,
	timestamp TIMESTAMP
) timestamp(timestamp) PARTITION BY YEAR WAL
WITH maxUncommittedRows=500000, o3MaxLag=600000000us
DEDUP UPSERT KEYS(timestamp);

# original table is backed up
❯ qdb-table-names --uuid
qdb_cli_backup_cme_liq_ba_6S_dd70f217_4931_428f_8d84_3fa6003fbe4c
```


### Batch change partitioning strategy and enable deduplication with `xargs`

Change partition to `BY YEAR`:

```plain
$ qdb-table-names cme_liq | xargs -I{} qdb-cli --info cor -q {} {} -t timestamp -P YEAR --no-backup-original-table

INFO: Log level set to INFO
INFO: Connecting to http://localhost:9000
INFO: Starting create-or-replace operation for table 'cme_liq_ba_ZF' using temp table '__qdb_cli_temp_cme_liq_ba_ZF_b802072f_3d4b_40bb_9661_beae1838e3f5'...
WARNING: Input query from query string does not start with SELECT. Assuming it's valid QuestDB shorthand.
WARNING: Query: cme_liq_ba_ZF
INFO: Using query from query string for table creation.
INFO: Creating temporary table '__qdb_cli_temp_cme_liq_ba_ZF_b802072f_3d4b_40bb_9661_beae1838e3f5' from query...
INFO: Successfully created temporary table '__qdb_cli_temp_cme_liq_ba_ZF_b802072f_3d4b_40bb_9661_beae1838e3f5'.
INFO: Checking if target table 'cme_liq_ba_ZF' exists...
INFO: --no-backup-original-table specified. Dropping original table 'cme_liq_ba_ZF'...
INFO: Successfully dropped original table 'cme_liq_ba_ZF'.
INFO: Renaming temporary table '__qdb_cli_temp_cme_liq_ba_ZF_b802072f_3d4b_40bb_9661_beae1838e3f5' to target table 'cme_liq_ba_ZF'...
INFO: Successfully renamed temporary table '__qdb_cli_temp_cme_liq_ba_ZF_b802072f_3d4b_40bb_9661_beae1838e3f5' to 'cme_liq_ba_ZF'.
{
  "status": "OK",
  "message": "Successfully created/replaced table 'cme_liq_ba_ZF'. Original table was dropped (no backup).",
  "target_table": "cme_liq_ba_ZF",
  "upsert_keys_set": null,
  "backup_table": null,
  "original_dropped_no_backup": true
}
INFO: Log level set to INFO
INFO: Connecting to http://localhost:9000
INFO: Starting create-or-replace operation for table 'cme_liq_ba_ZT' using temp table '__qdb_cli_temp_cme_liq_ba_ZT_e1827495_381a_4029_a744_aa3982a85fe6'...
WARNING: Input query from query string does not start with SELECT. Assuming it's valid QuestDB shorthand.
WARNING: Query: cme_liq_ba_ZT
INFO: Using query from query string for table creation.
INFO: Creating temporary table '__qdb_cli_temp_cme_liq_ba_ZT_e1827495_381a_4029_a744_aa3982a85fe6' from query...
INFO: Successfully created temporary table '__qdb_cli_temp_cme_liq_ba_ZT_e1827495_381a_4029_a744_aa3982a85fe6'.
INFO: Checking if target table 'cme_liq_ba_ZT' exists...
INFO: --no-backup-original-table specified. Dropping original table 'cme_liq_ba_ZT'...
INFO: Successfully dropped original table 'cme_liq_ba_ZT'.
INFO: Renaming temporary table '__qdb_cli_temp_cme_liq_ba_ZT_e1827495_381a_4029_a744_aa3982a85fe6' to target table 'cme_liq_ba_ZT'...
INFO: Successfully renamed temporary table '__qdb_cli_temp_cme_liq_ba_ZT_e1827495_381a_4029_a744_aa3982a85fe6' to 'cme_liq_ba_ZT'.
{
  "status": "OK",
  "message": "Successfully created/replaced table 'cme_liq_ba_ZT'. Original table was dropped (no backup).",
  "target_table": "cme_liq_ba_ZT",
  "upsert_keys_set": null,
  "backup_table": null,
  "original_dropped_no_backup": true
}
```




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

## Code Stats

Below are updated for version 3.0.3.

See also https://teddysc.me/blog/code-stats-visualization

### LOC by file

![](https://g.teddysc.me/d149da246628052d4550f3f0baa41dd5?b)

Interactive: https://g.teddysc.me/511ceb311b59770268a709a589ce4fef

### Token count by function

![](https://g.teddysc.me/3b8c417c0df3ce78b43c76915e5133fb?b)

https://g.teddysc.me/5c5532cb807d1af05e8f94a100d5d758

This shows how complex the `create-or-replace-table-from-query` subcommand is.


### Function LOC Sunburst Chart

![](https://g.teddysc.me/442854d30153d0107fe5500a73d854a5?b)

https://g.teddysc.me/4b237bf675c319dabe99cf7edb6f79e2