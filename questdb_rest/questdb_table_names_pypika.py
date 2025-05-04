#!/usr/bin/env python3

import argparse
import subprocess
import sys
from typing import List, Optional, Tuple

# Attempt to import pypika, provide guidance if missing
try:
    from pypika import (
        Criterion,
        Field,
        Order,
        Query,
        Table,
        functions as fn,
    )
    from pypika.queries import QueryBuilder
    from pypika.terms import PseudoColumn, LiteralValue
except ImportError:
    print(
        "Error: pypika library not found. Please install it: pip install pypika",
        file=sys.stderr,
    )
    sys.exit(1)

# --- Constants ---
# Regex matching UUID-4 with either dashes or underscores (same as bash)
UUID_REGEX = r"[0-9a-f]{8}([-_][0-9a-f]{4}){3}[-_][0-9a-f]{12}"

# Known columns in the 'tables()' function result for validation
KNOWN_TABLES_COLS = [
    "id",
    "table_name",  # Changed from name for clarity with Field name
    "designatedTimestamp",
    "partitionBy",
    "maxUncommittedRows",
    "o3MaxLag",
    "walEnabled",
    "directoryName",
    "dedup",
    "ttlValue",
    "ttlUnit",
    "matView",
]

PARTITION_OPTIONS = ["NONE", "YEAR", "MONTH", "DAY", "HOUR", "WEEK"]

# --- Argument Parsing ---


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Get a list of table names or full table info from QuestDB, with filtering and sorting options.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""Examples:
  # list all table names (default order)
  {prog}

  # list tables NOT matching 'backup_'
  {prog} -v backup_

  # list tables partitioned by YEAR or MONTH
  {prog} -P YEAR,MONTH

  # list WAL tables with deduplication enabled and a designated timestamp
  {prog} -d -t

  # show full info for tables starting with 'trade', partitioned by DAY
  {prog} -f -P DAY trades_

  # show full info for tables matching 'schwab' with length >= 10 (options after regex)
  {prog} schwab -l 10 -f

  # list tables matching 'schwab' that have no designated timestamp
  {prog} schwab -T

  # list tables sorted by name descending, limit 10
  {prog} -s table_name -r -n 10
""".format(
            prog="qdb-table-names.py"
        ),  # Use python script name
    )

    # --- Filtering Options ---
    parser.add_argument(
        "regex",
        nargs="?",
        help="Optional positional argument: Regex pattern to match table names (uses '~').",
    )
    parser.add_argument(
        "-v",
        "--inverse-match",
        action="store_true",
        help="Use inverse regex match ('!~') for the positional regex.",
    )

    uuid_group = parser.add_mutually_exclusive_group()
    uuid_group.add_argument(
        "-u",
        "--uuid",
        action="store_true",
        help="Only tables containing a UUID-4 in their name.",
    )
    uuid_group.add_argument(
        "-U",
        "--no-uuid",
        action="store_true",
        help="Only tables NOT containing a UUID-4 in their name.",
    )

    parser.add_argument(
        "-P",
        "--partitionBy",
        metavar="P",
        help="Filter by partitioning strategy. P is a comma-separated list "
        f"(no spaces) of values like {','.join(PARTITION_OPTIONS)}. Example: -P YEAR,MONTH",
    )

    ts_group = parser.add_mutually_exclusive_group()
    ts_group.add_argument(
        "-t",
        "--has-designated-timestamp",
        action="store_true",
        help="Only tables that have a designated timestamp column.",
    )
    ts_group.add_argument(
        "-T",
        "--no-designated-timestamp",
        action="store_true",
        help="Only tables that do NOT have a designated timestamp column.",
    )

    dedup_group = parser.add_mutually_exclusive_group()
    dedup_group.add_argument(
        "-d",
        "--dedup-enabled",
        action="store_true",
        help="Only tables with deduplication enabled.",
    )
    dedup_group.add_argument(
        "-D",
        "--dedup-disabled",
        action="store_true",
        help="Only tables with deduplication disabled.",
    )

    parser.add_argument(
        "-l", "--min-length", type=int, help="Only tables with name length >= N."
    )
    parser.add_argument(
        "-L", "--max-length", type=int, help="Only tables with name length <= N."
    )

    # --- Sorting & Limiting ---
    parser.add_argument(
        "-s",
        "--sort",
        nargs="?",
        const="table_name",  # Default value if -s is present without an argument
        metavar="COL",
        help="Sort results by column COL. Defaults to 'table_name' if COL is omitted. "
        f"Available columns: {', '.join(KNOWN_TABLES_COLS)}. "
        "If -s is not used, results are typically ordered by 'id'.",
    )
    parser.add_argument(
        "-r",
        "--reverse",
        action="store_true",
        help="Reverse the sort order (requires -s). Appends DESC to ORDER BY.",
    )
    parser.add_argument(
        "-n",
        "--limit",
        type=int,
        metavar="N",
        help="Limit the number of results returned. Passes '-l N' to qdb-cli.",
    )

    # --- Output Options ---
    parser.add_argument(
        "-f",
        "--full-cols",
        action="store_true",
        help="Show all columns from the 'tables' table in PSQL format. "
        "(Default: shows only table names, one per line)",
    )

    return parser


# --- Helper to validate arguments after parsing ---
def validate_args(args: argparse.Namespace):
    if args.reverse and not args.sort:
        print(
            "Error: -r|--reverse requires -s|--sort to be specified.", file=sys.stderr
        )
        sys.exit(1)

    if args.sort and args.sort not in KNOWN_TABLES_COLS:
        print(f"Error: Invalid sort column '{args.sort}'.", file=sys.stderr)
        print(
            f"Available columns: {', '.join(KNOWN_TABLES_COLS)}",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.partitionBy:
        partitions = args.partitionBy.split(",")
        invalid_partitions = [
            p for p in partitions if p.strip().upper() not in PARTITION_OPTIONS
        ]
        if invalid_partitions:
            print(
                f"Error: Invalid partitionBy value(s): {','.join(invalid_partitions)}",
                file=sys.stderr,
            )
            print(f"Available options: {','.join(PARTITION_OPTIONS)}", file=sys.stderr)
            sys.exit(1)


# --- Build the SQL query string ---
def build_sql_query(args: argparse.Namespace) -> str:
    # Use PseudoColumn for 'tables' as it's a function call in QuestDB SQL
    tables_func = PseudoColumn("tables()")
    # Treat the result like a table for selection/filtering/ordering
    tables_table = Table("tables")  # PyPika needs a Table object here

    # Start building the query with SELECT and FROM
    query = Query.from_(tables_func).select(
        tables_table.star if args.full_cols else tables_table.table_name
    )

    # --- Build WHERE Clause ---
    # Collect conditions supported directly by PyPika
    pypika_conditions: List[Criterion] = []

    # Partition By Filter
    if args.partitionBy:
        partitions = [p.strip().upper() for p in args.partitionBy.split(",")]
        if partitions:
            pypika_conditions.append(tables_table.partitionBy.isin(partitions))

    # Designated Timestamp Filter
    if args.has_designated_timestamp:
        pypika_conditions.append(tables_table.designatedTimestamp.isnotnull())
    elif args.no_designated_timestamp:
        pypika_conditions.append(tables_table.designatedTimestamp.isnull())

    # Deduplication Filter
    if args.dedup_enabled:
        pypika_conditions.append(tables_table.dedup == True)
    elif args.dedup_disabled:
        pypika_conditions.append(tables_table.dedup == False)

    # Length Filters
    if args.min_length is not None:
        pypika_conditions.append(fn.Length(tables_table.table_name) >= args.min_length)
    if args.max_length is not None:
        pypika_conditions.append(fn.Length(tables_table.table_name) <= args.max_length)

    # Apply PyPika conditions if any exist
    if pypika_conditions:
        query = query.where(Criterion.all(pypika_conditions))

    # Collect conditions requiring raw SQL strings
    raw_sql_conditions: List[str] = []

    # Regex Filter
    if args.regex:
        operator = "!~" if args.inverse_match else "~"
        # Basic escaping for single quotes in the pattern
        safe_regex = args.regex.replace("'", "''")
        # Use the column name directly in the raw string
        raw_sql_conditions.append(f"table_name {operator} '{safe_regex}'")

    # UUID Filter
    if args.uuid:
        raw_sql_conditions.append(f"table_name ~ '{UUID_REGEX}'")
    elif args.no_uuid:
        raw_sql_conditions.append(f"table_name !~ '{UUID_REGEX}'")

    # Get the SQL string generated by PyPika so far
    # Use get_sql() for consistency, though str() often works
    sql_string = query.get_sql()

    # Append raw SQL conditions if any exist
    if raw_sql_conditions:
        raw_sql_part = " AND ".join(raw_sql_conditions)
        if query._wheres:  # Check if PyPika already added a WHERE clause
            # Append using AND
            sql_string += f" AND ({raw_sql_part})"
        else:
            # Add a new WHERE clause
            sql_string += f" WHERE {raw_sql_part}"

    # Apply ORDER BY (append manually as well for simplicity)
    if args.sort:
        # Quote the sort column name for safety (PyPika's Field doesn't quote automatically here)
        sort_col_sql = LiteralValue(args.sort).get_sql(quote_char='"')
        order_by_clause = f" ORDER BY {sort_col_sql}"
        if args.reverse:
            order_by_clause += " DESC"
        sql_string += order_by_clause

    # --- Post-processing for QuestDB syntax ---
    # Pypika doesn't quote function calls like tables(), remove quotes manually
    sql_string = sql_string.replace('"tables()"', "tables()")
    # Pypika might quote fields from the pseudo-table 'tables', remove those too
    sql_string = sql_string.replace('"tables".', "")

    return sql_string


# --- Build the qdb-cli command list ---
def build_cli_command(args: argparse.Namespace, sql_query: str) -> List[str]:
    cmd = ["qdb-cli", "exec", "-q", sql_query]

    if args.full_cols:
        cmd.append("--psql")
    else:
        # If only showing names, extract that column via qdb-cli
        cmd.extend(["-x", "table_name"])

    if args.limit is not None:
        cmd.extend(["-l", str(args.limit)])

    return cmd


# --- Run the command ---
def run_command(command: List[str]):
    try:
        # Set encoding for reliable text processing
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace", # Handle potential decoding errors
        )
        # Print stdout (the actual results)
        print(result.stdout, end="")
        # Print stderr (logs, warnings) to stderr
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")

    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {' '.join(command)}", file=sys.stderr)
        print(f"Return Code: {e.returncode}", file=sys.stderr)
        if e.stdout:
            print(f"stdout:\n{e.stdout}", file=sys.stderr)
        if e.stderr:
            print(f"stderr:\n{e.stderr}", file=sys.stderr)
        sys.exit(e.returncode)
    except FileNotFoundError:
        print("Error: 'qdb-cli' command not found.", file=sys.stderr)
        print("Please ensure it's installed and in your PATH.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)


# --- Main Execution ---
def main():
    parser = setup_arg_parser()
    args = parser.parse_args()
    validate_args(args)

    sql_query = build_sql_query(args)
    cli_command = build_cli_command(args, sql_query)

    # Optional: Print the command for debugging
    # print(f"Executing: {' '.join(cli_command)}", file=sys.stderr)

    run_command(cli_command)


if __name__ == "__main__":
    main()