#!/usr/bin/env python3

import argparse
import subprocess
import sys
import re
from typing import List, Optional, Sequence

from pypika import Query, Table, Field, Order, Criterion
from pypika import functions as fn
from pypika.terms import LiteralValue # To use Criterion.raw for regex

# --- Constants ---
UUID_REGEX = r"[0-9a-f]{8}([-_][0-9a-f]{4}){3}[-_][0-9a-f]{12}"
KNOWN_TABLES_COLS = [
    "id", "table_name", "designatedTimestamp", "partitionBy",
    "maxUncommittedRows", "o3MaxLag", "walEnabled", "directoryName",
    "dedup", "ttlValue", "ttlUnit", "matView"
]
PARTITION_BY_CHOICES = ['NONE', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'WEEK']

# --- Argument Parser ---
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Get a list of table names or full table info from QuestDB, with filtering and sorting options.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""Examples:
  # list all table names (default order)
  %(prog)s

  # list tables NOT matching 'backup_'
  %(prog)s -v backup_

  # list tables partitioned by YEAR or MONTH
  %(prog)s -P YEAR,MONTH

  # list WAL tables with deduplication enabled and a designated timestamp
  %(prog)s -d -t

  # show full info for tables starting with 'trade', partitioned by DAY
  %(prog)s -f -P DAY trades_

  # show full info for tables matching 'schwab' with length >= 10 (options after regex)
  %(prog)s schwab -l 10 -f

  # list tables matching 'schwab' that have no designated timestamp
  %(prog)s schwab -T

  # list tables sorted by name descending, limit 10
  %(prog)s -s table_name -r -n 10
"""
    )

    # Positional Argument
    parser.add_argument(
        "regex",
        nargs="?",
        help="Optional positional argument: Regex pattern to match table names (uses '~')."
    )

    # Filtering Options
    filter_group = parser.add_argument_group("Filtering Options")
    filter_group.add_argument(
        "-v", "--inverse-match",
        action="store_true",
        help="Use inverse regex match ('!~') for the positional regex."
    )
    uuid_group = filter_group.add_mutually_exclusive_group()
    uuid_group.add_argument(
        "-u", "--uuid",
        action="store_true",
        help="Only tables containing a UUID-4 in their name."
    )
    uuid_group.add_argument(
        "-U", "--no-uuid",
        action="store_true",
        help="Only tables NOT containing a UUID-4 in their name."
    )
    filter_group.add_argument(
        "-P", "--partitionBy",
        metavar="P",
        help=f"Filter by partitioning strategy. P is a comma-separated list "
             f"(no spaces) of values like {','.join(PARTITION_BY_CHOICES)}. "
             f"Example: -P YEAR,MONTH"
    )
    ts_group = filter_group.add_mutually_exclusive_group()
    ts_group.add_argument(
        "-t", "--has-designated-timestamp",
        action="store_true",
        help="Only tables that have a designated timestamp column."
    )
    ts_group.add_argument(
        "-T", "--no-designated-timestamp",
        action="store_true",
        help="Only tables that do NOT have a designated timestamp column."
    )
    dedup_group = filter_group.add_mutually_exclusive_group()
    dedup_group.add_argument(
        "-d", "--dedup-enabled",
        action="store_true",
        help="Only tables with deduplication enabled."
    )
    dedup_group.add_argument(
        "-D", "--dedup-disabled",
        action="store_true",
        help="Only tables with deduplication disabled."
    )
    filter_group.add_argument(
        "-l", "--min-length",
        type=int,
        metavar="N",
        help="Only tables with name length >= N."
    )
    filter_group.add_argument(
        "-L", "--max-length",
        type=int,
        metavar="N",
        help="Only tables with name length <= N."
    )

    # Sorting & Limiting
    sort_limit_group = parser.add_argument_group("Sorting & Limiting")
    sort_limit_group.add_argument(
        "-s", "--sort",
        nargs="?",
        const="table_name", # Value if flag is present without arg
        default=None,       # Value if flag is not present
        metavar="COL",
        help=f"Sort results by column COL. Defaults to 'table_name' if COL is omitted. "
             f"Available: {', '.join(KNOWN_TABLES_COLS)}. "
             f"If -s is not used, results are typically ordered by 'id'."
    )
    sort_limit_group.add_argument(
        "-r", "--reverse",
        action="store_true",
        help="Reverse the sort order (requires -s). Appends DESC to ORDER BY."
    )
    sort_limit_group.add_argument(
        "-n", "--limit",
        type=int,
        metavar="N",
        help="Limit the number of results returned. Passes '-l N' to qdb-cli."
    )

    # Output Options
    output_group = parser.add_argument_group("Output Options")
    output_group.add_argument(
        "-f", "--full-cols",
        action="store_true",
        help="Show all columns from the 'tables' table in PSQL format. "
             "(Default: shows only table names, one per line)"
    )

    return parser

# --- Main Execution ---
def main():
    parser = build_parser()
    args = parser.parse_args()

    # --- Argument Validation ---
    if args.reverse and args.sort is None:
        parser.error("-r/--reverse requires -s/--sort to be specified.")

    if args.sort is not None and args.sort not in KNOWN_TABLES_COLS:
        parser.error(f"Invalid sort column '{args.sort}'. "
                     f"Available columns: {', '.join(KNOWN_TABLES_COLS)}")

    partition_by_filters = []
    if args.partitionBy:
        partition_by_filters = args.partitionBy.split(',')
        for p_val in partition_by_filters:
            if p_val.upper() not in PARTITION_BY_CHOICES:
                 parser.error(f"Invalid partitionBy value '{p_val}'. "
                              f"Choices: {', '.join(PARTITION_BY_CHOICES)}")

    # --- Build Pypika Query ---
    tables_table = Table("tables") # Represents the tables() function/table
    query = Query.from_(tables_table)

    # Select columns
    if args.full_cols:
        query = query.select(tables_table.star)
    else:
        query = query.select(tables_table.table_name)

    # Build WHERE conditions
    conditions: List[Criterion] = []

    if args.regex:
        operator = "!~" if args.inverse_match else "~"
        # Escape single quotes for SQL string literal
        safe_regex = args.regex.replace("'", "''")
        # Use Criterion.raw as pypika doesn't have built-in regex operators
        conditions.append(Criterion.raw(f"table_name {operator} '{safe_regex}'"))

    if args.uuid:
        conditions.append(Criterion.raw(f"table_name ~ '{UUID_REGEX}'"))
    elif args.no_uuid:
        conditions.append(Criterion.raw(f"table_name !~ '{UUID_REGEX}'"))

    if partition_by_filters:
        # Case-insensitive comparison is safer if DB supports it, otherwise uppercase
        conditions.append(tables_table.partitionBy.isin([p.upper() for p in partition_by_filters]))

    if args.has_designated_timestamp:
        conditions.append(tables_table.designatedTimestamp.notnull())
    elif args.no_designated_timestamp:
        conditions.append(tables_table.designatedTimestamp.isnull())

    if args.dedup_enabled:
        conditions.append(tables_table.dedup.eq(True))
    elif args.dedup_disabled:
        conditions.append(tables_table.dedup.eq(False))

    if args.min_length is not None:
        conditions.append(fn.Length(tables_table.table_name) >= args.min_length)
    if args.max_length is not None:
        conditions.append(fn.Length(tables_table.table_name) <= args.max_length)

    # Apply combined conditions
    if conditions:
        # Combine all conditions with AND
        combined_criterion = conditions[0]
        for next_criterion in conditions[1:]:
            combined_criterion &= next_criterion
        query = query.where(combined_criterion)

    # Apply sorting
    if args.sort:
        sort_order = Order.desc if args.reverse else Order.asc
        # Pypika handles quoting if necessary based on Field name
        query = query.orderby(Field(args.sort), order=sort_order)

    # --- Build qdb-cli Command ---
    sql_string = query.get_sql()

    command: List[str] = ["qdb-cli", "exec", "-q", sql_string]

    if args.limit is not None:
        command.extend(["-l", str(args.limit)])

    if args.full_cols:
        command.append("--psql")
    else:
        command.extend(["-x", "table_name"])

    # --- Execute Command ---
    # print(f"Executing: {' '.join(command)}", file=sys.stderr) # Optional debug
    try:
        # Run and let qdb-cli print directly to stdout/stderr
        # check=True raises CalledProcessError if qdb-cli returns non-zero
        process = subprocess.run(command, check=True, text=True)

    except FileNotFoundError:
        print("Error: 'qdb-cli' command not found. Is it installed and in your PATH?", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        # qdb-cli likely printed its own error message to stderr already
        print(f"Error executing qdb-cli: {e}", file=sys.stderr)
        sys.exit(e.returncode)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()