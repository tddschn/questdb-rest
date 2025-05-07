#!/usr/bin/env python3
# qdb_search_tv_symbols.py

import argparse
import subprocess
import sys
import shlex
from typing import List, Optional

try:
    from pypika import Query, Table, Field, Criterion, functions as fn
    from pypika.terms import LiteralValue
except ImportError:
    print(
        "Error: pypika library not found. Please install it: pip install pypika",
        file=sys.stderr,
    )
    sys.exit(1)


# --- Argument Parsing ---
def setup_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search the tv_symbols_us table in QuestDB using pypika and qdb-cli.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""Examples:
  # Case-insensitive search for 'spy' in the ticker field
  %(prog)s spy

  # Case-insensitive search for 'apple' in the full field
  %(prog)s -f apple

  # Search for 'goog' in ticker, filtered by NASDAQ and NYSE namespaces
  %(prog)s goog -N NASDAQ NYSE

  # Dry run search for 'tsla' with --info for qdb-cli, and custom host
  %(prog)s tsla --dry-run --info --host myquestdb.local
""",
    )

    parser.add_argument(
        "search_query",
        help="The string to search for.",
    )
    parser.add_argument(
        "-f",
        "--full",
        action="store_true",
        help="Search in the 'full' field instead of the 'ticker' field.",
    )
    parser.add_argument(
        "-N",
        "--namespaces",
        nargs="*",
        metavar="NAMESPACE",
        default=[],
        help="Filter by one or more namespaces (e.g., AMEX NASDAQ).",
    )
    parser.add_argument(
        "-i",
        "--info",
        action="store_true",
        help="Pass the --info flag to underlying qdb-cli calls for its verbose logging.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the qdb-cli command that would be executed, but don't run it.",
    )
    parser.add_argument(
        "qdb_cli_args",
        nargs=argparse.REMAINDER,
        help="Remaining arguments are passed directly as global options to `qdb-cli` (e.g., --host, --port). Place these *after* script-specific options.",
    )
    return parser


# --- SQL Query Building ---
def build_sql_query(args: argparse.Namespace) -> str:
    tv_symbols_us = Table("tv_symbols_us")

    # Select specific columns as shown in the user's example output
    query_builder = Query.from_(tv_symbols_us).select(
        tv_symbols_us.namespace, tv_symbols_us.ticker, tv_symbols_us.full
    )

    conditions = []

    # Prepare search value: uppercase and escape single quotes for the SQL pattern literal
    search_value_pattern = args.search_query.upper().replace("'", "''")

    if args.full:
        # Case-insensitive search on 'full' field using "UPPER(column) ~ 'UPPER_PATTERN'"
        # fn.Upper(Field) generates UPPER("column_name") which is correct for QuestDB
        field_expr_sql = fn.Upper(tv_symbols_us.full).get_sql(quote_char='"')
        conditions.append(LiteralValue(f"{field_expr_sql} ~ '{search_value_pattern}'"))
    else:
        # Default: Case-insensitive search on 'ticker' field
        field_expr_sql = fn.Upper(tv_symbols_us.ticker).get_sql(quote_char='"')
        conditions.append(LiteralValue(f"{field_expr_sql} ~ '{search_value_pattern}'"))

    if args.namespaces:
        # pypika's isin handles quoting of values if they are string literals,
        # resulting in "namespace" IN ('VAL1', 'VAL2')
        conditions.append(tv_symbols_us.namespace.isin(args.namespaces))

    if conditions:
        # Criterion.all combines all conditions with AND
        query_builder = query_builder.where(Criterion.all(conditions))

    # Add semicolon for QuestDB convention
    sql_str = query_builder.get_sql(quote_char='"') + ";"
    return sql_str


# --- qdb-cli Runner ---
def run_qdb_cli(
    global_qdb_options: List[str],
    exec_specific_args: List[str],
    dry_run: bool = False,
    script_info_flag: bool = False,  # To control this script's "+ Running" message
) -> None:
    """
    Constructs and runs the qdb-cli command.
    `global_qdb_options` are options like --host, --port, and script's --info.
    `exec_specific_args` are arguments for the `exec` subcommand itself (e.g., -q SQL --psql).
    """
    cmd_list = ["qdb-cli"]
    cmd_list.extend(global_qdb_options)  # Global options first
    cmd_list.append("exec")  # Then the subcommand
    cmd_list.extend(exec_specific_args)  # Then subcommand's arguments

    if dry_run:
        print(f"Dry run: {shlex.join(cmd_list)}")
        return

    if script_info_flag:  # If this script's -i/--info was used
        print(f"+ Running: {shlex.join(cmd_list)}", file=sys.stderr)

    try:
        result = subprocess.run(
            cmd_list,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        # Print stdout (the actual results from qdb-cli)
        sys.stdout.write(result.stdout)

        # Print stderr (logs from qdb-cli, if qdb-cli's --info was passed via global_qdb_options) to stderr
        if result.stderr:
            sys.stderr.write(result.stderr)

    except FileNotFoundError:
        print("Error: 'qdb-cli' command not found.", file=sys.stderr)
        print("Please ensure it's installed and in your PATH.", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(
            f"Error: qdb-cli command failed with exit code {e.returncode}.",
            file=sys.stderr,
        )
        if e.stdout:
            print(f"  qdb-cli stdout:\n{e.stdout.strip()}", file=sys.stderr)
        if e.stderr:
            print(f"  qdb-cli stderr:\n{e.stderr.strip()}", file=sys.stderr)
        sys.exit(e.returncode)
    except Exception as e:
        print(f"An unexpected error occurred running qdb-cli: {e}", file=sys.stderr)
        sys.exit(1)


# --- Main Execution ---
def main():
    parser = setup_arg_parser()
    args = parser.parse_args()

    if not args.search_query:
        # argparse usually handles this if 'search_query' is not optional.
        # This check is a safeguard if its definition changes.
        parser.error("the following arguments are required: search_query")

    sql_query = build_sql_query(args)

    # Global options for qdb-cli (connection details, --info for qdb-cli itself)
    qdb_global_options = []
    if args.info:  # This script's --info flag controls qdb-cli's --info flag
        qdb_global_options.append("--info")
    if args.qdb_cli_args:  # REMAINDER args like --host, --port
        qdb_global_options.extend(args.qdb_cli_args)

    # Arguments for the `qdb-cli exec` subcommand itself
    qdb_exec_specific_args = ["-q", sql_query, "--psql"]

    run_qdb_cli(
        global_qdb_options=qdb_global_options,
        exec_specific_args=qdb_exec_specific_args,
        dry_run=args.dry_run,
        script_info_flag=args.info,  # Pass this script's info flag to control "+ Running"
    )


if __name__ == "__main__":
    main()
