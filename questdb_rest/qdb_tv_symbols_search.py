#!/usr/bin/env python3
# qdb-search-symbols.py

import argparse
import subprocess
import sys
import shlex
from typing import List, Dict, Any, Optional

# Attempt to import pypika, provide guidance if missing
try:
    from pypika import (
        Query,
        Table,
        Field,
        Criterion,
    )  # functions as fn (fn not used in this version)
    # LiteralValue is not needed with the string-appending approach for raw conditions
except ImportError:
    print(
        "Error: pypika library not found. Please install it: pip install pypika",
        file=sys.stderr,
    )
    sys.exit(1)

DEFAULT_TABLE_NAME = "tv_symbols_us"


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"Search a symbols table (default: '{DEFAULT_TABLE_NAME}') in QuestDB using qdb-cli and pypika.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=f"""Examples:
  # Search for 'spy' (case-insensitive) in the 'ticker' field of '{DEFAULT_TABLE_NAME}'
  {sys.argv[0]} spy

  # Search for 'SPDR' (case-insensitive) in the 'full' field
  {sys.argv[0]} SPDR --field full

  # Search for 'SPY' (case-sensitive) in the 'ticker' field
  {sys.argv[0]} SPY --case-sensitive

  # Search for 'BTC' (case-insensitive) in 'ticker', filter by 'BINANCE' and 'COINBASE' namespaces
  {sys.argv[0]} BTC -n BINANCE COINBASE

  # Search in a different table
  {sys.argv[0]} mypattern --table-name other_symbols_table

  # Dry run the command
  {sys.argv[0]} aapl --dry-run

  # Pass connection arguments to qdb-cli
  {sys.argv[0]} tsla --host my.questdb.instance --port 9001
""",
    )
    parser.add_argument(
        "search_term",
        help="The term/pattern to search for. Treated as a regex pattern for QuestDB's '~' operator.",
    )
    parser.add_argument(
        "--field",
        choices=["ticker", "full"],
        default="ticker",
        help="Field to search in (default: ticker).",
    )
    parser.add_argument(
        "--case-sensitive",
        action="store_true",
        help="Perform a case-sensitive search (default is case-insensitive).",
    )
    parser.add_argument(
        "-n",
        "--namespaces",
        nargs="*",
        default=[],
        metavar="NAMESPACE",
        help="Filter by one or more namespaces (e.g., AMEX NASDAQ).",
    )
    parser.add_argument(
        "--table-name",
        default=DEFAULT_TABLE_NAME,
        help=f"Name of the table to search (default: {DEFAULT_TABLE_NAME}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the qdb-cli command instead of executing it.",
    )
    parser.add_argument(
        "-i",
        "--info",
        action="store_true",
        help="Pass the --info flag to qdb-cli for verbose logging, and enable verbose script logging.",
    )
    parser.add_argument(
        "qdb_cli_args",
        nargs=argparse.REMAINDER,
        help="Remaining arguments are passed directly to `qdb-cli` (e.g., --host, --port, auth details). Place these *after* script-specific options.",
    )
    return parser


def build_sql_query(args: argparse.Namespace) -> str:
    """Builds the SQL query string using pypika."""

    target_table = Table(args.table_name)
    # PyPika automatically quotes table and field names with double quotes.
    query_builder = Query.from_(target_table).select(
        target_table.namespace, target_table.ticker, target_table.full
    )

    pypika_conditions = []
    raw_sql_conditions = []

    # Prepare search term and field expression for raw SQL
    # Escape single quotes in the user-provided search term for SQL literal
    search_pattern_escaped = args.search_term.replace("'", "''")
    # Field name for raw SQL must be explicitly double-quoted if it needs to be case-sensitive
    # or contains special characters. PyPika does this by default for its Field objects.
    # For raw SQL, we ensure it's double-quoted.
    field_to_search_in_raw_sql = f'"{args.field}"'

    if args.case_sensitive:
        # QuestDB's ~ operator is case-sensitive by default
        raw_sql_conditions.append(
            f"{field_to_search_in_raw_sql} ~ '{search_pattern_escaped}'"
        )
    else:
        # For case-insensitive, apply UPPER to both field and pattern in raw SQL
        # The pattern should also be uppercased.
        pattern_uppercase_escaped = search_pattern_escaped.upper()
        raw_sql_conditions.append(
            f"UPPER({field_to_search_in_raw_sql}) ~ '{pattern_uppercase_escaped}'"
        )

    # Namespace filtering (can be handled by PyPika's `isin`)
    if args.namespaces:
        # target_table.namespace will be correctly quoted by PyPika (e.g., "namespace")
        # PyPika's isin method correctly quotes string literals in the list.
        pypika_conditions.append(target_table.namespace.isin(args.namespaces))

    # Apply PyPika-native conditions first
    if pypika_conditions:
        query_builder = query_builder.where(Criterion.all(pypika_conditions))

    # Get the SQL string built so far by PyPika
    sql_string = query_builder.get_sql()  # Using get_sql() is preferred over str()

    # Append raw SQL conditions
    if raw_sql_conditions:
        raw_sql_part = " AND ".join(raw_sql_conditions)
        # Check if PyPika already added a WHERE clause
        if " WHERE " in sql_string.upper():
            # Append using AND. Parenthesize the raw part for safety.
            sql_string += f" AND ({raw_sql_part})"
        else:
            # Add a new WHERE clause
            sql_string += f" WHERE {raw_sql_part}"

    # Adding a semicolon for QuestDB convention.
    return sql_string + ";"


def parse_qdb_cli_remainder_args(remainder_args: List[str]) -> (List[str], List[str]):
    """
    Parses remainder arguments into connection-related and other passthrough args.
    This is a simplified parser; qdb-cli itself has more sophisticated parsing.
    """
    qdb_connection_args: List[str] = []
    qdb_passthrough_args: List[str] = []

    i = 0
    while i < len(remainder_args):
        arg = remainder_args[i]
        # Known connection-related arguments that take a value
        if arg in (
            "-H",
            "--host",
            "--port",
            "-u",
            "--user",
            "-p",
            "--password",
            "--timeout",
            "--scheme",
            "--config",
        ):
            qdb_connection_args.append(arg)
            if i + 1 < len(remainder_args) and not remainder_args[i + 1].startswith(
                "-"
            ):
                qdb_connection_args.append(remainder_args[i + 1])
                i += 1
            else:
                # Option expecting arg but next is another option or end of list.
                # qdb-cli will handle this error.
                print(
                    f"Warning: Argument {arg} might be missing a value in passthrough args.",
                    file=sys.stderr,
                )
        else:
            qdb_passthrough_args.append(arg)
        i += 1

    return qdb_connection_args, qdb_passthrough_args


def run_command_with_qdb_cli(
    sql_query: str,
    qdb_connection_args: List[str],
    qdb_passthrough_args: List[str],
    dry_run: bool,
    script_info_flag: bool,
) -> None:
    """
    Constructs and runs the qdb-cli command.
    """
    cmd = ["qdb-cli"]

    if script_info_flag:
        # Check if --info or -i is already present to avoid duplication
        has_info_flag = any(
            arg in ("-i", "--info")
            for arg in qdb_connection_args + qdb_passthrough_args
        )
        if not has_info_flag:
            cmd.append("--info")

    cmd.extend(qdb_connection_args)

    # Core command: exec -q <SQL> --psql
    # Any passthrough args that are not connection args are assumed to be for 'exec'
    cmd.extend(["exec"])
    cmd.extend(qdb_passthrough_args)
    cmd.extend(
        ["-q", sql_query, "--psql"]
    )  # -q and --psql are specific to this script's exec call

    if dry_run:
        print(f"Dry run: {shlex.join(cmd)}")
        return

    if script_info_flag:
        print(f"+ Running: {shlex.join(cmd)}", file=sys.stderr)

    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.stdout:
            print(result.stdout, end="")

        if (
            script_info_flag and result.stderr
        ):  # Only show qdb-cli stderr if script's info flag is on
            print(f"  qdb-cli stderr:\n{result.stderr.strip()}", file=sys.stderr)

    except FileNotFoundError:
        print(
            "Error: 'qdb-cli' command not found. Please ensure it's installed and in your PATH.",
            file=sys.stderr,
        )
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(
            f"Error: qdb-cli command failed with exit code {e.returncode}.",
            file=sys.stderr,
        )
        if e.stdout:  # Show stdout/stderr from failed command for debugging
            print(f"  qdb-cli stdout:\n{e.stdout.strip()}", file=sys.stderr)
        if e.stderr:
            print(f"  qdb-cli stderr:\n{e.stderr.strip()}", file=sys.stderr)
        sys.exit(e.returncode)
    except Exception as e:
        print(
            f"An unexpected error occurred while running qdb-cli: {e}", file=sys.stderr
        )
        sys.exit(1)


def main():
    parser = setup_arg_parser()
    args = parser.parse_args()

    if not args.search_term:
        # Argparse should handle this if 'search_term' is not optional,
        # but good to have a check if it were made optional.
        parser.error("the following arguments are required: search_term")

    qdb_conn_args, qdb_passthrough_exec_args = parse_qdb_cli_remainder_args(
        args.qdb_cli_args
    )

    if args.info:  # Script's own --info logging
        print(f"Script args: {args}", file=sys.stderr)
        print(f"Parsed qdb_connection_args: {qdb_conn_args}", file=sys.stderr)
        print(
            f"Parsed qdb_passthrough_exec_args (for 'exec'): {qdb_passthrough_exec_args}",
            file=sys.stderr,
        )

    sql_query = build_sql_query(args)
    if args.info:
        print(f"Generated SQL query: {sql_query}", file=sys.stderr)

    run_command_with_qdb_cli(
        sql_query, qdb_conn_args, qdb_passthrough_exec_args, args.dry_run, args.info
    )


if __name__ == "__main__":
    main()
