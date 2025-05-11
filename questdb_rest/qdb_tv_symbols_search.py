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

DEFAULT_TABLE_NAME_US = "tv_symbols_us"
DEFAULT_TABLE_NAME_ALL = "tv_symbols"


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search the tv_symbols_us table in QuestDB using pypika and qdb-cli.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="This script is for self use only, it won't work for you unless you have the same db as mine.\n\nExamples:\n  # Case-insensitive search for 'spy' in the ticker field\n  %(prog)s spy\n\n  # Case-insensitive search for 'apple' in the full field\n  %(prog)s -f apple\n\n  # Search for 'goog' in ticker, filtered by NASDAQ and NYSE namespaces\n  %(prog)s goog -N NASDAQ NYSE\n  \n  # Search for 'aapl' and output as CSV\n  %(prog)s aapl --csv\n\n  # Search for 'msft' and output as CSV without header\n  %(prog)s msft --csv --no-header\n\n  # Dry run search for 'tsla' with --info for qdb-cli, and custom host\n  %(prog)s tsla --dry-run --info --host myquestdb.local\n",
    )
    parser.add_argument("search_query", help="The string to search for.")
    parser.add_argument(
        "-a",
        "--all",
        help='Search in the "tv_symbols" table instead of "tv_symbols_us".',
        action="store_true",
        dest="search_all",
    )
    parser.add_argument(
        "-f",
        "--full",
        action="store_true",
        help="Search in the 'full' field instead of the 'ticker' field.",
    )
    parser.add_argument(
        "-n",
        "--namespaces",
        nargs="+",
        metavar="NAMESPACE",
        default=[],
        help="Filter by one or more namespaces (e.g., AMEX NASDAQ). Requires at least one value if -N is used.",
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
    output_group = parser.add_argument_group("Output Options")
    output_group.add_argument(
        "--csv",
        action="store_true",
        dest="csv_output",
        help="Output data as CSV using 'qdb-cli exp'. Default is PSQL table via 'qdb-cli exec'.",
    )
    output_group.add_argument(
        "--no-header",
        action="store_true",
        dest="no_header_csv",
        help="When using --csv, omit the header row from the CSV output (passes '--nm' to 'qdb-cli exp').",
    )
    # Removed the explicit "qdb_cli_args" with nargs=REMAINDER.
    # Passthrough args will be collected by parse_known_args() in main().
    return parser


# --- SQL Query Building ---


def build_sql_query(args: argparse.Namespace) -> str:
    if args.search_all:
        tv_symbols_us = Table("tv_symbols")
    else:
        tv_symbols_us = Table("tv_symbols_us")
    # Start with selecting columns
    query_builder = Query.from_(tv_symbols_us).select(
        tv_symbols_us.namespace, tv_symbols_us.ticker, tv_symbols_us.full
    )
    # Conditions that PyPika can build fluently (like IN)
    pypika_conditions: List[Criterion] = []
    if args.namespaces:
        pypika_conditions.append(
            tv_symbols_us.namespace.isin([x.upper() for x in args.namespaces])
        )
    if pypika_conditions:
        query_builder = query_builder.where(Criterion.all(pypika_conditions))
    # Get the SQL string built so far by PyPika
    sql_string = query_builder.get_sql(quote_char='"')
    # Now, build the raw SQL condition for the regex search
    raw_sql_search_conditions: List[str] = []
    search_value_pattern = args.search_query.upper().replace("'", "''")
    if args.full:
        # fn.Upper(Field).get_sql() is fine for getting the UPPER(column) part
        field_expr_sql = fn.Upper(tv_symbols_us.full).get_sql(quote_char='"')
        raw_sql_search_conditions.append(f"{field_expr_sql} ~ '{search_value_pattern}'")
    else:
        field_expr_sql = fn.Upper(tv_symbols_us.ticker).get_sql(quote_char='"')
        raw_sql_search_conditions.append(f"{field_expr_sql} ~ '{search_value_pattern}'")
    # Append raw SQL search conditions
    if raw_sql_search_conditions:
        # Join multiple raw conditions with AND if there were ever a need for more than one raw search part
        raw_sql_part = " AND ".join(raw_sql_search_conditions)
        # Check if PyPika already added a WHERE clause (from namespaces filter)
        if " WHERE " in sql_string.upper():
            # Append using AND
            sql_string += f" AND ({raw_sql_part})"
        else:
            # Add a new WHERE clause
            sql_string += f" WHERE {raw_sql_part}"
    # Add semicolon for QuestDB convention
    sql_string += ";"
    return sql_string


# --- qdb-cli Runner ---


def run_qdb_cli(
    global_qdb_options: List[str],
    subcmd_specific_args: List[str],
    dry_run: bool = False,
    script_info_flag: bool = False,
    subcommand: str = "exec",
) -> None:  # Renamed from exec_specific_args
    # Added subcommand parameter
    "\n    Constructs and runs the qdb-cli command.\n    `global_qdb_options` are options like --host, --port, and script's --info.\n    `subcmd_specific_args` are arguments for the `exec` or `exp` subcommand.\n"
    cmd_list = ["qdb-cli"]
    cmd_list.extend(global_qdb_options)
    cmd_list.append(subcommand)  # Use the specified subcommand
    cmd_list.extend(subcmd_specific_args)
    if dry_run:
        print(f"Dry run: {shlex.join(cmd_list)}")
        return
    if script_info_flag:
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
        sys.stdout.write(result.stdout)
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
    # Use parse_known_args to separate script args from passthrough args
    # Arguments defined in setup_arg_parser will be in 'args'.
    # Unrecognized arguments will be in 'passthrough_cli_args'.
    args, passthrough_cli_args = parser.parse_known_args()
    if not args.search_query:
        # This check might be redundant if search_query is mandatory and positional,
        # but it's a good safeguard.
        parser.error("the following arguments are required: search_query")
    if args.no_header_csv and (not args.csv_output):
        parser.error("--no-header can only be used with --csv.")
    sql_query = build_sql_query(args)
    qdb_global_options = []
    if args.info:  # This script's --info flag controls qdb-cli's --info flag
        qdb_global_options.append("--info")
    # Add the correctly separated passthrough arguments
    if passthrough_cli_args:
        qdb_global_options.extend(passthrough_cli_args)
    subcmd_to_run: str
    qdb_subcmd_specific_args: List[str]
    if args.csv_output:
        subcmd_to_run = "exp"
        # For 'qdb-cli exp', the query is a direct argument, not prefixed by -q
        qdb_subcmd_specific_args = [sql_query]
        if args.no_header_csv:
            qdb_subcmd_specific_args.append("--nm")
    else:
        subcmd_to_run = "exec"
        qdb_subcmd_specific_args = [
            "-q",
            sql_query,
            "--psql",
        ]  # For this script's "+ Running" message
    run_qdb_cli(
        global_qdb_options=qdb_global_options,
        subcmd_specific_args=qdb_subcmd_specific_args,
        dry_run=args.dry_run,
        script_info_flag=args.info,
        subcommand=subcmd_to_run,
    )


if __name__ == "__main__":
    main()
