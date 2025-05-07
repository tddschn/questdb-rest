#!/usr/bin/env python3
# qdb-search-symbols.py

import argparse
import subprocess
import sys
import shlex
from typing import List, Dict, Any, Optional

# Attempt to import pypika, provide guidance if missing
try:
    from pypika import Query, Table, Field, Criterion, functions as fn
    from pypika.terms import LiteralValue  # Not strictly needed if using Criterion.raw
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
    query = Query.from_(target_table).select(
        target_table.namespace, target_table.ticker, target_table.full
    )

    conditions = []

    # Prepare search term and field expression
    search_pattern = args.search_term.replace(
        "'", "''"
    )  # Basic SQL string literal escape
    search_on_field_name = args.field  # 'ticker' or 'full'

    if args.case_sensitive:
        # QuestDB's ~ operator is case-sensitive by default
        # "search_field_name" ~ 'pattern'
        conditions.append(
            Criterion.raw(f"\"{search_on_field_name}\" ~ '{search_pattern}'")
        )
    else:
        # UPPER("search_field_name") ~ 'PATTERN_UPPERCASE'
        processed_pattern = search_pattern.upper()
        conditions.append(
            Criterion.raw(f"UPPER(\"{search_on_field_name}\") ~ '{processed_pattern}'")
        )

    # Namespace filtering
    if args.namespaces:
        # Namespaces are typically case-sensitive.
        # If namespaces can contain single quotes, they'd need escaping for isin.
        # Pypika's isin should handle quoting of list items correctly.
        conditions.append(target_table.namespace.isin(args.namespaces))

    if conditions:
        query = query.where(Criterion.all(conditions))

    # Pypika by default double-quotes identifiers, which is fine for QuestDB.
    # Adding a semicolon for QuestDB convention.
    return str(query) + ";"


def parse_qdb_cli_remainder_args(remainder_args: List[str]) -> (List[str], List[str]):
    """
    Parses remainder arguments into connection-related and other passthrough args.
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
            if i + 1 < len(remainder_args):
                qdb_connection_args.append(remainder_args[i + 1])
                i += 1
            else:
                # This case (option expecting arg but none found) should be caught by qdb-cli itself.
                # Or we could raise an error here. For now, just pass it.
                if arg not in (
                    "--config"
                ):  # --config might be the last one if used without path
                    print(
                        f"Warning: Argument {arg} expects a value but none was found in passthrough args.",
                        file=sys.stderr,
                    )
        # Known boolean flags for connection/global settings (not passed to exec directly usually)
        # elif arg in ("-i", "--info", "-D", "--debug", "-R", "--dry-run"): # These are handled by script or qdb-cli main
        #    qdb_connection_args.append(arg) # Let qdb-cli handle its own global flags
        else:
            # Assume other arguments are for the `exec` command or general qdb-cli flags
            qdb_passthrough_args.append(arg)
        i += 1

    return qdb_connection_args, qdb_passthrough_args


def run_command_with_qdb_cli(
    sql_query: str,
    qdb_connection_args: List[str],
    qdb_passthrough_args: List[str],
    dry_run: bool,
    script_info_flag: bool,  # Script's own --info flag
) -> None:
    """
    Constructs and runs the qdb-cli command.
    """
    cmd = ["qdb-cli"]

    # Add script's --info to qdb-cli's global options if set
    if script_info_flag:
        # Avoid duplicate --info if already in qdb_connection_args or qdb_passthrough_args
        # This is a simple check; a more robust way would be to parse qdb_cli_args more thoroughly.
        already_has_info = any(
            arg == "--info" or arg == "-i"
            for arg in qdb_connection_args + qdb_passthrough_args
        )
        if not already_has_info:
            cmd.append("--info")

    cmd.extend(qdb_connection_args)

    # Core command: exec -q <SQL> --psql
    cmd.extend(["exec", "-q", sql_query, "--psql"])

    cmd.extend(qdb_passthrough_args)  # Add other passthrough args for `exec`

    if dry_run:
        print(f"Dry run: {shlex.join(cmd)}")
        return

    if script_info_flag:  # Script's own verbose logging
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

        # Print qdb-cli's stderr only if script's info flag is on, to avoid clutter
        if script_info_flag and result.stderr:
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
        if e.stdout:
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
        parser.error("the following arguments are required: search_term")

    qdb_conn_args, qdb_passthrough_exec_args = parse_qdb_cli_remainder_args(
        args.qdb_cli_args
    )

    if args.info:
        print(f"Script args: {args}", file=sys.stderr)
        print(f"Parsed qdb_connection_args: {qdb_conn_args}", file=sys.stderr)
        print(
            f"Parsed qdb_passthrough_exec_args: {qdb_passthrough_exec_args}",
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
