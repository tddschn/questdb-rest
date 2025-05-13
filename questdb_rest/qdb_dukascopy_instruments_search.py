#!/usr/bin/env python3
# questdb_rest/qdbdcs.py
import argparse
import subprocess
import sys
import shlex
from typing import List, Optional

# Attempt pypika import
try:
    from pypika import Query, Table, Field, Criterion, functions as fn
    from pypika.terms import LiteralValue
except ImportError:
    print(
        "Error: pypika library not found. Please install it: pip install pypika",
        file=sys.stderr,
    )
    sys.exit(1)

# --- Constants ---
DEFAULT_TABLE_NAME = "dukascopy_instruments"
SEARCHABLE_FIELDS = ["instrument_id", "name", "description"]
DEFAULT_SEARCH_FIELD = "instrument_id"

# --- Argument Parsing ---


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"Search the {DEFAULT_TABLE_NAME} table in QuestDB using pypika and qdb-cli.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=f"""Examples:
  # Case-insensitive search for 'eurusd' in the default 'instrument_id' field
  %(prog)s -I eurusd

  # Case-sensitive search for 'Bitcoin vs US Dollar' in the 'name' field
  %(prog)s -f name "Bitcoin vs US Dollar"

  # Search for 'btc' (case-insensitive) in 'instrument_id', filter by group 'vccy'
  %(prog)s -I -g vccy btc

  # Search for 'Euro Bund' in 'description', output as CSV without header
  %(prog)s -f description "Euro Bund" --csv --no-header

  # Dry run search for 'adausd' with --info for qdb-cli, and custom host/port
  %(prog)s adausd --dry-run --info --host myquestdb.local --port 9001
""",
    )
    parser.add_argument("search_query", help="The string to search for.")

    parser.add_argument(
        "-f",
        "--field",
        choices=SEARCHABLE_FIELDS,
        default=DEFAULT_SEARCH_FIELD,
        help=f"Field to search in. Default: {DEFAULT_SEARCH_FIELD}. Choices: {', '.join(SEARCHABLE_FIELDS)}",
    )
    parser.add_argument(
        "-I",
        "--ignore-case",
        action="store_true",
        help="Perform case-insensitive search (uses LOWER() and ~).",
    )
    parser.add_argument(
        "-g",
        "--group",
        nargs="+",
        metavar="GROUP_ID",
        default=[],
        dest="group_ids",  # Changed dest name for clarity
        help="Filter by one or more group_id values (case-sensitive).",
    )

    # Execution/Output options (similar to qdbtvs)
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

    return parser


# --- SQL Query Building ---


def build_sql_query(args: argparse.Namespace) -> str:
    """Builds the SQL query using pypika and manual string appending for regex."""
    instruments_table = Table(DEFAULT_TABLE_NAME)

    # Start with selecting all columns
    query_builder = Query.from_(instruments_table).select(instruments_table.star)

    # --- Build WHERE conditions ---
    pypika_conditions: List[Criterion] = []
    raw_sql_conditions: List[str] = []

    # 1. Group ID Filter (PyPika)
    if args.group_ids:
        # Case-sensitive match for group IDs typically
        pypika_conditions.append(instruments_table.group_id.isin(args.group_ids))

    # 2. Primary Search Filter (Raw SQL for '~' operator)
    search_field = Field(args.field)
    search_value = args.search_query

    # Escape single quotes in the search value for SQL safety
    safe_search_value = search_value.replace("'", "''")

    if args.ignore_case:
        # Use LOWER() on both sides for case-insensitive regex match
        # fn.Lower(search_field).get_sql() correctly generates LOWER("field_name")
        column_expression = fn.Lower(search_field).get_sql(quote_char='"')
        # The pattern for ~ should also be lowercased
        pattern = safe_search_value.lower()
        raw_sql_conditions.append(f"{column_expression} ~ '{pattern}'")
    else:
        # Case-sensitive regex match
        column_expression = search_field.get_sql(quote_char='"')
        pattern = safe_search_value
        raw_sql_conditions.append(f"{column_expression} ~ '{pattern}'")

    # --- Combine conditions ---
    # Apply PyPika conditions first
    if pypika_conditions:
        query_builder = query_builder.where(Criterion.all(pypika_conditions))

    # Get the SQL built by PyPika so far
    sql_string = query_builder.get_sql(quote_char='"')

    # Append raw SQL conditions
    if raw_sql_conditions:
        raw_sql_part = " AND ".join(raw_sql_conditions)
        # Check if PyPika already added a WHERE clause
        if " WHERE " in sql_string.upper():
            sql_string += f" AND ({raw_sql_part})"
        else:
            sql_string += f" WHERE {raw_sql_part}"

    # Add semicolon
    sql_string += ";"
    return sql_string


# --- qdb-cli Runner (Copied from qdbtvs, slightly adjusted comments) ---


def run_qdb_cli(
    global_qdb_options: List[str],
    subcmd_specific_args: List[str],
    dry_run: bool = False,
    script_info_flag: bool = False,
    subcommand: str = "exec",
) -> None:
    """
    Constructs and runs the qdb-cli command.
    `global_qdb_options` are options like --host, --port, and script's --info.
    `subcmd_specific_args` are arguments for the `exec` or `exp` subcommand.
    """
    cmd_list = ["qdb-cli"]
    cmd_list.extend(global_qdb_options)
    cmd_list.append(subcommand)
    cmd_list.extend(subcmd_specific_args)

    if dry_run:
        print(f"Dry run: {shlex.join(cmd_list)}")
        return

    if script_info_flag:  # Use the script's --info flag to decide whether to print this
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
        # Only print qdb-cli's stderr if the script's --info flag was set
        if script_info_flag and result.stderr:
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
        # Print captured output regardless of script's --info flag on error
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
    # Separate script args from passthrough args for qdb-cli
    args, passthrough_cli_args = parser.parse_known_args()

    # Validate arguments
    if args.no_header_csv and not args.csv_output:
        parser.error("--no-header can only be used with --csv.")
    if not args.search_query:
        parser.error("the following arguments are required: search_query")

    # Build the SQL query
    sql_query = build_sql_query(args)

    # Prepare global qdb-cli options (passthrough + script's --info)
    qdb_global_options = []
    if args.info:
        qdb_global_options.append("--info")
    if passthrough_cli_args:
        qdb_global_options.extend(passthrough_cli_args)

    # Determine subcommand and its specific arguments
    subcmd_to_run: str
    qdb_subcmd_specific_args: List[str]

    if args.csv_output:
        subcmd_to_run = "exp"
        # 'exp' takes query directly
        qdb_subcmd_specific_args = [sql_query]
        if args.no_header_csv:
            qdb_subcmd_specific_args.append("--nm")
    else:
        subcmd_to_run = "exec"
        # 'exec' uses -q and default to --psql for table output
        qdb_subcmd_specific_args = ["-q", sql_query, "--psql"]

    # Run the command
    run_qdb_cli(
        global_qdb_options=qdb_global_options,
        subcmd_specific_args=qdb_subcmd_specific_args,
        dry_run=args.dry_run,
        script_info_flag=args.info,  # Pass the script's info flag status
        subcommand=subcmd_to_run,
    )


if __name__ == "__main__":
    main()
