#!/usr/bin/env python3
# questdb_rest/qdb_canned_queries_pypika.py

import argparse
import subprocess
import sys
from typing import List, Optional

# Attempt to import pypika, provide guidance if missing
try:
    # Use LiteralValue for direct QuestDB syntax where needed
    from pypika import Query, Table, Field
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
        description="Run a canned QuestDB query using pypika, reading table names from args or stdin.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""Examples:
  # Count rows in a specific table
  %(prog)s my_table

  # Get distinct values for 'colA' in 'my_table'
  %(prog)s --distinct my_table colA

  # Get distinct values and their counts for 'colA' in 'my_table'
  %(prog)s --count-distinct my_table colA

  # Count rows in multiple tables read from stdin
  qdb-tables trades_ | %(prog)s

  # Get distinct 'symbol' values for multiple tables from stdin
  qdb-tables trades_ | %(prog)s --distinct - symbol

  # Dry run distinct count for tables from stdin
  qdb-tables trades_ | %(prog)s -n --count-distinct - symbol
""",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "-d",
        "--distinct",
        action="store_true",
        help="Distinct mode: SELECT distinct(col) FROM table.",
    )
    mode_group.add_argument(
        "-c",
        "--count-distinct",
        action="store_true",
        help="Distinct count mode: SELECT distinct(col), count() FROM table.",
    )
    # Default mode is 'count' if neither --distinct nor --count-distinct is specified.

    parser.add_argument(
        "table_name",
        nargs="?",
        help="""The name of the QuestDB table. If omitted or '-',
        table names will be read from standard input (one per line).""",
    )
    parser.add_argument(
        "col_name",
        nargs="?",
        help="""The name of the column. Required for --distinct and
        --count-distinct modes.""",
    )

    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Show the command that would be run, but do not execute it.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show the command before running it.",
    )

    return parser


# --- SQL Query Building ---
def build_sql_query(mode: str, table_name: str, col_name: Optional[str] = None) -> str:
    """Builds the SQL query using pypika, ensuring identifiers are quoted."""

    # Use pypika's Table to handle quoting of the table name automatically
    # Pypika usually uses double quotes by default which works for QuestDB
    table = Table(table_name)

    if mode == "count":
        # QuestDB uses count() without args
        # Use LiteralValue for direct SQL function representation
        query = Query.from_(table).select(LiteralValue("count()"))
    elif mode == "distinct":
        if not col_name:
            raise ValueError("Column name is required for distinct mode.")
        # Use LiteralValue for QuestDB's distinct(col) syntax
        # Ensure the column name within the LiteralValue is also quoted
        quoted_col = Field(col_name).get_sql(
            quote_char='"'
        )  # Get pypika's quoted version
        query = Query.from_(table).select(LiteralValue(f"distinct({quoted_col})"))

    elif mode == "distinct_count":
        if not col_name:
            raise ValueError("Column name is required for distinct count mode.")
        # Use LiteralValue for both distinct(col) and count()
        quoted_col = Field(col_name).get_sql(
            quote_char='"'
        )  # Get pypika's quoted version
        query = Query.from_(table).select(
            LiteralValue(f"distinct({quoted_col})"), LiteralValue("count()")
        )
    else:
        raise ValueError(f"Invalid mode: {mode}")

    # Add semicolon for QuestDB
    return query.get_sql(quote_char='"') + ";"


# --- Build the qdb-cli command list ---
def build_cli_command(
    mode: str, sql_query: str, col_name: Optional[str] = None
) -> List[str]:
    """Builds the qdb-cli command arguments list."""
    cmd = ["qdb-cli", "exec", "-q", sql_query]

    if mode == "count":
        # Extract the 'count' column directly
        cmd.extend(["-x", "count"])
    elif mode == "distinct":
        if not col_name:
            # This should be caught earlier, but double-check
            raise ValueError("Column name needed for distinct mode CLI command.")
        # Extract the specified distinct column. The column name in the result
        # might just be 'distinct', depending on QuestDB version.
        # Let's assume the result column is named after the input column for extraction.
        # If QuestDB names it 'distinct', change "-x", col_name to "-x", "distinct".
        # Testing shows QuestDB 7.x names the column after the input column.
        cmd.extend(["-x", col_name])
    elif mode == "distinct_count":
        # Use psql format for the two-column output
        cmd.append("--psql")

    return cmd


# --- Run the command ---
def run_command(command: List[str], dry_run: bool, verbose: bool):
    """Executes the command list using subprocess."""
    cmd_str = " ".join(
        # Basic quoting for display
        f"'{arg}'" if " " in arg else arg
        for arg in command
    )

    if dry_run:
        print(f"Dry run: {cmd_str}")
        return
    if verbose:
        print(f"Running: {cmd_str}", file=sys.stderr)

    try:
        # Set encoding for reliable text processing
        # Handle potential decoding errors
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        # Print stdout (the actual results)
        print(result.stdout, end="")
        # Print stderr (logs, warnings from qdb-cli) to stderr
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {cmd_str}", file=sys.stderr)
        print(f"Return Code: {e.returncode}", file=sys.stderr)
        if e.stdout:
            print(f"--- qdb-cli stdout ---\n{e.stdout}", file=sys.stderr)
        if e.stderr:
            print(f"--- qdb-cli stderr ---\n{e.stderr}", file=sys.stderr)
        # Exit with the same code as the failed command
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

    # Determine mode
    mode = "count"  # Default
    if args.distinct:
        mode = "distinct"
    elif args.count_distinct:
        mode = "distinct_count"

    # Validate required column name for specific modes
    if mode in ["distinct", "distinct_count"] and not args.col_name:
        parser.error(
            f"Argument 'col_name' is required for --{mode.replace('_', '-')} mode."
        )

    # Determine table names source
    table_names: List[str] = []
    read_from_stdin = False

    if args.table_name is None or args.table_name == "-":
        if sys.stdin.isatty():
            parser.error(
                "Table name not provided and no data piped via stdin."
                " Provide a table name or pipe names from stdin."
            )
        else:
            read_from_stdin = True
            print("Reading table names from standard input...", file=sys.stderr)
            table_names = [line.strip() for line in sys.stdin if line.strip()]
            if not table_names:
                print("Warning: Standard input was empty.", file=sys.stderr)
                sys.exit(0)
    else:
        table_names = [args.table_name]

    # Process each table
    print(f"Running mode '{mode}' for {len(table_names)} table(s)...", file=sys.stderr)
    if mode != "count":
        print(f"Using column: '{args.col_name}'", file=sys.stderr)

    for i, table in enumerate(table_names):
        if read_from_stdin and len(table_names) > 1:
            # Add separator for visual clarity when processing multiple tables from stdin
            if i > 0:
                print("\n---\n", file=sys.stderr)
            print(
                f"Processing table {i + 1}/{len(table_names)}: '{table}'",
                file=sys.stderr,
            )

        try:
            sql = build_sql_query(mode, table, args.col_name)
            cli_cmd = build_cli_command(mode, sql, args.col_name)
            run_command(cli_cmd, args.dry_run, args.verbose)
        except ValueError as e:
            print(f"Error processing table '{table}': {e}", file=sys.stderr)
            # Decide whether to continue or stop on error for multiple tables
            # For now, let's stop on configuration errors (like missing col name)
            sys.exit(1)
        except Exception as e:
            print(
                f"Unexpected error during processing for table '{table}': {e}",
                file=sys.stderr,
            )
            # Continue to next table if possible, or exit
            # Let run_command handle subprocess errors and exit codes
            pass  # Allow loop to continue if run_command didn't exit


if __name__ == "__main__":
    main()
