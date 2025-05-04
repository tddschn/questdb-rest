#!/usr/bin/env python3
# qdb-gen-random-data
# ---
# Generates random data in QuestDB using its rnd_* functions via qdb-cli.
# ---
import argparse
import subprocess
import sys
import json
import shlex
from typing import Dict, List, Tuple, Optional

# --- Configuration: Mapping CLI types to QuestDB functions and types ---
# Format: 'cli_type_name': (rnd_function_call, sql_data_type)
# Added default arguments for functions requiring them for basic usage.
TYPE_MAPPING: Dict[str, Tuple[str, str]] = {
    "boolean": ("rnd_boolean()", "BOOLEAN"),
    "byte": ("rnd_byte()", "BYTE"),
    "short": ("rnd_short()", "SHORT"),
    "int": ("rnd_int()", "INT"),
    "long": ("rnd_long()", "LONG"),
    "float": ("rnd_float()", "FLOAT"),
    "double": ("rnd_double()", "DOUBLE"),
    "char": ("rnd_char()", "CHAR"),
    "string": ("rnd_str(5, 10, 0)", "STRING"), # Example: 5-10 chars, no nulls
    "symbol": ("rnd_symbol(4, 1, 5, 0)", "SYMBOL"), # Example: 4 distinct symbols, 1-5 chars, no nulls
    "varchar": ("rnd_varchar(5, 10, 0)", "VARCHAR"), # Example: 5-10 chars, no nulls
    "date": ("rnd_date(to_date('2020-01-01', 'yyyy-MM-dd'), now(), 0)", "DATE"), # Example: since 2020, no nulls
    "timestamp": ("rnd_timestamp(to_timestamp('2020-01-01T00:00:00.000Z', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ'), now(), 0)", "TIMESTAMP"), # Example: since 2020, no nulls
    "long256": ("rnd_long256()", "LONG256"),
    "uuid": ("rnd_uuid4()", "UUID"),
    "ipv4": ("rnd_ipv4()", "IPV4"),
    "binary": ("rnd_bin(4, 16, 0)", "BINARY"), # Example: 4-16 bytes, no nulls
}

DEFAULT_TYPES = ["float"]

# --- Helper Functions ---

def run_qdb_cli(cmd_args: List[str], check: bool = True) -> subprocess.CompletedProcess:
    """Runs a qdb-cli command and returns the result."""
    try:
        # Ensure 'qdb-cli' is the first element
        if not cmd_args or cmd_args[0] != "qdb-cli":
            cmd_args.insert(0, "qdb-cli")

        print(f"+ Running: {shlex.join(cmd_args)}", file=sys.stderr)
        # Use text=True for automatic decoding, capture output
        result = subprocess.run(
            cmd_args,
            check=check,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace' # Handle potential decoding errors
        )
        if result.stderr:
            print(f"  stderr:\n{result.stderr.strip()}", file=sys.stderr)
        return result
    except FileNotFoundError:
        print("Error: 'qdb-cli' command not found.", file=sys.stderr)
        print("Please ensure it's installed and in your PATH.", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        # Error is already printed by the check=True mechanism if stderr/stdout exists
        print(f"Error: qdb-cli command failed with exit code {e.returncode}.", file=sys.stderr)
        # If check=False was used, we might want to print details here
        # print(f"  stdout:\n{e.stdout}", file=sys.stderr)
        # print(f"  stderr:\n{e.stderr}", file=sys.stderr)
        # Re-raise or exit based on 'check' argument or desired behavior
        if check:
            sys.exit(e.returncode) # Propagate error exit code
        raise # Re-raise if check=False and caller needs to handle it
    except Exception as e:
        print(f"An unexpected error occurred running qdb-cli: {e}", file=sys.stderr)
        sys.exit(1)

def check_table_exists(table_name: str) -> bool:
    """Checks if a QuestDB table exists using `qdb-cli chk`."""
    print(f"+ Checking if table '{table_name}' exists...", file=sys.stderr)
    result = run_qdb_cli(["chk", table_name], check=False) # Don't exit script if check fails

    if result.returncode == 0:
        try:
            chk_output = json.loads(result.stdout)
            exists = chk_output.get("status") == "Exists"
            print(f"  Result: {'Exists' if exists else 'Does not exist'}", file=sys.stderr)
            return exists
        except json.JSONDecodeError:
            print(f"Warning: Could not parse JSON from 'qdb-cli chk {table_name}'. Assuming table does not exist.", file=sys.stderr)
            print(f"  Raw stdout: {result.stdout}", file=sys.stderr)
            return False
        except Exception as e:
             print(f"Warning: Error processing 'qdb-cli chk {table_name}' output: {e}. Assuming table does not exist.", file=sys.stderr)
             return False
    elif result.returncode == 3: # Specific exit code from `chk` if table does not exist
        print("  Result: Does not exist (exit code 3)", file=sys.stderr)
        return False
    else:
        # Other errors (connection, etc.)
        print(f"Warning: 'qdb-cli chk {table_name}' failed with exit code {result.returncode}. Assuming table does not exist.", file=sys.stderr)
        return False # Treat other errors as "doesn't exist" for safety

def build_select_list(types_to_generate: List[str]) -> List[str]:
    """Builds the SELECT clause parts: `rnd_func() AS col_name`."""
    select_parts = []
    for type_name in types_to_generate:
        rnd_func, _ = TYPE_MAPPING[type_name]
        col_name = f"{type_name}_val" # Define column name convention
        select_parts.append(f"{rnd_func} AS {col_name}")
    return select_parts

def build_create_statement(table_name: str, types_to_generate: List[str], timestamp_col: Optional[str], partition_by: Optional[str]) -> str:
    """Builds the CREATE TABLE statement."""
    col_defs = []
    has_timestamp_type = False
    timestamp_col_name_in_defs = None

    for type_name in types_to_generate:
        _, sql_type = TYPE_MAPPING[type_name]
        col_name = f"{type_name}_val"
        col_defs.append(f'"{col_name}" {sql_type}') # Quote column names
        if type_name == "timestamp":
            has_timestamp_type = True
            if timestamp_col_name_in_defs is None: # Store the first timestamp col name
                timestamp_col_name_in_defs = col_name

    # Validate timestamp column if provided
    effective_timestamp_col = None
    if timestamp_col:
        if timestamp_col not in [f"{t}_val" for t in types_to_generate if t == "timestamp"]:
             print(f"Error: Specified --timestamp-col '{timestamp_col}' does not match any generated timestamp column name ('timestamp_val').", file=sys.stderr)
             sys.exit(1)
        effective_timestamp_col = timestamp_col
    elif has_timestamp_type:
        # Auto-detect if only one timestamp type is present and --timestamp-col not given
        timestamp_cols = [f"{t}_val" for t in types_to_generate if t == "timestamp"]
        if len(timestamp_cols) == 1:
            effective_timestamp_col = timestamp_cols[0]
            print(f"i Auto-detected designated timestamp column: '{effective_timestamp_col}'", file=sys.stderr)
        else:
             print(f"Warning: Multiple timestamp columns generated ({', '.join(timestamp_cols)}) but --timestamp-col not specified. No designated timestamp will be set.", file=sys.stderr)


    # Escape table name for the query
    safe_table_name = table_name.replace('"', '""')
    create_sql = f'CREATE TABLE "{safe_table_name}" (\n  '
    create_sql += ",\n  ".join(col_defs)
    create_sql += "\n)"

    if effective_timestamp_col:
         create_sql += f' TIMESTAMP("{effective_timestamp_col}")' # Quote column name

    if partition_by:
        if not effective_timestamp_col:
            print("Error: --partitionBy requires a designated timestamp column (--timestamp-col or auto-detected).", file=sys.stderr)
            sys.exit(1)
        create_sql += f" PARTITION BY {partition_by.upper()}" # Partition strategy usually case-insensitive

    create_sql += ";"
    return create_sql

def build_insert_statement(table_name: str, select_list: List[str], rows: int) -> str:
    """Builds the INSERT INTO ... SELECT ... statement."""
    # Escape table name for the query
    safe_table_name = table_name.replace('"', '""')
    select_clause = ",\n    ".join(select_list)

    insert_sql = f'INSERT INTO "{safe_table_name}"\n  SELECT\n    {select_clause}\n  FROM long_sequence({rows});'
    return insert_sql

# --- Argument Parser ---
def setup_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate random data in QuestDB using rnd_* functions via qdb-cli.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "-n", "--name",
        help="Name of the target table. If provided, data will be inserted. If the table doesn't exist, it will be created first."
    )
    parser.add_argument(
        "-N", "--rows",
        type=int,
        required=True,
        help="Number of rows to generate."
    )
    parser.add_argument(
        "-t", "--types",
        nargs='*',
        default=DEFAULT_TYPES,
        choices=list(TYPE_MAPPING.keys()),
        metavar='TYPE',
        help=f"Space-separated list of data types to generate. Default: {' '.join(DEFAULT_TYPES)}. Available: {', '.join(TYPE_MAPPING.keys())}"
    )
    # Removed --create flag, creation is implicit if --name is given and table doesn't exist.
    # parser.add_argument(
    #     "-c", "--create",
    #     action='store_true',
    #     help="Create the table specified by --name if it doesn't exist (Requires --name)."
    # )
    parser.add_argument(
        "-P", "--partitionBy",
        choices=["NONE", "YEAR", "MONTH", "DAY", "HOUR", "WEEK"],
        help="Partitioning strategy (requires --name and a timestamp column)."
    )
    parser.add_argument(
        "--timestamp-col",
        help="Specify the designated timestamp column name (e.g., 'timestamp_val'). Must be one of the generated columns of type 'timestamp'. Auto-detected if only one timestamp column is generated."
    )
    parser.add_argument(
        "--dry-run",
        action='store_true',
        help="Print the qdb-cli commands that would be executed, but don't run them."
    )
    # Allow passing extra arguments directly to all `qdb-cli exec` calls
    parser.add_argument(
        'qdb_cli_args',
        nargs=argparse.REMAINDER,
        help="Remaining arguments are passed directly to `qdb-cli exec` (e.g., --host, --port, auth details)."
    )

    return parser

# --- Main Execution ---
def main():
    parser = setup_arg_parser()
    args = parser.parse_args()

    if args.rows <= 0:
        print("Error: --rows must be a positive integer.", file=sys.stderr)
        sys.exit(1)

    # Validate selected types
    invalid_types = [t for t in args.types if t not in TYPE_MAPPING]
    if invalid_types:
        print(f"Error: Invalid type(s) specified: {', '.join(invalid_types)}", file=sys.stderr)
        print(f"Available types: {', '.join(TYPE_MAPPING.keys())}", file=sys.stderr)
        sys.exit(1)

    # Ensure unique types
    types_to_generate = sorted(list(set(args.types)))
    if not types_to_generate:
        print("Error: No data types selected to generate.", file=sys.stderr)
        sys.exit(1)


    # --- Build SQL components ---
    select_list = build_select_list(types_to_generate)

    # --- Execution Logic ---
    qdb_exec_base_cmd = ["exec"] + args.qdb_cli_args # Base command + extra user args

    if not args.name:
        # --- Mode: Print data to stdout ---
        print("i No table name specified (--name). Printing generated data to stdout.", file=sys.stderr)
        select_query = f"SELECT\n  {', '.join(select_list)}\nFROM long_sequence({args.rows});"
        qdb_cmd = qdb_exec_base_cmd + ["--psql", "-q", select_query] # Use psql format for printing

        if args.dry_run:
            print("\n--- Dry Run: Command to print data ---")
            print(shlex.join(["qdb-cli"] + qdb_cmd))
        else:
            result = run_qdb_cli(qdb_cmd)
            print(result.stdout, end='') # Print query result to stdout
            print("\nData printed successfully.", file=sys.stderr)

    else:
        # --- Mode: Create/Insert into table ---
        table_name = args.name
        print(f"i Target table: '{table_name}'", file=sys.stderr)

        # 1. Check if table exists (unless dry run)
        exists = False
        if not args.dry_run:
            exists = check_table_exists(table_name)

        # 2. Create table if it doesn't exist
        create_cmd_str = None
        if not exists:
            print(f"i Table '{table_name}' does not exist. Will attempt to create it.", file=sys.stderr)
            try:
                create_sql = build_create_statement(
                    table_name,
                    types_to_generate,
                    args.timestamp_col,
                    args.partitionBy
                )
                create_cmd_str = shlex.join(["qdb-cli"] + qdb_exec_base_cmd + ["-q", create_sql])

                if args.dry_run:
                    print("\n--- Dry Run: Command to CREATE table ---")
                    print(create_cmd_str)
                else:
                    print(f"Creating table '{table_name}'...", file=sys.stderr)
                    run_qdb_cli(qdb_exec_base_cmd + ["-q", create_sql]) # Raises on error
                    print(f"Table '{table_name}' created successfully.", file=sys.stderr)

            except SystemExit: # Catch sys.exit from build_create_statement validation
                sys.exit(1) # Propagate exit
            except Exception as e:
                print(f"\nError building CREATE statement: {e}", file=sys.stderr)
                sys.exit(1)
        elif not args.dry_run:
             print(f"i Table '{table_name}' already exists. Proceeding to insert data.", file=sys.stderr)


        # 3. Insert data
        insert_cmd_str = None
        try:
            insert_sql = build_insert_statement(table_name, select_list, args.rows)
            insert_cmd_str = shlex.join(["qdb-cli"] + qdb_exec_base_cmd + ["-q", insert_sql])

            if args.dry_run:
                 print("\n--- Dry Run: Command to INSERT data ---")
                 print(insert_cmd_str)
                 # Also print the create command if it would have run
                 if not exists and create_cmd_str:
                      print("\n(Table would have been created first if not dry run)")
            else:
                print(f"Inserting {args.rows} rows into '{table_name}'...", file=sys.stderr)
                run_qdb_cli(qdb_exec_base_cmd + ["-q", insert_sql]) # Raises on error
                print(f"Data inserted successfully into '{table_name}'.", file=sys.stderr)

        except Exception as e:
            print(f"\nError building/executing INSERT statement: {e}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    main()