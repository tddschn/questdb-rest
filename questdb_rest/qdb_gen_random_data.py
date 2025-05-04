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
# Example: 5-10 chars, no nulls
# Example: 4 distinct symbols, 1-5 chars, no nulls
# Example: 5-10 chars, no nulls
# Example: since 2020, no nulls
# Example: since 2020, no nulls
# Example: 4-16 bytes, no nulls
# qdb-gen-random-data.py
# Format: 'cli_type_name': (rnd_function_call, sql_data_type)
# Updated date and timestamp to use fixed end values instead of now()
# Kept binary as is, display issue might be separate.
# qdb-gen-random-data.py
# Updated timestamp to use literal strings directly, hoping QuestDB parses them correctly as constants.
# Simplified date literals
# Use literal timestamp strings directly
# Kept as is, display might be CLI issue
# "geohash": ("rnd_geohash(30)", "GEOHASH"),
TYPE_MAPPING: Dict[str, Tuple[str, str]] = {
    "boolean": ("rnd_boolean()", "BOOLEAN"),
    "byte": ("rnd_byte()", "BYTE"),
    "short": ("rnd_short()", "SHORT"),
    "int": ("rnd_int()", "INT"),
    "long": ("rnd_long()", "LONG"),
    "float": ("rnd_float()", "FLOAT"),
    "double": ("rnd_double()", "DOUBLE"),
    "char": ("rnd_char()", "CHAR"),
    "string": ("rnd_str(5, 10, 0)", "STRING"),
    "symbol": ("rnd_symbol(4, 1, 5, 0)", "SYMBOL"),
    "varchar": ("rnd_varchar(5, 10, 0)", "VARCHAR"),
    "date": ("rnd_date('2020-01-01', '2024-12-31', 0)", "DATE"),
    "timestamp": (
        "rnd_timestamp('2020-01-01T00:00:00.000000Z', '2024-12-31T23:59:59.999999Z', 0)",
        "TIMESTAMP",
    ),
    "long256": ("rnd_long256()", "LONG256"),
    "uuid": ("rnd_uuid4()", "UUID"),
    "ipv4": ("rnd_ipv4()", "IPV4"),
    "binary": ("rnd_bin()", "BINARY"),
}
DEFAULT_TYPES = ["float"]
# --- Helper Functions ---


def run_qdb_cli(
    cmd_args: List[str], check: bool = True, info: bool = False
) -> subprocess.CompletedProcess:
    """Runs a qdb-cli command and returns the result."""
    try:
        # Ensure 'qdb-cli' is the first element
        full_cmd = ["qdb-cli"]
        # Prepend --info if requested
        if info:
            full_cmd.append("--info")
        # Append the actual command and its arguments
        full_cmd.extend(cmd_args)
        # Only print if --info is passed to this script
        if info:
            print(f"+ Running: {shlex.join(full_cmd)}", file=sys.stderr)
        # Use text=True for automatic decoding, capture output
        # Handle potential decoding errors
        result = subprocess.run(
            full_cmd,
            check=check,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        # Only print stderr if --info is passed
        if info and result.stderr:
            print(f"  stderr:\n{result.stderr.strip()}", file=sys.stderr)
        return result
    except FileNotFoundError:
        print("Error: 'qdb-cli' command not found.", file=sys.stderr)
        print("Please ensure it's installed and in your PATH.", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        # Error details are usually printed to stderr by qdb-cli itself
        print(
            f"Error: qdb-cli command failed with exit code {e.returncode}.",
            file=sys.stderr,
        )
        # Print captured output only if info flag wasn't set (as it would be redundant)
        if not info:
            if e.stdout:
                print(f"  qdb-cli stdout:\n{e.stdout.strip()}", file=sys.stderr)
            if e.stderr:
                print(f"  qdb-cli stderr:\n{e.stderr.strip()}", file=sys.stderr)
        if check:
            sys.exit(e.returncode)  # Propagate error exit code
        raise  # Re-raise if check=False and caller needs to handle it
    except Exception as e:
        print(f"An unexpected error occurred running qdb-cli: {e}", file=sys.stderr)
        sys.exit(1)


def build_select_list(types_to_generate: List[str], repeat: int) -> List[str]:
    """Builds the SELECT clause parts: `rnd_func() AS col_name[_N]`."""
    select_parts = []
    for type_name in types_to_generate:
        rnd_func, _ = TYPE_MAPPING[type_name]
        if repeat == 1:
            col_name = f"{type_name}_val"  # Original naming convention
            select_parts.append(f"{rnd_func} AS {col_name}")
        else:
            for i in range(1, repeat + 1):
                col_name = f"{type_name}_val_{i}"  # Append suffix for repeats > 1
                select_parts.append(f"{rnd_func} AS {col_name}")
    return select_parts


def build_create_statement(
    table_name: str,
    types_to_generate: List[str],
    repeat: int,
    timestamp_col: Optional[str],
    partition_by: Optional[str],
    info: bool = False,
) -> str:
    """Builds the CREATE TABLE statement, handling repeated columns."""
    col_defs = []
    generated_ts_cols = []  # Store names of all generated timestamp columns
    for type_name in types_to_generate:
        _, sql_type = TYPE_MAPPING[type_name]
        if repeat == 1:
            col_name = f"{type_name}_val"
            col_defs.append(f'"{col_name}" {sql_type}')
            if type_name == "timestamp":
                generated_ts_cols.append(col_name)
        else:
            for i in range(1, repeat + 1):
                col_name = f"{type_name}_val_{i}"
                col_defs.append(f'"{col_name}" {sql_type}')
                if type_name == "timestamp":
                    generated_ts_cols.append(col_name)
    # Validate and determine the designated timestamp column
    effective_timestamp_col = None
    if timestamp_col:
        # Check if the specified column is among the generated timestamp columns
        if timestamp_col not in generated_ts_cols:
            print(
                f"Error: Specified --timestamp-col '{timestamp_col}' does not match any generated timestamp column name ({', '.join(generated_ts_cols) or 'none'}).",
                file=sys.stderr,
            )
            sys.exit(1)
        effective_timestamp_col = timestamp_col
    elif len(generated_ts_cols) == 1:
        # Auto-detect if exactly one timestamp column was generated overall
        effective_timestamp_col = generated_ts_cols[0]
        if info:
            print(
                f"+ Auto-detected designated timestamp column: '{effective_timestamp_col}'",
                file=sys.stderr,
            )
    elif len(generated_ts_cols) > 1:
        # Always warn if multiple timestamps and none specified
        print(
            f"Warning: Multiple timestamp columns generated ({', '.join(generated_ts_cols)}) but --timestamp-col not specified. No designated timestamp will be set.",
            file=sys.stderr,
        )
    # Escape table name for the query
    safe_table_name = table_name.replace('"', '""')
    create_sql = f'CREATE TABLE "{safe_table_name}" (\n  '
    create_sql += ",\n  ".join(col_defs)
    create_sql += "\n)"
    if effective_timestamp_col:
        # Ensure the column name is quoted if it contains special characters or needs case sensitivity
        safe_ts_col_name = effective_timestamp_col.replace('"', '""')
        create_sql += f' TIMESTAMP("{safe_ts_col_name}")'
    if partition_by:
        if not effective_timestamp_col:
            print(
                "Error: --partitionBy requires a designated timestamp column (--timestamp-col or auto-detected).",
                file=sys.stderr,
            )
            sys.exit(1)
        # Partition strategy usually case-insensitive, but keep as provided
        create_sql += f" PARTITION BY {partition_by.upper()}"
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
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-n",
        "--name",
        help="Name of the target table. If provided, data will be inserted. If the table doesn't exist, it will be created first.",
    )
    parser.add_argument(
        "-N", "--rows", type=int, default=10, help="Number of rows to generate."
    )
    # Group for type selection
    type_group = (
        parser.add_mutually_exclusive_group()
    )  # Keep default for when -A is not used
    type_group.add_argument(
        "-t",
        "--types",
        nargs="*",
        default=DEFAULT_TYPES,
        choices=list(TYPE_MAPPING.keys()),
        metavar="TYPE",
        help=f"Space-separated list of data types to generate. Default: {' '.join(DEFAULT_TYPES)}. Available: {', '.join(TYPE_MAPPING.keys())}",
    )
    type_group.add_argument(
        "-A",
        "--all-types",
        action="store_true",
        help="Generate columns for all available types, ignoring --types.",
    )
    # New argument for repeating types
    parser.add_argument(
        "-r",
        "--repeat",
        type=int,
        default=1,
        metavar="COUNT",
        help="Repeat each specified type this many times (default: 1).",
    )
    parser.add_argument(
        "-P",
        "--partitionBy",
        choices=["NONE", "YEAR", "MONTH", "DAY", "HOUR", "WEEK"],
        help="Partitioning strategy (requires --name and a designated timestamp column).",
    )
    parser.add_argument(
        "--timestamp-col",
        help="Specify the designated timestamp column name (e.g., 'timestamp_val' or 'timestamp_val_1'). Must be one of the generated columns of type 'timestamp'. Auto-detected if only one timestamp column is generated overall.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the qdb-cli commands that would be executed, but don't run them.",
    )
    parser.add_argument(
        "-i",
        "--info",
        action="store_true",
        help="Pass the --info flag to underlying qdb-cli calls for verbose logging.",
    )
    # New CSV flag
    parser.add_argument(
        "-c",
        "--csv",
        action="store_true",
        help="Output data as CSV using 'qdb-cli exp' instead of '--psql' table format (only applicable when --name is not used).",
    )
    parser.add_argument(
        "qdb_cli_args",
        nargs=argparse.REMAINDER,
        help="Remaining arguments are passed directly to `qdb-cli` (e.g., --host, --port, auth details). Place these *after* script-specific options.",
    )
    return parser


# --- Main Execution ---
from questdb_rest import (
    QuestDBClient,
    QuestDBError,
    QuestDBConnectionError,
    QuestDBAPIError,
)
import os


def main():
    parser = setup_arg_parser()
    args = parser.parse_args()
    if args.rows <= 0:
        print("Error: --rows must be a positive integer.", file=sys.stderr)
        sys.exit(1)
    # Validate repeat argument
    if args.repeat <= 0:
        print("Error: --repeat must be a positive integer.", file=sys.stderr)
        sys.exit(1)
    elif args.repeat > 1 and args.info:
        print(
            f"i Each specified type will be repeated {args.repeat} times.",
            file=sys.stderr,
        )
    # Determine types to generate
    if args.all_types:
        types_to_generate = sorted(list(TYPE_MAPPING.keys()))
        if args.info:
            print(
                f"i --all-types specified. Generating all types: {', '.join(types_to_generate)}",
                file=sys.stderr,
            )
    else:
        # Validate selected types if --all-types is not used
        invalid_types = [t for t in args.types if t not in TYPE_MAPPING]
        if invalid_types:
            print(
                f"Error: Invalid type(s) specified: {', '.join(invalid_types)}",
                file=sys.stderr,
            )
            print(f"Available types: {', '.join(TYPE_MAPPING.keys())}", file=sys.stderr)
            sys.exit(1)
        # Ensure unique types
        types_to_generate = sorted(list(set(args.types)))
        if args.info:
            print(
                f"i Generating specified types: {', '.join(types_to_generate)}",
                file=sys.stderr,
            )
    if not types_to_generate:
        # This condition should theoretically not be met if default types exist
        # or if --all-types is used, but keep as a safeguard.
        print("Error: No data types selected to generate.", file=sys.stderr)
        sys.exit(1)
    # --- Separate qdb-cli connection args from exec args ---
    qdb_client_args_dict = {}
    qdb_exec_or_exp_extra_args = []  # Renamed to reflect potential exp usage
    remainder = args.qdb_cli_args
    # Simple parsing for known connection args
    i = 0
    while i < len(remainder):
        arg = remainder[i]
        # Check for args that take a value
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
            if i + 1 < len(remainder):
                val = remainder[i + 1]
                # Map CLI arg to QuestDBClient constructor arg name
                if arg in ("-H", "--host"):
                    qdb_client_args_dict["host"] = val
                elif arg == "--port":
                    qdb_client_args_dict["port"] = int(val)  # Client expects int
                elif arg in ("-u", "--user"):
                    qdb_client_args_dict["user"] = val
                elif arg in ("-p", "--password"):
                    qdb_client_args_dict["password"] = val
                elif arg == "--timeout":
                    qdb_client_args_dict["timeout"] = int(val)  # Client expects int
                elif arg == "--scheme":
                    qdb_client_args_dict["scheme"] = val
                elif arg == "--config":
                    qdb_client_args_dict["config_path"] = val  # Client uses config_path
                i += 1  # Skip next arg (the value)
            else:
                # Option requires value but none provided, pass it to exec/exp
                qdb_exec_or_exp_extra_args.append(arg)
        # Check for flags
        # Flags -i/-D (info/debug for qdb-cli itself) are handled by run_qdb_cli
        # Flag -R (dry-run for qdb-cli itself) is handled by script's --dry-run
        # BooleanOptionalAction --stop-on-error / --no-stop-on-error
        elif arg in ("--stop-on-error", "--no-stop-on-error"):
            qdb_exec_or_exp_extra_args.append(arg)  # Pass to exec/exp
        elif arg.startswith("-"):  # Treat other flags as exec/exp args
            qdb_exec_or_exp_extra_args.append(arg)
        else:  # Treat non-flags as exec/exp args (shouldn't happen with REMAINDER but be safe)
            qdb_exec_or_exp_extra_args.append(arg)
        i += 1
    # --- Build SQL components ---
    # Pass repeat argument here
    select_list = build_select_list(types_to_generate, args.repeat)
    # --- Instantiate Client (only if needed and not dry run) ---
    client: Optional[QuestDBClient] = None
    # Client is needed if inserting (--name) OR if checking existence (also --name)
    if args.name and (not args.dry_run):
        try:
            if "config_path" in qdb_client_args_dict:
                config_path = qdb_client_args_dict.pop("config_path")
                client = QuestDBClient.from_config_file(config_path)
                # Override loaded config with specific CLI connection args
                if "host" in qdb_client_args_dict:
                    # Basic host replace in base_url
                    base_host = client.base_url.split("://")[1].split(":")[0]
                    client.base_url = client.base_url.replace(
                        base_host, qdb_client_args_dict["host"], 1
                    )
                if "port" in qdb_client_args_dict:
                    # Basic port replace in base_url
                    port_str = f":{client.base_url.split(':')[-1].split('/')[0]}"
                    client.base_url = client.base_url.replace(
                        port_str, f":{qdb_client_args_dict['port']}", 1
                    )
                if "user" in qdb_client_args_dict or "password" in qdb_client_args_dict:
                    # Update auth tuple, preserving parts not overridden
                    base_user = client.auth[0] if client.auth else None
                    base_pass = client.auth[1] if client.auth else None
                    final_user = qdb_client_args_dict.get("user", base_user)
                    final_pass = qdb_client_args_dict.get("password", base_pass)
                    client.auth = (final_user, final_pass) if final_user else None
                if "timeout" in qdb_client_args_dict:
                    client.timeout = qdb_client_args_dict["timeout"]
                if "scheme" in qdb_client_args_dict:
                    # Replace scheme in base_url
                    base_scheme = client.base_url.split("://")[0]
                    client.base_url = client.base_url.replace(
                        f"{base_scheme}://", f"{qdb_client_args_dict['scheme']}://", 1
                    )
            else:
                # Standard init: uses explicit args > default config > defaults
                client = QuestDBClient(**qdb_client_args_dict)
            # Log connection details if info requested
            if args.info and client:
                log_host = client.base_url.split("://")[1].split(":")[0]
                port_part = client.base_url.split(":")[-1]
                log_port = (
                    int(port_part.split("/")[0])
                    if port_part.split("/")[0].isdigit()
                    else QuestDBClient.DEFAULT_PORT
                )
                log_scheme = client.base_url.split("://")[0]
                log_user_info = f" as user '{client.auth[0]}'" if client.auth else ""
                print(
                    f"i Client connected to {log_scheme}://{log_host}:{log_port}{log_user_info}",
                    file=sys.stderr,
                )
        except (QuestDBError, ValueError, TypeError) as e:
            print(f"Error: Failed to initialize QuestDB client: {e}", file=sys.stderr)
            sys.exit(1)
        except FileNotFoundError as e:
            print(
                f"Error: Config file specified via --config not found: {e}",
                file=sys.stderr,
            )
            sys.exit(1)
        except Exception as e:  # Catch other potential init errors
            print(
                f"Error: An unexpected error occurred during client initialization: {e}",
                file=sys.stderr,
            )
            sys.exit(1)
    # --- Execution Logic ---
    if not args.name:
        # --- Mode: Print data to stdout ---
        output_format = "CSV" if args.csv else "PSQL table"
        if args.info:
            print(
                f"i No table name specified (--name). Printing generated data to stdout as {output_format}.",
                file=sys.stderr,
            )
        select_query = (
            f"SELECT\n  {', '.join(select_list)}\nFROM long_sequence({args.rows});"
        )
        # Base command for exec/exp, including any *non-connection* args passed
        qdb_cmd_base = []
        if args.csv:
            qdb_cmd_base = ["exp"] + qdb_exec_or_exp_extra_args + [select_query]
            # Construct full command for dry run display
            dry_run_cmd_name = "exp"
            dry_run_query_arg = [select_query]
            dry_run_format_arg = []
        else:
            qdb_cmd_base = (
                ["exec"] + qdb_exec_or_exp_extra_args + ["-q", select_query, "--psql"]
            )
            # Construct full command for dry run display
            dry_run_cmd_name = "exec"
            dry_run_query_arg = ["-q", select_query]
            dry_run_format_arg = ["--psql"]
        if args.dry_run:
            print(f"\n--- Dry Run: Command to print data ({output_format}) ---")
            dry_run_full_cmd = ["qdb-cli"]
            if args.info:
                dry_run_full_cmd.append("--info")
            # Include original connection args for dry-run clarity
            dry_run_full_cmd.extend(args.qdb_cli_args)
            # Use the actual command part built for exec/exp
            dry_run_full_cmd.extend(
                [dry_run_cmd_name]
                + qdb_exec_or_exp_extra_args
                + dry_run_query_arg
                + dry_run_format_arg
            )
            print(shlex.join(dry_run_full_cmd))
        else:
            result = run_qdb_cli(
                qdb_cmd_base, info=args.info
            )  # Pass script's info flag
            print(result.stdout, end="")
            if args.info:
                print(
                    f"\nData printed successfully ({output_format}).", file=sys.stderr
                )
    else:
        # --- Mode: Create/Insert into table ---
        table_name = args.name
        if args.csv:
            print(
                "Warning: --csv flag is ignored when --name is specified (inserting data).",
                file=sys.stderr,
            )
        if args.info:
            print(f"i Target table: '{table_name}'", file=sys.stderr)
        # Base command for exec only, including any *non-connection* args passed
        qdb_exec_base_cmd = ["exec"] + qdb_exec_or_exp_extra_args
        # 1. Check if table exists (unless dry run)
        exists = False
        if not args.dry_run:
            if not client:  # Should not happen if name is set, but check
                print(
                    "Error: Client not initialized, cannot check table existence.",
                    file=sys.stderr,
                )
                sys.exit(1)
            if args.info:
                print(
                    f"+ Checking if table '{table_name}' exists using client...",
                    file=sys.stderr,
                )
            try:
                exists = client.table_exists(table_name)
                if args.info:
                    print(
                        f"  Result: {('Exists' if exists else 'Does not exist')}",
                        file=sys.stderr,
                    )
            except QuestDBError as e:
                print(
                    f"Warning: Failed to check table existence for '{table_name}': {e}. Assuming it does not exist.",
                    file=sys.stderr,
                )
                exists = False  # Proceed assuming not exists on error
        # 2. Create table if it doesn't exist
        create_cmd_str = None
        if not exists:
            if args.info:
                print(
                    f"i Table '{table_name}' does not exist. Will attempt to create it.",
                    file=sys.stderr,
                )
            try:  # Pass info flag here too
                # Pass repeat argument here
                create_sql = build_create_statement(
                    table_name,
                    types_to_generate,
                    args.repeat,
                    args.timestamp_col,
                    args.partitionBy,
                    info=args.info,
                )
                create_cmd_args = qdb_exec_base_cmd + ["-q", create_sql]
                # Construct the full command for dry run display
                create_full_cmd_dry_run = ["qdb-cli"]
                if args.info:
                    create_full_cmd_dry_run.append("--info")
                # Include original connection args
                create_full_cmd_dry_run.extend(args.qdb_cli_args)
                # Add the actual command part (exec only for create)
                create_full_cmd_dry_run.extend(
                    ["exec"] + qdb_exec_or_exp_extra_args + ["-q", create_sql]
                )
                create_cmd_str = shlex.join(create_full_cmd_dry_run)
                if args.dry_run:
                    print("\n--- Dry Run: Command to CREATE table ---")
                    print(create_cmd_str)
                else:
                    if args.info:
                        print(f"Creating table '{table_name}'...", file=sys.stderr)
                    run_qdb_cli(
                        create_cmd_args, info=args.info
                    )  # Pass script's info flag
                    if args.info:
                        print(
                            f"Table '{table_name}' created successfully.",
                            file=sys.stderr,
                        )
            except SystemExit:
                # Propagate exit from build_create_statement validation
                raise
            except Exception as e:
                print(
                    f"\nError building/executing CREATE statement: {e}", file=sys.stderr
                )
                sys.exit(1)
        elif not args.dry_run:
            if args.info:
                print(
                    f"i Table '{table_name}' already exists. Proceeding to insert data.",
                    file=sys.stderr,
                )
        # 3. Insert data
        insert_cmd_str = None
        try:
            insert_sql = build_insert_statement(table_name, select_list, args.rows)
            insert_cmd_args = qdb_exec_base_cmd + ["-q", insert_sql]
            # Construct the full command for dry run display
            insert_full_cmd_dry_run = ["qdb-cli"]
            if args.info:
                insert_full_cmd_dry_run.append("--info")
            # Include original connection args
            insert_full_cmd_dry_run.extend(args.qdb_cli_args)
            # Add the actual command part (exec only for insert)
            insert_full_cmd_dry_run.extend(
                ["exec"] + qdb_exec_or_exp_extra_args + ["-q", insert_sql]
            )
            insert_cmd_str = shlex.join(insert_full_cmd_dry_run)
            if args.dry_run:
                print("\n--- Dry Run: Command to INSERT data ---")
                print(insert_cmd_str)
                # Show create command again if it would have run
                if not exists and create_cmd_str:
                    print("\n(Table creation command shown above)")
            else:
                if args.info:
                    print(
                        f"Inserting {args.rows} rows into '{table_name}'...",
                        file=sys.stderr,
                    )
                run_qdb_cli(insert_cmd_args, info=args.info)  # Pass script's info flag
                if args.info:
                    print(
                        f"Data inserted successfully into '{table_name}'.",
                        file=sys.stderr,
                    )
        except Exception as e:
            print(f"\nError building/executing INSERT statement: {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
