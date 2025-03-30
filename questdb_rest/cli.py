#!/usr/bin/env python3


#!/usr/bin/env uv run

# /// script
# requires-python = '>=3.11'
# dependencies = [
#     # requests is now a dependency of questdb_rest
#     "sqlparse",
#     "icecream",
#     "./questdb_rest.py" # Include the library file as a dependency
# ]
# ///

import argparse
import sys
import json
import logging
from getpass import getpass
from pathlib import Path
from typing import Callable, Dict, Any, Tuple
import os  # ensure os is imported

# Import the client and exceptions from the library
from questdb_rest import (
    QuestDBClient,
    QuestDBError,
    QuestDBConnectionError,
    QuestDBAPIError,
)

# --- Configuration ---
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 9000  # Use the default from the library/docs
# -------------------

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)
# ---------------------


# --- Table Name Generation Functions (Keep as is) ---
def get_table_name_from_stem(p: Path, **kwargs) -> str:
    """Default: returns the filename without extension."""
    return p.stem


def get_table_name_add_prefix(p: Path, prefix: str = "", **kwargs) -> str:
    """Returns the filename stem with a prefix added."""
    prefix_str = (
        prefix  # Removed default 'import_' here, rely on arg default or presence
    )
    return f"{prefix_str}{p.stem}"


TABLE_NAME_FUNCTIONS: Dict[str, Tuple[Callable[..., str], list[str]]] = {
    "stem": (get_table_name_from_stem, []),
    "add_prefix": (get_table_name_add_prefix, ["prefix"]),
}
# --------------------------------------


# --- SQL Statement Extraction (Keep as is) ---
def extract_statements_from_sql(sql_string: str) -> list[str]:
    """
    Parses a string containing one or more SQL statements using sqlparse.
    """
    import sqlparse  # Keep import local to this function/exec command

    if not isinstance(sql_string, str):
        raise TypeError("Input must be a string.")
    raw_statements = sqlparse.split(sql_string)
    cleaned_statements = [stmt.strip() for stmt in raw_statements if stmt.strip()]
    return cleaned_statements


# --------------------------------------

# --- Dry Run Simulation Helpers ---


def simulate_imp(args, file_path, table_name, schema_source):
    logger.info("[DRY-RUN] Simulating /imp request:")
    logger.info(f"[DRY-RUN]   File: '{file_path}'")
    logger.info(f"[DRY-RUN]   Target Table: '{table_name}'")
    if schema_source:
        logger.info(f"[DRY-RUN]   Schema Source: '{schema_source}'")
    params = {
        "name": table_name,
        "partitionBy": args.partitionBy,
        "timestamp": args.timestamp,
        "overwrite": str(args.overwrite).lower()
        if args.overwrite is not None
        else None,
        "atomicity": args.atomicity,
        "delimiter": args.delimiter,
        "forceHeader": str(args.forceHeader).lower()
        if args.forceHeader is not None
        else None,
        "skipLev": str(args.skipLev).lower() if args.skipLev is not None else None,
        "fmt": args.fmt,
        "o3MaxLag": args.o3MaxLag,
        "maxUncommittedRows": args.maxUncommittedRows,
        "create": str(args.create).lower() if args.create is not None else None,
    }
    filtered_params = {k: v for k, v in params.items() if v is not None}
    logger.info(f"[DRY-RUN]   Params: {filtered_params}")
    # Simulate successful response structure based on fmt
    if args.fmt == "json":
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "operation": "import",
                    "status": "OK (Simulated)",
                    "location": table_name,
                    "rowsRejected": 0,
                    "rowsImported": 0,
                    "header": bool(args.forceHeader)
                    if args.forceHeader is not None
                    else False,
                    "columns": [],  # Cannot simulate columns without parsing file
                },
                indent=2,
            )
        )
    else:  # tabular
        print(f"+--- [DRY-RUN] Import Simulation for {table_name} ---+")
        print(f"| Status: OK (Simulated)")
        print(f"+----------------------------------------------------+")


def simulate_exec(args, statement, statement_index, total_statements):
    logger.info(
        f"[DRY-RUN] Simulating /exec request ({statement_index}/{total_statements}):"
    )
    params = {
        "query": statement,
        "limit": args.limit,
        "count": str(args.count).lower() if args.count is not None else None,
        "nm": str(args.nm).lower() if args.nm is not None else None,
        "timings": str(args.timings).lower() if args.timings is not None else None,
        "explain": str(args.explain).lower() if args.explain is not None else None,
        "quoteLargeNum": str(args.quoteLargeNum).lower()
        if args.quoteLargeNum is not None
        else None,
    }
    filtered_params = {k: v for k, v in params.items() if v is not None}
    logger.info(f"[DRY-RUN]   Params: {filtered_params}")
    headers = {}
    if args.statement_timeout:
        headers["Statement-Timeout"] = str(args.statement_timeout)
        logger.info(f"[DRY-RUN]   Headers: {headers}")
    # Simulate a DDL OK response for simplicity
    print(json.dumps({"dry_run": True, "ddl": "OK (Simulated)"}, indent=2))


def simulate_exp(args):
    logger.info("[DRY-RUN] Simulating /exp request:")
    params = {
        "query": args.query,
        "limit": args.limit,
        "nm": str(args.nm).lower() if args.nm is not None else None,
    }
    filtered_params = {k: v for k, v in params.items() if v is not None}
    logger.info(f"[DRY-RUN]   Params: {filtered_params}")
    output_dest = args.output_file if args.output_file else "stdout"
    logger.info(f"[DRY-RUN]   Output Target: {output_dest}")
    # Simulate CSV output
    if not args.nm:  # If header is not skipped
        print('"dry_run_col1","dry_run_col2"')
    print('"simulated_val1","simulated_val2"')


def simulate_chk(args):
    logger.info("[DRY-RUN] Simulating /chk request:")
    params = {"f": "json", "j": args.table_name, "version": "2"}
    logger.info(f"[DRY-RUN]   Params: {params}")
    # Simulate 'Exists' for predictability in dry-run
    print(json.dumps({"dry_run": True, "status": "Exists (Simulated)"}, indent=2))


# --- Command Handlers (Refactored to use QuestDBClient) ---


def handle_imp(args, client: QuestDBClient):
    """Handles the /imp (import) command using the client."""
    any_file_failed = False
    num_files = len(args.files)
    json_separator = "\n"

    schema_content = None
    schema_file_obj = None
    schema_source_desc = None  # For logging

    try:
        # --- Prepare Schema (once if provided) ---
        if args.schema:
            schema_content = args.schema
            schema_source_desc = "command line string"
            logger.debug("Using schema string provided via --schema")
        elif args.schema_file:
            try:
                # Open here, pass the object to client.imp
                schema_file_obj = open(args.schema_file, "rb")
                schema_source_desc = f"file '{args.schema_file}'"
                logger.debug(f"Using schema file: {args.schema_file}")
            except IOError as e:
                logger.warning(f"Error opening schema file '{args.schema_file}': {e}")
                sys.exit(1)  # Cannot proceed if schema file is required but unreadable

        # --- Iterate Through Input Files ---
        for i, file_path_str in enumerate(args.files):
            file_path = Path(file_path_str)
            logger.info(f"--- Processing file {i + 1}/{num_files}: '{file_path}' ---")

            # --- Determine Table Name ---
            table_name = args.name
            if not table_name:
                if args.name_func:
                    name_func_choice = args.name_func
                    if name_func_choice in TABLE_NAME_FUNCTIONS:
                        func, req_args = TABLE_NAME_FUNCTIONS[name_func_choice]
                        func_kwargs = {}
                        if "prefix" in req_args:
                            func_kwargs["prefix"] = (
                                args.name_func_prefix or ""
                            )  # Pass empty string if None
                        try:
                            table_name = func(file_path, **func_kwargs)
                            logger.info(
                                f"Using name function '{name_func_choice}' -> table name: '{table_name}'"
                            )
                        except Exception as e:
                            logger.error(
                                f"Error executing name function '{name_func_choice}' for file '{file_path}': {e}"
                            )
                            any_file_failed = True
                            if args.stop_on_error:
                                sys.exit(1)
                            else:
                                continue
                    else:
                        logger.error(
                            f"Internal error: Unknown name function '{name_func_choice}'."
                        )
                        any_file_failed = True
                        if args.stop_on_error:
                            sys.exit(1)
                        else:
                            continue
                else:
                    table_name = get_table_name_from_stem(file_path)
                    logger.info(
                        f"Using default naming (file stem) -> table name: '{table_name}'"
                    )

            if not table_name:
                logger.error(
                    f"Could not determine table name for file '{file_path}'. Skipping."
                )
                any_file_failed = True
                if args.stop_on_error:
                    sys.exit(1)
                else:
                    continue

            # --- Dry Run Check ---
            if args.dry_run:
                simulate_imp(args, file_path, table_name, schema_source_desc)
                # Add separator if not the first file
                if i > 0 and args.fmt == "json":
                    sys.stdout.write(json_separator)
                continue  # Skip actual import in dry-run

            # --- Make the Request via Client ---
            data_file_obj_for_request = None
            try:
                # Open data file just before the request
                data_file_obj_for_request = open(file_path, "rb")

                logger.info(f"Importing '{file_path}' into table '{table_name}'...")

                response = client.imp(
                    data_file_obj=data_file_obj_for_request,
                    data_file_name=file_path.name,  # Pass filename explicitly
                    schema_json_str=schema_content,  # Pass prepared schema string
                    schema_file_obj=schema_file_obj,  # Pass prepared schema file obj
                    table_name=table_name,
                    partition_by=args.partitionBy,
                    timestamp_col=args.timestamp,
                    overwrite=args.overwrite,
                    atomicity=args.atomicity,
                    delimiter=args.delimiter,
                    force_header=args.forceHeader,
                    skip_lev=args.skipLev,
                    fmt=args.fmt,
                    o3_max_lag=args.o3MaxLag,
                    max_uncommitted_rows=args.maxUncommittedRows,
                    create_table=args.create,
                )

                # --- Process Response ---
                import_failed_this_file = False
                response_text = ""
                response_json = None

                try:
                    if args.fmt == "json":
                        response_json = response.json()
                        # Check status within JSON response
                        if response_json.get("status") != "OK":
                            import_failed_this_file = True
                            logger.warning(
                                f"Import of '{file_path}' failed (JSON status: {response_json.get('status')})."
                            )
                            if (
                                "errors" in response_json
                            ):  # Log column errors if present
                                logger.warning(
                                    f"Column Errors: {response_json['errors']}"
                                )

                    else:  # Tabular format
                        response_text = response.text
                        # Basic check for tabular failure (less reliable) - might need improvement
                        # error is always in the response text for tabular
                        # if (
                        #     "error" in response_text.lower()
                        #     or response.status_code >= 400
                        # ):
                        #     import_failed_this_file = True
                        #     logger.warning(
                        #         f"Import of '{file_path}' may have failed (status code: {response.status_code})."
                        #     )

                except json.JSONDecodeError:
                    import_failed_this_file = True
                    logger.warning(
                        f"File '{file_path}': Received non-JSON response when JSON format was requested."
                    )
                    response_text = response.text  # Get text for logging
                    logger.warning(
                        f"Raw response: {response_text[:500]}"
                    )  # Log first 500 chars

                # --- Output Response ---
                if i > 0 and args.fmt == "json":
                    sys.stdout.write(json_separator)

                if args.fmt == "json" and response_json is not None:
                    json.dump(response_json, sys.stdout, indent=2)
                    sys.stdout.write("\n")
                else:
                    sys.stdout.write(response_text)
                    if response_text and not response_text.endswith("\n"):
                        sys.stdout.write("\n")

                # --- Handle Failure ---
                if import_failed_this_file:
                    any_file_failed = True
                    if args.stop_on_error:
                        logger.warning(
                            "Stopping execution due to import failure (stop-on-error enabled)."
                        )
                        sys.exit(1)
                else:
                    logger.info(f"File '{file_path}' processed.")

            except (
                QuestDBError,
                OSError,
                IOError,
            ) as e:  # Catch client errors and file errors
                logger.warning(f"Processing failed for file '{file_path}': {e}")
                any_file_failed = True
                if args.stop_on_error:
                    logger.warning(
                        "Stopping execution due to error (stop-on-error enabled)."
                    )
                    sys.exit(1)
                else:
                    logger.warning(
                        "Continuing with next file (stop-on-error disabled)."
                    )
            except KeyboardInterrupt:
                logger.info("\nOperation cancelled by user during file processing.")
                sys.exit(130)
            finally:
                # Close data file if it was opened
                if data_file_obj_for_request:
                    data_file_obj_for_request.close()

    finally:
        # Close schema file if it was opened
        if schema_file_obj:
            schema_file_obj.close()

    # --- Final Exit Status ---
    if any_file_failed:
        logger.warning("One or more files failed during import.")
        sys.exit(2)  # Indicate partial failure
    else:
        logger.info("All files processed.")  # Changed message slightly
        sys.exit(0)


def handle_exec(args, client: QuestDBClient):
    """Handles the /exec command using the client."""
    import importlib  # added for module query import

    sql_content = ""
    source_description = ""

    # New: load query from a Python module if specified
    if args.get_query_from_python_module:
        try:
            module_spec, sep, var_name = args.get_query_from_python_module.partition(
                ":"
            )
            if not sep:
                logger.error(
                    "Invalid format for --get-query-from-python-module. Expected module_path:variable_name."
                )
                sys.exit(1)
            # append cwd to sys.path to allow local module imports
            logger.info(
                f"Adding current working directory {Path.cwd()} to sys.path for module import."
            )
            sys.path.append(str(Path.cwd()))
            logger.debug(f"sys.path: {sys.path}")  # Log the sys.path for debugging
            logger.info(f"Importing module: {module_spec}")
            mod = importlib.import_module(module_spec)
            query_str = getattr(mod, var_name, None)
            if not isinstance(query_str, str):
                logger.error("The specified variable from module is not a string.")
                sys.exit(1)
            sql_content = query_str
            source_description = args.get_query_from_python_module
            logger.info(
                f"Loaded SQL from module variable: {args.get_query_from_python_module}"
            )
        except Exception as e:
            logger.error(f"Error loading query from module: {e}")
            sys.exit(1)
    elif args.query:
        sql_content = args.query
        source_description = "query string"
    elif args.file:
        try:
            with open(args.file, "r", encoding="utf-8") as f:
                sql_content = f.read()
            source_description = f"file '{args.file}'"
        except IOError as e:
            logger.warning(f"Error reading SQL file '{args.file}': {e}")
            sys.exit(1)
    elif not sys.stdin.isatty():
        sql_content = sys.stdin.read()
        source_description = "standard input"
        if not sql_content:
            logger.warning("Received empty input from stdin.")
            sys.exit(1)
    else:
        logger.warning("No SQL query provided via argument, file, module, or stdin.")
        sys.exit(1)

    # 2. Extract Statements (same as before)
    try:
        statements = extract_statements_from_sql(sql_content)
    except Exception as e:
        logger.error(f"Failed to parse SQL from {source_description}: {e}")
        sys.exit(1)

    if not statements:
        logger.warning(f"No valid SQL statements found in {source_description}.")
        sys.exit(0)

    logger.info(f"Found {len(statements)} statement(s) in {source_description}.")

    any_statement_failed = False
    json_separator = "\n"

    # 3. Execute Statements Iteratively
    for i, statement in enumerate(statements):
        logger.info(f"Executing statement {i + 1}/{len(statements)}...")
        logger.debug(
            f"Statement: {statement[:100]}{'...' if len(statement) > 100 else ''}"
        )

        # --- Dry Run Check ---
        if args.dry_run:
            simulate_exec(args, statement, i + 1, len(statements))
            if i > 0:  # Print separator if not first dry-run statement
                sys.stdout.write(json_separator)
            continue  # Skip actual execution

        try:
            response_json = client.exec(
                query=statement,
                limit=args.limit,
                count=args.count,
                nm=args.nm,
                timings=args.timings,
                explain=args.explain,
                quote_large_num=args.quoteLargeNum,
                statement_timeout=args.statement_timeout,
            )

            # Check for errors within the JSON response (QuestDB API errors)
            # Note: QuestDBAPIError exception handles HTTP errors, this checks logical errors in 200 OK response
            if isinstance(response_json, dict) and "error" in response_json:
                logger.error(
                    f"Error executing statement {i + 1}: {response_json['error']}"
                )
                # Print simplified error to stdout, detailed error already logged by client
                sys.stdout.write(
                    f"-- Statement {i + 1} Error --\nError: {response_json['error']}\n"
                )
                sys.stdout.write(f"Query: {response_json.get('query', statement)}\n")
                any_statement_failed = True
                if args.stop_on_error:
                    logger.warning(
                        "Stopping execution due to error (stop-on-error enabled)."
                    )
                    sys.exit(1)
                else:
                    logger.warning("Continuing execution (stop-on-error disabled).")
                    continue  # Skip to next statement

            # Print separator if this is not the first successful output
            if i > 0:
                # Check if the *previous* one resulted in output (wasn't a skipped error)
                # This logic might need refinement if complex error/skip patterns occur.
                # Simplification: always print separator after the first statement's output.
                sys.stdout.write(json_separator)

            if args.one:
                # get json path dataset[0][0] and print
                # it's a str, not json
                if isinstance(response_json, dict) and "dataset" in response_json:
                    if (
                        len(response_json["dataset"]) > 0
                        and len(response_json["dataset"][0]) > 0
                    ):
                        sys.stdout.write(f"{response_json['dataset'][0][0]}\n")

            # New markdown formatting for exec output
            elif (
                (args.markdown or args.psql)
                and isinstance(response_json, dict)
                and "columns" in response_json
                and "dataset" in response_json
            ):
                try:
                    from tabulate import tabulate

                    headers = [col["name"] for col in response_json["columns"]]
                    table = response_json["dataset"]
                    fmt = "psql"
                    if args.psql:
                        fmt = "psql"
                    elif args.markdown:
                        fmt = "github"
                    md_table = tabulate(table, headers=headers, tablefmt=fmt)
                    sys.stdout.write(md_table + "\n")
                except ImportError:
                    sys.stdout.write(
                        "Tabulate library not installed. Please install 'tabulate'.\n"
                    )
                    json.dump(response_json, sys.stdout, indent=2)
                    sys.stdout.write("\n")
            else:
                json.dump(response_json, sys.stdout, indent=2)
                sys.stdout.write("\n")

            logger.info(f"Statement {i + 1} executed successfully.")

        except QuestDBAPIError as e:
            # Error logged by client, just record failure and decide stop/continue
            logger.warning(f"Statement {i + 1} failed with API error.")
            # Output error info from exception to stdout for visibility
            sys.stdout.write(f"-- Statement {i + 1} Error --\nError: {e}\n")
            if e.response_data and "query" in e.response_data:
                sys.stdout.write(f"Query: {e.response_data['query']}\n")
            elif "query" in statement:  # Fallback to original statement
                sys.stdout.write(f"Query: {statement}\n")

            any_statement_failed = True
            if args.stop_on_error:
                logger.warning(
                    "Stopping execution due to API error (stop-on-error enabled)."
                )
                sys.exit(1)
            else:
                logger.warning("Continuing execution (stop-on-error disabled).")
        except QuestDBError as e:  # Catch other client errors (connection etc.)
            logger.warning(f"Statement {i + 1} failed: {e}")
            any_statement_failed = True
            if args.stop_on_error:
                logger.warning(
                    "Stopping execution due to error (stop-on-error enabled)."
                )
                sys.exit(1)
            else:
                logger.warning("Continuing execution (stop-on-error disabled).")
        except KeyboardInterrupt:
            logger.info(
                f"\nOperation cancelled by user during statement {i + 1} execution."
            )
            sys.exit(130)

    # 4. Final Exit Status
    if any_statement_failed:
        logger.warning("One or more statements failed during execution.")
        sys.exit(2)
    else:
        logger.info("All statements executed successfully.")
        sys.exit(0)


def handle_exp(args, client: QuestDBClient):
    """Handles the /exp command using the client."""

    logger.info(f"Exporting data from {client.base_url}...")
    logger.info(f"Query: {args.query}")

    # --- Dry Run Check ---
    if args.dry_run:
        simulate_exp(args)
        sys.exit(0)  # Exit after simulation

    stream_enabled = bool(args.output_file)
    output_file_handle = None
    output_target_desc = "stdout"

    try:
        # Get the response object from the client
        response = client.exp(
            query=args.query,
            limit=args.limit,
            nm=args.nm,
            stream_response=stream_enabled,  # Tell client to stream if writing to file
        )

        # --- Output Response to stdout or file ---
        if args.output_file:
            output_file_path = Path(args.output_file)
            output_target_desc = f"file '{output_file_path}'"
            logger.info(f"Writing output to {output_target_desc}")
            try:
                # Open file in binary write mode
                output_file_handle = open(output_file_path, "wb")
                # Iterate over content chunks and write to file
                for chunk in response.iter_content(chunk_size=8192):
                    output_file_handle.write(chunk)
                logger.info(f"Successfully exported data to {output_target_desc}")
            except IOError as e:
                logger.warning(
                    f"Error writing to output file '{output_file_path}': {e}"
                )
                sys.exit(1)
        else:
            # Write text response directly to stdout
            output_text = (
                response.text
            )  # Assumes content fits in memory if not streaming
            sys.stdout.write(output_text)
            # Ensure the output ends with a newline if it doesn't already
            if output_text and not output_text.endswith("\n"):
                sys.stdout.write("\n")
            logger.info("Successfully exported data to stdout.")

    except QuestDBError as e:
        logger.error(f"Export failed: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user during export.")
        sys.exit(130)
    finally:
        # Ensure the file handle is closed if it was opened
        if output_file_handle:
            output_file_handle.close()
            # Close the response connection if streaming was used and file writing finished/failed
        if stream_enabled and "response" in locals() and response:
            response.close()


def handle_chk(args, client: QuestDBClient):
    """Handles the /chk command using the client."""
    table_name = args.table_name
    logger.info(f"Checking existence of table '{table_name}'...")

    # --- Dry Run Check ---
    if args.dry_run:
        simulate_chk(args)
        sys.exit(0)

    try:
        exists = client.table_exists(table_name)
        status_message = "Exists" if exists else "Does not exist"
        logger.info(f"Result: Table '{table_name}' {status_message.lower()}.")
        # Output consistent JSON to stdout
        print(json.dumps({"tableName": table_name, "status": status_message}, indent=2))
        sys.exit(0 if exists else 3)  # Exit 0 if exists, Exit 3 if not exists

    except QuestDBAPIError as e:
        logger.error(f"API Error checking table '{table_name}': {e}")
        # Output error JSON to stdout
        print(
            json.dumps(
                {"tableName": table_name, "status": "Error", "detail": str(e)}, indent=2
            )
        )
        sys.exit(1)
    except QuestDBError as e:
        logger.error(f"Error checking table '{table_name}': {e}")
        print(
            json.dumps(
                {"tableName": table_name, "status": "Error", "detail": str(e)}, indent=2
            )
        )
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user.")
        sys.exit(130)


def handle_gen_config(args, client: Any = None):
    """Generates a default config file at ~/.questdb-rest/config.json."""
    config_dir = os.path.expanduser("~/.questdb-rest")
    config_file = os.path.join(config_dir, "config.json")
    default_config = {
        "host": "localhost",
        "port": 9000,
        "user": "",
        "password": "",
        "timeout": 60,
        "scheme": "http",
    }
    try:
        os.makedirs(config_dir, exist_ok=True)
        with open(config_file, "w") as cf:
            json.dump(default_config, cf, indent=2)
        print(f"Default config file generated at {config_file}")
    except Exception as e:
        print(f"Error generating config file: {e}")
        sys.exit(1)
    sys.exit(0)


# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(
        description="QuestDB REST API Command Line Interface.\nLogs to stderr, outputs data to stdout.\n\n"
        "Uses QuestDB REST API via questdb_rest library.",
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )

    # Global arguments
    parser.add_argument(
        "-H",
        "--host",
        default=DEFAULT_HOST,
        help=f"QuestDB server host (default: {DEFAULT_HOST}).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"QuestDB REST API port (default: {DEFAULT_PORT}).",
    )
    parser.add_argument("-u", "--user", help="Username for basic authentication.")
    parser.add_argument(
        "-p",
        "--password",
        help="Password for basic authentication. If -u is given but -p is not, will prompt securely.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=QuestDBClient.DEFAULT_TIMEOUT,
        help=f"Request timeout in seconds (default: {QuestDBClient.DEFAULT_TIMEOUT}).",
    )
    log_level_group = parser.add_mutually_exclusive_group()
    log_level_group.add_argument(
        "-W",
        "--warning",
        action="store_true",
        help="Use warning level logging",
    )
    log_level_group.add_argument(
        "-D",
        "--debug",
        action="store_true",
        help="Enable debug level logging to stderr.",
    )
    parser.add_argument(
        "-R",
        "--dry-run",
        action="store_true",
        help="Simulate API calls without sending them. Logs intended actions.",
    )
    parser.add_argument(
        "--config",
        help="Path to a config JSON file (overrides default).",
        default=None,
    )

    subparsers = parser.add_subparsers(
        dest="command", required=True, help="Available sub-commands"
    )

    # --- IMP Sub-command ---
    parser_imp = subparsers.add_parser(
        "imp",
        help="Import data from file(s) using /imp.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        add_help=False,
    )
    parser_imp.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    parser_imp.add_argument(
        "files", nargs="+", help="Path(s) to the data file(s) to import."
    )
    parser_imp.add_argument(
        "-n",
        "--name",
        help="Explicit table name. Overrides --name-func. Applied to ALL files.",
    )
    parser_imp.add_argument(
        "--name-func",
        choices=list(TABLE_NAME_FUNCTIONS.keys()),
        help=f"Function to generate table name from filename (ignored if --name set). Available: {', '.join(TABLE_NAME_FUNCTIONS.keys())}",
        default=None,
    )
    parser_imp.add_argument(
        "--name-func-prefix",
        help="Prefix string for 'add_prefix' name function.",
        default="",
    )  # Default to empty string
    schema_group = parser_imp.add_mutually_exclusive_group()
    schema_group.add_argument(
        "--schema-file", help="Path to JSON schema file. Applied to ALL files."
    )
    schema_group.add_argument(
        "-s", "--schema", help="JSON schema string. Applied to ALL files. Use quotes."
    )
    parser_imp.add_argument(
        "-P",
        "--partitionBy",
        choices=["NONE", "YEAR", "MONTH", "DAY", "HOUR", "WEEK"],
        help="Partitioning strategy (if table created).",
    )
    parser_imp.add_argument(
        "-t", "--timestamp", help="Designated timestamp column name (if table created)."
    )
    parser_imp.add_argument(
        "-o",
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        help="Overwrite existing table data/structure.",
    )
    parser_imp.add_argument(
        "-a",
        "--atomicity",
        choices=["skipCol", "skipRow", "abort"],
        default="skipCol",
        help="Behavior on data errors during import.",
    )
    parser_imp.add_argument(
        "-d", "--delimiter", help="Specify CSV delimiter character."
    )
    parser_imp.add_argument(
        "-F",
        "--forceHeader",
        action=argparse.BooleanOptionalAction,
        help="Force treating the first line as a header.",
    )
    parser_imp.add_argument(
        "-S",
        "--skipLev",
        action=argparse.BooleanOptionalAction,
        help="Skip Line Extra Values.",
    )
    parser_imp.add_argument(
        "--fmt",
        choices=["tabular", "json"],
        default="tabular",
        help="Format for the response message to stdout.",
    )
    parser_imp.add_argument(
        "-O",
        "--o3MaxLag",
        type=int,
        help="Set O3 max lag (microseconds, if table created).",
    )
    parser_imp.add_argument(
        "-M",
        "--maxUncommittedRows",
        type=int,
        help="Set max uncommitted rows (if table created).",
    )
    parser_imp.add_argument(
        "-c",
        "--create",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Automatically create table if it does not exist.",
    )
    parser_imp.add_argument(
        "--stop-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stop execution immediately if importing any file fails.",
    )
    parser_imp.set_defaults(func=handle_imp)

    # --- EXEC Sub-command ---
    parser_exec = subparsers.add_parser(
        "exec",
        help="Execute SQL statement(s) using /exec (returns JSON).\nReads SQL from --query, --file, --get-query-from-python-module, or stdin.",
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )
    parser_exec.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    query_input_group = parser_exec.add_mutually_exclusive_group()
    query_input_group.add_argument("-q", "--query", help="SQL query string to execute.")
    query_input_group.add_argument(
        "-f", "--file", help="Path to file containing SQL statements."
    )
    # New option: get query from python module (e.g. a_module.b_module:my_sql_statement)
    query_input_group.add_argument(
        "-P",
        "--get-query-from-python-module",
        help="Get query from a Python module in the format 'module_path:variable_name'.",
    )
    parser_exec.add_argument(
        "-l",
        "--limit",
        help='Limit results (e.g., "10", "10,20"). Applies per statement.',
    )
    parser_exec.add_argument(
        "-C",
        "--count",
        action=argparse.BooleanOptionalAction,
        help="Include row count in response.",
    )
    parser_exec.add_argument(
        "--nm",
        dest="nm",
        action=argparse.BooleanOptionalAction,
        help="Skip metadata in response.",
    )
    parser_exec.add_argument(
        "-T",
        "--timings",
        action=argparse.BooleanOptionalAction,
        help="Include execution timings.",
    )
    parser_exec.add_argument(
        "-E",
        "--explain",
        action=argparse.BooleanOptionalAction,
        help="Include execution plan details.",
    )
    parser_exec.add_argument(
        "-Q",
        "--quoteLargeNum",
        action=argparse.BooleanOptionalAction,
        help="Return LONG numbers as quoted strings.",
    )
    parser_exec.add_argument(
        "--statement-timeout",
        type=int,
        help="Query timeout in milliseconds (per statement).",
    )
    parser_exec.add_argument(
        "--stop-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stop execution if any SQL statement fails.",
    )
    # New markdown output option
    exec_format_group = parser_exec.add_mutually_exclusive_group()
    exec_format_group.add_argument(
        "-o",
        "--one",
        action="store_true",
        help="Extract and Display the first item in query result",
    )
    exec_format_group.add_argument(
        "-m",
        "--markdown",
        action="store_true",
        help="Display query result in Markdown table format using tabulate.",
    )
    # -p, --psql format
    exec_format_group.add_argument(
        "-p",
        "--psql",
        action="store_true",
        help="Display query result in PostgreSQL table format using tabulate.",
    )
    parser_exec.set_defaults(func=handle_exec)

    # --- EXP Sub-command ---
    parser_exp = subparsers.add_parser(
        "exp",
        help="Export data using /exp (returns CSV to stdout or file).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        add_help=False,
    )
    parser_exp.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    parser_exp.add_argument("query", help="SQL query for data export.")
    parser_exp.add_argument(
        "-l", "--limit", help='Limit results (e.g., "10", "10,20", "-20").'
    )
    parser_exp.add_argument(
        "--nm",
        dest="nm",
        action=argparse.BooleanOptionalAction,
        help="Skip header row in CSV output.",
    )
    parser_exp.add_argument(
        "-o", "--output-file", help="Path to save exported CSV data (default: stdout)."
    )
    parser_exp.set_defaults(func=handle_exp)

    # --- CHK Sub-command ---
    parser_chk = subparsers.add_parser(
        "chk",
        help="Check if a table exists using /chk (returns JSON).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        add_help=False,
    )
    parser_chk.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    parser_chk.add_argument("table_name", help="Name of the table to check.")
    parser_chk.set_defaults(func=handle_chk)

    # --- GEN-CONFIG Sub-command ---
    parser_gen_config = subparsers.add_parser(
        "gen-config",
        help="Generate a default config file at ~/.questdb-rest/config.json",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        add_help=False,
    )
    parser_gen_config.set_defaults(func=handle_gen_config)

    # --- Parse Arguments ---
    try:
        args = parser.parse_args()

        # --- Load config for password if available ---
        import os

        actual_password = args.password
        if args.user and not args.password and not args.dry_run:
            config_file = os.path.expanduser("~/.questdb-rest/config.json")
            config = {}
            if os.path.exists(config_file):
                try:
                    with open(config_file, "r") as cf:
                        config = json.load(cf)
                except Exception as e:
                    logger.debug(f"Error loading config file {config_file}: {e}")
            if "password" in config:
                actual_password = config["password"]
                logger.info("Using password from config file.")
            else:
                try:
                    actual_password = getpass(f"Password for user '{args.user}': ")
                    if not actual_password:
                        logger.warning("Password required but not provided.")
                        sys.exit(1)
                except (EOFError, KeyboardInterrupt):
                    logger.info("\nOperation cancelled during password input.")
                    sys.exit(130)

        # Validate imp --name-func arguments
        if args.command == "imp":
            if args.name_func == "add_prefix" and not args.name_func_prefix:
                logger.debug(
                    "Using default empty prefix for 'add_prefix' name function as --name-func-prefix was not provided."
                )
            elif args.name_func and args.name:
                logger.warning(
                    "Both --name and --name-func provided. Explicit --name will be used."
                )

    except Exception as e:
        parser.print_usage(sys.stderr)
        logger.error(f"Argument parsing error: {e}")
        sys.exit(2)

    # Set logging level
    if args.warning:
        logging.getLogger().setLevel(logging.WARNING)  # Set root logger level
        logger.setLevel(logging.WARNING)
        logger.debug("Debug logging enabled.")
    elif args.debug:
        logging.getLogger().setLevel(logging.DEBUG)  # Set root logger level
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled.")
    else:
        logger.setLevel(logging.INFO)

    # --- Instantiate Client ---
    # Do not instantiate client in dry-run mode to avoid connection attempts etc.
    client = None
    if not args.dry_run:
        try:
            if args.config:
                client = QuestDBClient.from_config_file(args.config)
            else:
                client = QuestDBClient(
                    host=args.host,
                    port=args.port,
                    user=args.user,
                    password=actual_password,  # Use potentially prompted password
                    timeout=args.timeout,
                )
        except (QuestDBError, ValueError) as e:
            logger.error(f"Failed to initialize QuestDB client: {e}")
            sys.exit(1)

    # Call the appropriate handler function
    try:
        # Pass the client instance to the handler (will be None in dry-run)
        args.func(args, client)
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user.")
        sys.exit(130)
    except SystemExit as e:
        # Allow sys.exit calls from handlers to propagate
        sys.exit(e.code)
    except Exception as e:
        # Catch-all for unexpected errors in command handlers
        logger.exception(
            f"An unexpected error occurred during command '{args.command}': {e}"
        )
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        # Final fallback
        logger.exception(f"An unexpected error occurred at the top level: {e}")
        sys.exit(1)
