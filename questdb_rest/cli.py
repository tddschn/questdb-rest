#!/usr/bin/env python3


#!/usr/bin/env uv run

# /// script
# requires-python = '>=3.11'
# dependencies = [
#     # requests is now a dependency of questdb_rest
#     "sqlparse",
#     "icecream",
#     "tabulate", # Added for exec --markdown/--psql
#     "./questdb_rest.py" # Include the library file as a dependency
# ]
# ///

import argparse
import html
from typing import Any, Dict
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
    level=logging.WARNING,
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


def simulate_schema(args, table_name):
    logger.info(f"[DRY-RUN] Simulating schema fetch for table '{table_name}':")
    query = f'SHOW CREATE TABLE "{table_name}";'  # Quote table name
    logger.info(f"[DRY-RUN]   Would execute query: {query}")
    # Simulate the output format: just the CREATE TABLE statement string
    print(
        f'CREATE TABLE "{table_name}" (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY; -- (Simulated)'
    )


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
            final_table_name = args.name  # Start with explicitly provided name

            if not final_table_name:  # Only derive if --name was not provided
                derived_table_name = None
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
                            derived_table_name = func(file_path, **func_kwargs)
                            logger.info(
                                f"Using name function '{name_func_choice}' -> derived name: '{derived_table_name}'"
                            )
                        except Exception as e:
                            logger.error(
                                f"Error executing name function '{name_func_choice}' for file '{file_path}': {e}"
                            )
                            any_file_failed = True
                            if args.stop_on_error:
                                sys.exit(1)
                            else:
                                continue  # Skip this file
                    else:
                        logger.error(
                            f"Internal error: Unknown name function '{name_func_choice}'."
                        )
                        any_file_failed = True
                        if args.stop_on_error:
                            sys.exit(1)
                        else:
                            continue  # Skip this file
                else:
                    # Default: use file stem
                    derived_table_name = get_table_name_from_stem(file_path)
                    logger.info(
                        f"Using default naming (file stem) -> derived name: '{derived_table_name}'"
                    )

                if not derived_table_name:
                    logger.error(
                        f"Could not derive table name for file '{file_path}'. Skipping."
                    )
                    any_file_failed = True
                    if args.stop_on_error:
                        sys.exit(1)
                    else:
                        continue  # Skip this file

                final_table_name = (
                    derived_table_name  # Assign derived name as the final name for now
                )

                # --- Apply dash-to-underscore conversion if requested ---
                if args.dash_to_underscore:
                    original_derived_name = final_table_name
                    final_table_name = final_table_name.replace("-", "_")
                    if original_derived_name != final_table_name:
                        logger.info(
                            f"Applied dash-to-underscore: '{original_derived_name}' -> '{final_table_name}'"
                        )
                    else:
                        logger.debug(
                            f"Dash-to-underscore requested, but derived name '{final_table_name}' contains no dashes."
                        )

            else:  # --name was explicitly provided
                logger.info(
                    f"Using explicitly provided table name: '{final_table_name}'"
                )
                if args.dash_to_underscore:
                    logger.warning(
                        "Ignoring --dash-to-underscore because explicit --name was provided."
                    )

            # --- Final check on table name validity ---
            if not final_table_name:
                logger.error(
                    f"Could not determine final table name for file '{file_path}'. Skipping."
                )
                any_file_failed = True
                if args.stop_on_error:
                    sys.exit(1)
                else:
                    continue  # Skip this file

            # --- Dry Run Check ---
            if args.dry_run:
                simulate_imp(args, file_path, final_table_name, schema_source_desc)
                # Add separator if not the first file and json format
                if i > 0 and args.fmt == "json":
                    sys.stdout.write(json_separator)
                continue  # Skip actual import in dry-run

            # --- Make the Request via Client ---
            data_file_obj_for_request = None
            try:
                # Open data file just before the request
                data_file_obj_for_request = open(file_path, "rb")

                logger.info(
                    f"Importing '{file_path}' into table '{final_table_name}'..."
                )

                response = client.imp(
                    data_file_obj=data_file_obj_for_request,
                    data_file_name=file_path.name,  # Pass filename explicitly
                    schema_json_str=schema_content,  # Pass prepared schema string
                    schema_file_obj=schema_file_obj,  # Pass prepared schema file obj
                    table_name=final_table_name,  # Use the final calculated name
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
    json_separator = "\n"  # Separator between JSON outputs for multiple statements
    markdown_psql_separator = "\n\n"  # Separator for markdown/psql tables
    output_separator = ""  # Will be set based on output format

    # Determine the separator based on the output format requested
    if args.markdown or args.psql:
        output_separator = markdown_psql_separator
    elif not args.one:  # Default JSON output uses newline
        output_separator = json_separator

    # 3. Execute Statements Iteratively
    first_output_written = False
    for i, statement in enumerate(statements):
        logger.info(f"Executing statement {i + 1}/{len(statements)}...")
        if args.explain_only:
            if not statement.lower().startswith("explain"):
                statement = f"EXPLAIN {statement}"
        elif args.create_table:
            new_table_name = args.new_table_name
            assert new_table_name, "New table name must be provided for --create-table"
            statement = f"CREATE TABLE {new_table_name} AS ({statement})"
        logger.debug(
            f"Statement: {statement[:100]}{'...' if len(statement) > 100 else ''}"
        )

        # --- Dry Run Check ---
        if args.dry_run:
            simulate_exec(args, statement, i + 1, len(statements))
            if first_output_written:  # Print separator if not first dry-run statement
                sys.stdout.write(output_separator)
            first_output_written = True
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
            if isinstance(response_json, dict) and "error" in response_json:
                logger.error(
                    f"Error executing statement {i + 1}: {response_json['error']}"
                )
                # Print simplified error to stdout, detailed error already logged by client
                sys.stderr.write(
                    f"-- Statement {i + 1} Error --\nError: {response_json['error']}\n"
                )
                sys.stderr.write(f"Query: {response_json.get('query', statement)}\n")
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
            if first_output_written and output_separator:
                sys.stdout.write(output_separator)

            # --- Handle Output Formatting ---
            output_written_this_statement = False
            if args.explain_only:
                if isinstance(response_json, dict) and "dataset" in response_json:
                    explain_text = explain_output_to_text(response_json)
                    sys.stdout.write(explain_text + "\n")
            elif args.one:
                if isinstance(response_json, dict) and "dataset" in response_json:
                    if (
                        len(response_json["dataset"]) > 0
                        and len(response_json["dataset"][0]) > 0
                    ):
                        sys.stdout.write(f"{response_json['dataset'][0][0]}\n")
                        output_written_this_statement = True
                    else:
                        logger.debug(
                            f"Statement {i + 1}: --one specified, but dataset was empty or lacked rows/columns."
                        )
                        # Optionally print an empty line or nothing? Let's print nothing.
                else:
                    logger.debug(
                        f"Statement {i + 1}: --one specified, but response was not a dict or lacked 'dataset'."
                    )

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
                    # Only print table if there are columns and/or data
                    if headers or table:
                        fmt = (
                            "psql" if args.psql else "github"
                        )  # Default to github if --markdown
                        md_table = tabulate(table, headers=headers, tablefmt=fmt)
                        sys.stdout.write(md_table + "\n")
                        output_written_this_statement = True
                    else:
                        logger.debug(
                            f"Statement {i + 1}: --markdown/psql specified, but no columns or data returned."
                        )
                        # Print empty line? Let's print nothing.
                except ImportError:
                    sys.stderr.write(
                        "Tabulate library not installed. Please install 'tabulate'. Falling back to JSON.\n"
                    )
                    # Fallback to JSON dump if tabulate is missing
                    json.dump(response_json, sys.stdout, indent=2)
                    sys.stdout.write("\n")
                    output_written_this_statement = True
                except Exception as tab_err:
                    # Catch other tabulate errors
                    logger.error(
                        f"Error during tabulate formatting for statement {i + 1}: {tab_err}"
                    )
                    sys.stderr.write(
                        f"Error during table formatting: {tab_err}. Falling back to JSON.\n"
                    )
                    json.dump(response_json, sys.stdout, indent=2)
                    sys.stdout.write("\n")
                    output_written_this_statement = True

            else:  # Default: JSON output
                # Only print JSON if it's not just a simple DDL response (like {'ddl': 'OK'})
                # unless it's the *only* thing in the response.
                if not (
                    len(response_json) == 1
                    and "ddl" in response_json
                    and response_json["ddl"] == "OK"
                ):
                    json.dump(response_json, sys.stdout, indent=2)
                    sys.stdout.write("\n")
                    output_written_this_statement = True
                else:
                    logger.debug(
                        f"Statement {i + 1}: Suppressing simple DDL OK response in default JSON output."
                    )

            if output_written_this_statement:
                first_output_written = True  # Mark that we have produced output

            logger.info(f"Statement {i + 1} executed successfully.")

        except QuestDBAPIError as e:
            # Error logged by client, just record failure and decide stop/continue
            logger.warning(f"Statement {i + 1} failed with API error: {e}")
            # Output error info from exception to stderr for visibility
            sys.stderr.write(f"-- Statement {i + 1} Error --\nError: {e}\n")
            if e.response_data and "query" in e.response_data:
                sys.stderr.write(f"Query: {e.response_data['query']}\n")
            elif "query" in statement:  # Fallback to original statement
                sys.stderr.write(f"Query: {statement}\n")

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
            sys.stderr.write(f"-- Statement {i + 1} Error --\nError: {e}\n")
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
    response = None  # Initialize response variable

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
                    if chunk:  # filter out keep-alive new chunks
                        output_file_handle.write(chunk)
                logger.info(f"Successfully exported data to {output_target_desc}")
            except IOError as e:
                logger.warning(
                    f"Error writing to output file '{output_file_path}': {e}"
                )
                sys.exit(1)
        else:
            # Write text response directly to stdout
            # Need to handle potential streaming even for stdout if necessary
            # For simplicity now, load text if not streaming to file.
            # If streaming is needed for stdout, iter_content should be used here too.
            output_text = response.text
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
        # Close the response connection if streaming was used, regardless of target
        if stream_enabled and response:
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


def explain_output_to_text(data: Dict[str, Any]) -> str:
    """Convert query plan dict to plain text output."""
    lines = [html.unescape(row[0]) for row in data.get("dataset", [])]
    return "\n".join(lines)


# --- NEW: handle_schema ---
def handle_schema(args, client: QuestDBClient):
    """Handles the schema command, fetching CREATE TABLE statements."""
    any_table_failed = False
    output_separator = "\n\n"  # Separate multiple CREATE TABLE statements

    if not args.dry_run:
        log_level = logging.WARNING
        logger.setLevel(log_level)

    # Iterate through provided table names
    first_output_written = False
    for i, table_name in enumerate(args.table_names):
        logger.info(
            f"Fetching schema for table {i + 1}/{len(args.table_names)}: '{table_name}'..."
        )

        # --- Dry Run Check ---
        if args.dry_run:
            simulate_schema(args, table_name)
            if first_output_written:
                sys.stdout.write(output_separator)
            first_output_written = True
            continue  # Skip actual execution

        # --- Actual Execution ---
        # Quote the table name for safety, especially if it contains special characters
        # QuestDB syntax typically uses double quotes for identifiers.
        # Handle potential existing quotes in table_name? Assume simple names for now.
        safe_table_name = table_name.replace('"', '""')  # Basic escaping if needed
        statement = f'SHOW CREATE TABLE "{safe_table_name}";'

        try:
            response_json = client.exec(
                query=statement,
                # These exec options are generally not relevant for SHOW CREATE TABLE
                limit=None,
                count=None,
                nm=None,
                timings=None,
                explain=None,
                quote_large_num=None,
                statement_timeout=args.statement_timeout,
            )

            # Check for errors within the JSON response
            if isinstance(response_json, dict) and "error" in response_json:
                logger.error(
                    f"Error fetching schema for '{table_name}': {response_json['error']}"
                )
                sys.stderr.write(
                    f"-- Error for table '{table_name}' --\nError: {response_json['error']}\n"
                )
                any_table_failed = True
                if args.stop_on_error:
                    logger.warning(
                        "Stopping execution due to error (stop-on-error enabled)."
                    )
                    sys.exit(1)
                else:
                    logger.warning("Continuing execution (stop-on-error disabled).")
                    continue  # Skip to next table

            # Extract the CREATE TABLE statement (equivalent to --one in exec)
            create_statement = None
            if isinstance(response_json, dict) and "dataset" in response_json:
                if (
                    len(response_json["dataset"]) > 0
                    and len(response_json["dataset"][0]) > 0
                ):
                    create_statement = response_json["dataset"][0][0]
                else:
                    logger.warning(
                        f"Received empty dataset for 'SHOW CREATE TABLE {table_name}'."
                    )
                    # Maybe the table exists but the command returned unexpectedly?
                    sys.stderr.write(
                        f"-- Warning for table '{table_name}' --\nReceived empty result for SHOW CREATE TABLE.\n"
                    )
                    any_table_failed = True  # Treat as failure
                    if args.stop_on_error:
                        sys.exit(1)
                    else:
                        continue

            else:
                logger.error(
                    f"Unexpected response format for 'SHOW CREATE TABLE {table_name}': {response_json}"
                )
                sys.stderr.write(
                    f"-- Error for table '{table_name}' --\nUnexpected response format from server.\n"
                )
                any_table_failed = True
                if args.stop_on_error:
                    sys.exit(1)
                else:
                    continue

            # Print separator if this is not the first successful output
            if first_output_written:
                sys.stdout.write(output_separator)

            # Print the extracted CREATE TABLE statement
            if create_statement:
                sys.stdout.write(create_statement)
                # Ensure trailing newline (though SHOW CREATE TABLE usually includes one)
                if not create_statement.endswith("\n"):
                    sys.stdout.write("\n")
                first_output_written = True
                logger.info(f"Successfully fetched schema for '{table_name}'.")

        except QuestDBAPIError as e:
            logger.warning(
                f"Fetching schema for '{table_name}' failed with API error: {e}"
            )
            sys.stderr.write(f"-- Error for table '{table_name}' --\nError: {e}\n")
            any_table_failed = True
            if args.stop_on_error:
                logger.warning(
                    "Stopping execution due to API error (stop-on-error enabled)."
                )
                sys.exit(1)
            else:
                logger.warning("Continuing execution (stop-on-error disabled).")
        except QuestDBError as e:  # Catch other client errors (connection etc.)
            logger.warning(f"Fetching schema for '{table_name}' failed: {e}")
            sys.stderr.write(f"-- Error for table '{table_name}' --\nError: {e}\n")
            any_table_failed = True
            if args.stop_on_error:
                logger.warning(
                    "Stopping execution due to error (stop-on-error enabled)."
                )
                sys.exit(1)
            else:
                logger.warning("Continuing execution (stop-on-error disabled).")
        except (
            IndexError,
            KeyError,
            TypeError,
        ) as e:  # Catch errors during result extraction
            logger.error(
                f"Error parsing response for 'SHOW CREATE TABLE {table_name}': {e}"
            )
            sys.stderr.write(
                f"-- Error for table '{table_name}' --\nFailed to parse response from server: {e}\n"
            )
            any_table_failed = True
            if args.stop_on_error:
                sys.exit(1)
            else:
                continue
        except KeyboardInterrupt:
            logger.info(
                f"\nOperation cancelled by user while fetching schema for '{table_name}'."
            )
            sys.exit(130)

    # --- Final Exit Status ---
    if any_table_failed:
        logger.warning("One or more table schemas could not be fetched.")
        sys.exit(2)  # Indicate partial failure if stop-on-error was false
    else:
        logger.info("All requested schemas fetched successfully.")
        sys.exit(0)


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


def detect_scheme_in_host(host_str):
    """
    Detect if the host string already includes a URL scheme (http:// or https://).
    Returns a tuple of (scheme, actual_host) if scheme is detected, or (None, host_str) if not.
    """
    if not host_str:
        return None, host_str

    if host_str.startswith("http://"):
        return "http", host_str[7:]  # Remove "http://" prefix
    elif host_str.startswith("https://"):
        return "https", host_str[8:]  # Remove "https://" prefix

    return None, host_str  # No scheme detected in host string


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
        default=None,  # Default handled by client init (checks config file first)
        help=f"QuestDB server host.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,  # Default handled by client init
        help=f"QuestDB REST API port.",
    )
    parser.add_argument(
        "-u", "--user", default=None, help="Username for basic authentication."
    )
    parser.add_argument(
        "-p",
        "--password",
        default=None,
        help="Password for basic authentication. If -u is given but -p is not, will prompt securely unless password is in config.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,  # Default handled by client init
        help="Request timeout in seconds.",
    )
    parser.add_argument(
        "--scheme",
        default=None,  # Default handled by client init
        choices=["http", "https"],
        help="Connection scheme (http or https).",
    )
    log_level_group = parser.add_mutually_exclusive_group()
    log_level_group.add_argument(
        "-i",  # Changed from -v to -i for INFO
        "--info",
        action="store_true",
        help="Use info level logging (default is WARNING).",
    )
    log_level_group.add_argument(
        "-D",  # Global Debug flag - MUST NOT clash with subcommand flags
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
        help="Path to a specific config JSON file (overrides default ~/.questdb-rest/config.json).",
        default=None,
    )
    # Shared stop-on-error argument for commands that process multiple items
    parser.add_argument(
        "--stop-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,  # Default to stopping on error
        help="Stop execution immediately if any item (file/statement/table) fails.",
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
        help="Explicit table name. Overrides --name-func and --dash-to-underscore. Applied to ALL files.",
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

    # --- NEW dash-to-underscore flag for imp ---
    # Note: Using long form only to avoid conflict with global -D (--debug)
    parser_imp.add_argument(
        "-D",
        "--dash-to-underscore",
        action="store_true",
        help="If table name is derived from filename (i.e., --name not set), convert dashes (-) to underscores (_). Compatible with --name-func.",
    )
    # --- End new flag ---

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
    # --stop-on-error is now global
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
        "-G",  # Changed short opt to avoid conflict with imp -P
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
    group_query_modifier = parser_exec.add_argument_group("Query Modifier Options")
    group_query_modifier.add_argument(
        "--explain-only",
        action="store_true",
        help="Only show the execution plan for the query(s), not the results. Will prefix EXPLAIN to the query(s) if not already present.",
    )
    group_query_modifier.add_argument(
        "--create-table",
        action="store_true",
        help="Create a new table from the query result(s).",
    )
    group_query_modifier.add_argument(
        "--new-table-name",
        help="Name of the new table to create from query result(s). Required if --create-table is used.",
    )
    # --stop-on-error is now global
    # Output formatting options
    exec_format_group = parser_exec.add_mutually_exclusive_group()
    exec_format_group.add_argument(
        "-1",  # Changed short opt to avoid conflict with imp -o
        "--one",
        action="store_true",
        help="Output only the value of the first column of the first row.",
    )
    exec_format_group.add_argument(
        "-m",
        "--markdown",
        action="store_true",
        help="Display query result(s) in Markdown table format using tabulate.",
    )
    exec_format_group.add_argument(
        "-P",  # Changed short opt to avoid conflict with global -p
        "--psql",
        action="store_true",
        help="Display query result(s) in PostgreSQL table format using tabulate.",
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
        help="Check if a table exists using /chk (returns JSON). Exit code 0 if exists, 3 if not.",
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

    # --- SCHEMA Sub-command (NEW) ---
    parser_schema = subparsers.add_parser(
        "schema",
        help="Fetch CREATE TABLE statement(s) for one or more tables.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        add_help=False,
    )
    parser_schema.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    parser_schema.add_argument(
        "table_names", nargs="+", help="Name(s) of the table(s) to get schema for."
    )
    # Inherits global --stop-on-error
    parser_schema.add_argument(
        "--statement-timeout",  # Allow timeout per table schema fetch
        type=int,
        help="Query timeout in milliseconds (per table).",
    )
    parser_schema.set_defaults(func=handle_schema)

    # --- GEN-CONFIG Sub-command ---
    parser_gen_config = subparsers.add_parser(
        "gen-config",
        help="Generate a default config file at ~/.questdb-rest/config.json",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        add_help=False,
    )
    parser_gen_config.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    # No client needed for gen-config
    parser_gen_config.set_defaults(func=handle_gen_config, requires_client=False)

    # --- Parse Arguments ---
    try:
        args = parser.parse_args()
        # Add requires_client default if not set by a specific command (like gen-config)
        if not hasattr(args, "requires_client"):
            args.requires_client = True

    except Exception as e:
        parser.print_usage(sys.stderr)
        logger.error(f"Argument parsing error: {e}")
        sys.exit(2)

    # Set logging level based on args
    log_level = logging.WARNING
    if args.info:
        log_level = logging.INFO
    elif args.debug:
        log_level = logging.DEBUG

    # Also set level for the CLI's own logger if needed for specific CLI messages
    logger.setLevel(log_level)
    # Configure logging for the questdb_rest library as well
    library_logger = logging.getLogger("questdb_rest")
    library_logger.setLevel(log_level)
    # Ensure library logs go to stderr if handler not already present
    if not library_logger.hasHandlers():
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter(
            "%(name)s %(levelname)s: %(message)s"
        )  # Simpler format for library
        handler.setFormatter(formatter)
        library_logger.addHandler(handler)

    if log_level == logging.DEBUG:
        logger.debug("Debug logging enabled for CLI and library.")

    # --- Handle Password Prompting ---
    # This needs to happen *before* client initialization, but *after* parsing args
    # Only prompt if a user is provided, no password is given, not dry run, and client is needed
    actual_password = args.password
    if args.requires_client and args.user and not args.password and not args.dry_run:
        # Check config file *first* before prompting
        config_to_check = args.config or os.path.expanduser(
            "~/.questdb-rest/config.json"
        )
        config = {}
        if os.path.exists(config_to_check):
            try:
                with open(config_to_check, "r") as cf:
                    config = json.load(cf)
                # Only use password from config if user matches OR if config user is empty/not present
                config_user = config.get("user")
                if "password" in config and (
                    args.user == config_user or not config_user
                ):
                    actual_password = config.get("password")
                    if actual_password:  # Make sure password is not empty string
                        logger.info("Using password from config file.")
                    else:
                        logger.debug(
                            "Password found in config but is empty, will prompt."
                        )
                        actual_password = None  # Reset to trigger prompt
            except Exception as e:
                logger.debug(
                    f"Error loading config file {config_to_check} for password check: {e}"
                )

        # Prompt if password wasn't loaded from config
        if actual_password is None:  # Check again after attempting config load
            try:
                actual_password = getpass(f"Password for user '{args.user}': ")
                if not actual_password:  # Handle empty input during prompt
                    logger.warning("Password required but not provided.")
                    sys.exit(1)
            except (EOFError, KeyboardInterrupt):
                logger.info("\nOperation cancelled during password input.")
                sys.exit(130)

    # --- Validate Command Specific Args ---
    # Example: Validate imp --name-func arguments
    if args.command == "imp":
        if args.name_func == "add_prefix" and not args.name_func_prefix:
            logger.debug(
                "Using default empty prefix for 'add_prefix' name function as --name-func-prefix was not provided."
            )
        elif args.name_func and args.name:
            logger.warning(
                "Both --name and --name-func provided. Explicit --name will be used."
            )
        # Add warning if --dash-to-underscore is used with explicit --name
        if args.name and args.dash_to_underscore:
            # This warning is now also handled inside handle_imp for clarity per file
            pass

    # --- Instantiate Client (if needed and not dry run) ---
    client = None
    if args.requires_client and not args.dry_run:
        try:
            # Check if host already contains a scheme
            detected_scheme = None
            actual_host = args.host
            if args.host:
                detected_scheme, actual_host = detect_scheme_in_host(args.host)
                if detected_scheme:
                    logger.debug(
                        f"Detected scheme '{detected_scheme}://' in host parameter: '{args.host}'"
                    )
                    # Override scheme if detected in host, unless explicitly provided via --scheme
                    if not args.scheme:
                        args.scheme = detected_scheme

            client_kwargs = {
                "host": actual_host,  # Use the host without scheme
                "port": args.port,
                "user": args.user,
                "password": actual_password,  # Use potentially prompted/config password
                "timeout": args.timeout,
                "scheme": args.scheme,
            }
            # Filter out None values so client uses its defaults/config loading
            filtered_kwargs = {k: v for k, v in client_kwargs.items() if v is not None}

            if args.config:
                # If a specific config file is given via --config, use from_config_file
                # We prioritize command-line args over the config file if both are present.
                # Let's use from_config_file first, then override with CLI args.
                try:
                    logger.info(
                        f"Loading configuration from specified file: {args.config}"
                    )
                    # Load base settings from the specified config file
                    base_client = QuestDBClient.from_config_file(args.config)

                    # Prepare overrides from CLI args (only if they were actually provided)
                    cli_overrides = {}
                    if args.host is not None:
                        cli_overrides["host"] = args.host
                    if args.port is not None:
                        cli_overrides["port"] = args.port
                    if args.user is not None:
                        cli_overrides["user"] = args.user
                    # Use actual_password which includes prompted/cli password if applicable
                    if args.user is not None or args.password is not None:
                        cli_overrides["password"] = actual_password
                    if args.timeout is not None:
                        cli_overrides["timeout"] = args.timeout
                    if args.scheme is not None:
                        cli_overrides["scheme"] = args.scheme

                    # Update the base client settings with CLI overrides
                    final_kwargs = {
                        "host": cli_overrides.get(
                            "host", base_client.base_url.split("://")[1].split(":")[0]
                        ),
                        "port": cli_overrides.get(
                            "port",
                            int(base_client.base_url.split(":")[-1].split("/")[0]),
                        ),
                        "user": cli_overrides.get(
                            "user", base_client.auth[0] if base_client.auth else None
                        ),
                        "password": cli_overrides.get(
                            "password",
                            base_client.auth[1] if base_client.auth else None,
                        ),
                        "timeout": cli_overrides.get("timeout", base_client.timeout),
                        "scheme": cli_overrides.get(
                            "scheme", base_client.base_url.split("://")[0]
                        ),
                    }
                    client = QuestDBClient(**final_kwargs)
                    logger.debug(
                        f"Client initialized from {args.config} and updated with CLI args."
                    )

                except FileNotFoundError:
                    logger.error(f"Config file not found: {args.config}")
                    sys.exit(1)
                except Exception as conf_err:
                    logger.error(f"Error loading config file {args.config}: {conf_err}")
                    sys.exit(1)

            else:
                # Standard initialization: uses CLI args > ~/.questdb-rest/config.json > defaults
                logger.debug(
                    "Initializing client using command-line arguments and default config."
                )
                client = QuestDBClient(**filtered_kwargs)

        except (QuestDBError, ValueError) as e:
            logger.error(f"Failed to initialize QuestDB client: {e}")
            sys.exit(1)
        except Exception as e:  # Catch other potential init errors
            logger.error(
                f"An unexpected error occurred during client initialization: {e}"
            )
            sys.exit(1)

    # Call the appropriate handler function
    try:
        # Pass the client instance (or None) and args to the handler
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
    # Setup IceCream (optional, for debugging convenience if installed)
    try:
        from icecream import install

        install()
    except ImportError:  # icecream not installed
        pass

    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        # Final fallback
        logger.exception(f"An unexpected error occurred at the top level: {e}")
        sys.exit(1)
