#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
# --------------------
# legacy uv run header
# --------------------
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
# --------------------
# imports
# --------------------
import uuid
import argparse
import html
from typing import Any, Dict, Union
import sys
import json
import logging
from getpass import getpass
from pathlib import Path
from typing import Callable, Tuple
import os  # ensure os is imported
import re
import argcomplete

# Import the client and exceptions from the library
from questdb_rest import (
    QuestDBClient,
    QuestDBError,
    QuestDBConnectionError,
    QuestDBAPIError,
    __version__,
    CLI_EPILOG,
)

_EXEC_EXTRACT_FIELD_SENTINEL = object()
# --- Configuration ---
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 9000  # Use the default from the library/docs
# -------------------
# --- Logging Setup ---
logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s: %(message)s", stream=sys.stderr
)
logger = logging.getLogger(__name__)
# ---------------------
# --- Table Name Generation Functions (Keep as is) ---


def get_table_name_from_stem(p: Path, **kwargs) -> str:
    """Default: returns the filename without extension."""
    return p.stem


def get_table_name_add_prefix(p: Path, prefix: str = "", **kwargs) -> str:
    """Returns the filename stem with a prefix added."""  # Removed default 'import_' here, rely on arg default or presence
    prefix_str = prefix
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


def simulate_drop(args, table_name, index, total):
    """Simulates the drop table command."""
    logger.info(f"[DRY-RUN] Simulating DROP TABLE ({index}/{total}):")
    logger.info(f"[DRY-RUN]   Target Table: '{table_name}'")
    # Quote name for simulation consistency
    safe_table_name = table_name.replace("'", "''")
    logger.info(f"[DRY-RUN]   Would execute: DROP TABLE '{safe_table_name}';")
    # Simulate DDL OK response
    print(
        json.dumps(
            {"dry_run": True, "table_dropped": table_name, "ddl": "OK (Simulated)"},
            indent=2,
        )
    )


def _get_schema_and_dedup_info(
    client: QuestDBClient, table_name: str, safe_table_name_quoted: str
) -> Dict[str, Any]:
    """Fetches and parses schema and deduplication info for a table."""
    result = {
        "exists": False,
        "is_dedup_enabled": False,
        "upsert_keys": None,
        "designated_timestamp": None,
        "create_statement": None,
        "error": None,
    }
    try:
        # 1. Check existence and current dedup status
        dedup_status_query = (
            f"SELECT dedup FROM tables() WHERE table_name = '{safe_table_name_quoted}'"
        )
        dedup_status_response = client.exec(query=dedup_status_query)
        if dedup_status_response.get("count", 0) == 0:
            result["error"] = "Table not found."
            return result
        result["exists"] = True
        result["is_dedup_enabled"] = dedup_status_response["dataset"][0][0]
        # 2. Fetch CREATE TABLE statement
        # Use double quotes for SHOW CREATE TABLE identifier as per QuestDB syntax
        safe_table_name_double_quoted = table_name.replace('"', '""')
        schema_query = f'SHOW CREATE TABLE "{safe_table_name_double_quoted}";'
        schema_response = client.exec(query=schema_query)
        if isinstance(schema_response, dict) and "error" in schema_response:
            raise QuestDBAPIError(
                f"Failed to fetch schema: {schema_response['error']}",
                response_data=schema_response,
            )
        if (
            isinstance(schema_response, dict)
            and schema_response.get("count", 0) > 0
            and (len(schema_response["dataset"]) > 0)
            and (len(schema_response["dataset"][0]) > 0)
        ):
            result["create_statement"] = schema_response["dataset"][0][0]
        else:
            result["error"] = "Could not retrieve CREATE TABLE statement."
            return result  # Cannot parse further
        # 3. Parse CREATE TABLE statement
        create_statement = result["create_statement"]
        # Parse Designated Timestamp (TIMESTAMP(...))
        ts_match = re.search(
            "TIMESTAMP\\s*\\(([^)]+)\\)", create_statement, re.IGNORECASE
        )
        if ts_match:
            # Strip potential quotes if column name was quoted, handle case
            result["designated_timestamp"] = ts_match.group(1).strip("\"`'")
        # Parse Upsert Keys (DEDUP UPSERT KEYS(...)) - only relevant if currently enabled
        if result["is_dedup_enabled"]:
            keys_match = re.search(
                "DEDUP UPSERT KEYS\\s*\\(([^)]+)\\)", create_statement, re.IGNORECASE
            )
            if keys_match:
                keys_str = keys_match.group(1)
                # Split by comma, strip whitespace and potential quotes
                result["upsert_keys"] = [
                    key.strip().strip("\"`'") for key in keys_str.split(",")
                ]
            else:
                # Should not happen if is_dedup_enabled is true, but handle defensively
                logger.warning(
                    f"Deduplication reported enabled for '{table_name}', but DEDUP clause not found in schema. Schema might be inconsistent or parsing failed."
                )
                result["upsert_keys"] = []  # Indicate keys couldn't be parsed
    except (QuestDBError, IndexError, KeyError, TypeError) as e:
        result["error"] = f"Failed to get table/schema info: {e}"
    return result


def handle_dedupe(args, client: QuestDBClient):
    """Handles the dedupe command for enabling/disabling/checking deduplication on multiple tables."""
    table_names_to_process = []
    source_description = ""
    # 1. Determine the source of table names (validation in get_args)
    if args.table_names:
        table_names_to_process = args.table_names
        source_description = "command line arguments"
    elif args.file:
        try:
            with open(args.file, "r", encoding="utf-8") as f:
                table_names_to_process = [
                    line.strip() for line in f if line.strip()
                ]  # Read non-empty lines
            source_description = f"file '{args.file}'"
        except IOError as e:
            logger.warning(f"Error reading table names file '{args.file}': {e}")
            sys.exit(1)
    elif not sys.stdin.isatty():
        logger.info("Reading table names from standard input (one per line)...")
        try:
            table_names_to_process = [
                line.strip() for line in sys.stdin if line.strip()
            ]  # Read non-empty lines
            source_description = "standard input"
        except Exception as e:
            logger.warning(f"Error reading table names from stdin: {e}")
            sys.exit(1)
    else:
        # This case should be caught by validation in get_args
        logger.error("Internal error: No table names source identified.")
        sys.exit(2)
    if not table_names_to_process:
        logger.warning(f"No valid table names found in {source_description}.")
        sys.exit(0)
    # Determine action
    action = "check"
    if args.enable:
        action = "enable"
    elif args.disable:
        action = "disable"
    logger.info(
        f"Processing {len(table_names_to_process)} table(s) from {source_description} for dedupe action '{action}'."
    )
    any_table_failed = False
    num_tables = len(table_names_to_process)
    json_separator = "\n"
    first_output_written = False
    for i, table_name in enumerate(table_names_to_process):
        logger.info(
            f"--- Processing dedupe action '{action}' for table {i + 1}/{num_tables}: '{table_name}' ---"
        )
        safe_table_name_quoted = table_name.replace(
            "'", "''"
        )  # Quote for queries like ALTER
        # --- Dry Run Check ---
        if args.dry_run:
            simulate_dedupe(args, table_name, i + 1, num_tables)
            if first_output_written:  # Print separator if not first dry-run output
                sys.stdout.write(json_separator)
            first_output_written = True
            continue  # Skip actual execution
        # --- Process Single Table ---
        try:
            # --- Get Current State (needed for check, enable validation) ---
            logger.debug(f"Fetching current status and schema for '{table_name}'...")
            table_info = _get_schema_and_dedup_info(
                client, table_name, safe_table_name_quoted
            )
            if table_info.get("error"):
                logger.error(
                    f"Error getting info for table '{table_name}': {table_info['error']}"
                )
                result = {
                    "status": "Error",
                    "table_name": table_name,
                    "action": action,
                    "message": table_info["error"],
                }
                if first_output_written:
                    sys.stdout.write(json_separator)
                print(json.dumps(result, indent=2))
                first_output_written = True
                any_table_failed = True
                if args.stop_on_error:
                    logger.warning(
                        "Stopping execution due to error (stop-on-error enabled)."
                    )
                    sys.exit(1)
                else:
                    continue  # Skip to next table
            is_dedup_enabled = table_info["is_dedup_enabled"]
            designated_ts_col = table_info["designated_timestamp"]
            current_upsert_keys = table_info["upsert_keys"]
            result = None  # Initialize result for this table
            # --- Perform Action ---
            if action == "enable":
                # Validation (keys provided globally via args, timestamp from schema)
                if not args.upsert_keys:
                    # Should be caught by get_args, but double-check
                    raise ValueError("--upsert-keys must be provided with --enable.")
                if not designated_ts_col:
                    raise ValueError(
                        f"Could not determine designated timestamp for table '{table_name}' from schema. Cannot enable."
                    )
                logger.info(
                    f"Designated timestamp column from schema: '{designated_ts_col}'"
                )
                provided_keys_lower = {k.lower() for k in args.upsert_keys}
                if designated_ts_col.lower() not in provided_keys_lower:
                    raise ValueError(
                        f"Designated timestamp column '{designated_ts_col}' must be included in --upsert-keys."
                    )
                keys_str = ", ".join(args.upsert_keys)
                enable_query = f"ALTER TABLE '{safe_table_name_quoted}' DEDUP ENABLE UPSERT KEYS({keys_str});"
                logger.debug(f"Executing: {enable_query}")
                response_json = client.exec(
                    query=enable_query, statement_timeout=args.statement_timeout
                )
                if isinstance(response_json, dict) and "error" in response_json:
                    error_msg = response_json["error"]
                    # Check specific errors like non-WAL
                    if "table is not WAL" in error_msg.lower():
                        message = f"Table is not WAL-enabled. {error_msg}"
                    else:
                        message = error_msg
                    raise QuestDBAPIError(
                        f"Failed to enable deduplication: {message}",
                        response_data=response_json,
                    )
                else:
                    logger.info(
                        f"Successfully enabled deduplication for '{table_name}' with keys: {args.upsert_keys}."
                    )
                    result = {
                        "status": "OK",
                        "table_name": table_name,
                        "action": "enable",
                        "deduplication_enabled": True,
                        "upsert_keys": args.upsert_keys,
                        "ddl": response_json.get("ddl")
                        if isinstance(response_json, dict)
                        else None,
                    }
            elif action == "disable":
                disable_query = f"ALTER TABLE '{safe_table_name_quoted}' DEDUP DISABLE;"
                logger.debug(f"Executing: {disable_query}")
                response_json = client.exec(
                    query=disable_query, statement_timeout=args.statement_timeout
                )
                if isinstance(response_json, dict) and "error" in response_json:
                    error_msg = response_json["error"]
                    if "table is not WAL" in error_msg.lower():
                        message = f"Table is not WAL-enabled. {error_msg}"
                    else:
                        message = error_msg
                    raise QuestDBAPIError(
                        f"Failed to disable deduplication: {message}",
                        response_data=response_json,
                    )
                else:
                    logger.info(
                        f"Successfully disabled deduplication for '{table_name}'."
                    )
                    result = {
                        "status": "OK",
                        "table_name": table_name,
                        "action": "disable",
                        "deduplication_enabled": False,
                        "ddl": response_json.get("ddl")
                        if isinstance(response_json, dict)
                        else None,
                    }
            elif action == "check":
                # Information already fetched
                # Only include keys if dedup is actually enabled and keys were parsed
                result = {
                    "status": "OK",
                    "table_name": table_name,
                    "action": "check",
                    "deduplication_enabled": is_dedup_enabled,
                    "designated_timestamp": designated_ts_col,
                    "upsert_keys": current_upsert_keys if is_dedup_enabled else None,
                }
                if is_dedup_enabled and current_upsert_keys is None:
                    result["warning"] = (
                        "Deduplication is enabled, but failed to parse UPSERT KEYS from schema."
                    )
                logger.info(f"Check Result - Deduplication enabled: {is_dedup_enabled}")
                if is_dedup_enabled:
                    logger.info(f"  Designated Timestamp: {designated_ts_col}")
                    logger.info(f"  Upsert keys: {current_upsert_keys}")
            # Print result for the current table
            if result:
                if first_output_written:
                    sys.stdout.write(json_separator)
                print(json.dumps(result, indent=2))
                first_output_written = True
        except (QuestDBAPIError, QuestDBError, ValueError) as e:
            logger.error(
                f"Error during dedupe '{action}' operation for '{table_name}': {e}"
            )
            error_details = getattr(e, "response_data", None)
            result = {
                "status": "Error",
                "table_name": table_name,
                "action": action,
                "message": str(e),
                "details": error_details,
            }
            if first_output_written:
                sys.stdout.write(json_separator)
            print(json.dumps(result, indent=2))
            first_output_written = True
            any_table_failed = True
            if args.stop_on_error:
                logger.warning(
                    "Stopping execution due to error (stop-on-error enabled)."
                )
                sys.exit(1)
            # else: continue to next table is implicit loop continuation
        except KeyboardInterrupt:
            logger.info("\nOperation cancelled by user.")
            sys.exit(130)
    # --- Final Exit Status ---
    if any_table_failed:
        logger.warning(f"One or more tables failed during dedupe '{action}' operation.")
        sys.exit(2)  # Indicate partial failure
    else:
        logger.info(f"All tables processed successfully for dedupe '{action}'.")
        sys.exit(0)


def simulate_dedupe(args, table_name, index, total):
    """Simulates the dedupe command for a single table."""
    logger.info(
        f"[DRY-RUN] Simulating dedupe operation ({index}/{total}) for table '{table_name}':"
    )
    action = "check"  # Default action
    if args.enable:
        action = "enable"
    elif args.disable:
        action = "disable"
    logger.info(f"[DRY-RUN]   Action: {action}")
    # Simulate fetching info (assume table exists and is basic WAL table for simulation)
    simulated_dedup_enabled = False  # Assume disabled initially for simulation clarity
    simulated_ts = "ts"  # Assume 'ts' as designated timestamp
    simulated_keys = None
    simulated_create_statement = f"CREATE TABLE '{table_name}' (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL;"
    logger.info("[DRY-RUN]   Simulating fetch of current status and schema...")
    logger.info(f"[DRY-RUN]     Assuming Dedup Enabled: {simulated_dedup_enabled}")
    logger.info(f"[DRY-RUN]     Assuming Designated Timestamp: {simulated_ts}")
    simulated_result = {
        "dry_run": True,
        "table_name": table_name,
        "action": action,
        "status": "OK (Simulated)",
    }  # Assume OK unless error
    if action == "enable":
        if not args.upsert_keys:
            logger.error("[DRY-RUN] Error: --upsert-keys are required for --enable.")
            simulated_result["status"] = "Error (Simulated)"
            simulated_result["message"] = "--upsert-keys missing"
        elif simulated_ts not in args.upsert_keys:
            logger.error(
                f"[DRY-RUN] Error: Designated timestamp '{simulated_ts}' not in provided keys {args.upsert_keys}."
            )
            simulated_result["status"] = "Error (Simulated)"
            simulated_result["message"] = (
                f"Designated timestamp '{simulated_ts}' missing from provided keys."
            )
        else:
            keys_str = ", ".join(args.upsert_keys)
            # Quote table name for ALTER statement
            safe_table_name_quoted = table_name.replace("'", "''")
            query = f"ALTER TABLE '{safe_table_name_quoted}' DEDUP ENABLE UPSERT KEYS({keys_str});"
            logger.info(f"[DRY-RUN]   Would execute: {query}")
            simulated_result["upsert_keys_set"] = args.upsert_keys
            simulated_result["ddl"] = "OK (Simulated)"
            simulated_result["message"] = "Deduplication would be enabled."
    elif action == "disable":
        # Quote table name for ALTER statement
        safe_table_name_quoted = table_name.replace("'", "''")
        query = f"ALTER TABLE '{safe_table_name_quoted}' DEDUP DISABLE;"
        logger.info(f"[DRY-RUN]   Would execute: {query}")
        simulated_result["ddl"] = "OK (Simulated)"
        simulated_result["message"] = "Deduplication would be disabled."
    elif action == "check":
        logger.info(
            "[DRY-RUN]   Simulating check result based on assumed initial state."
        )  # Will be None if simulated_dedup_enabled is False
        simulated_result["deduplication_enabled"] = simulated_dedup_enabled
        simulated_result["designated_timestamp"] = simulated_ts
        simulated_result["upsert_keys"] = simulated_keys
        simulated_result["message"] = (
            f"Checked status (Simulated: enabled={simulated_dedup_enabled})."
        )
    print(json.dumps(simulated_result, indent=2))


def simulate_rename(args, client):
    """Simulates the rename command, including backup logic."""
    old_name = args.old_table_name
    new_name = args.new_table_name
    safe_old_name = old_name.replace("'", "''")
    safe_new_name = new_name.replace("'", "''")
    logger.info("[DRY-RUN] Simulating rename operation:")
    logger.info(f"[DRY-RUN]   From: '{old_name}'")
    logger.info(f"[DRY-RUN]   To:   '{new_name}'")
    # Simulate checking if the *new* table name exists
    logger.info(
        f"[DRY-RUN]   1. Checking if target table '{new_name}' exists... (Assuming Yes for backup simulation)"
    )
    new_table_exists = True  # Assume exists to simulate backup path
    backup_name = None
    if new_table_exists:
        if args.no_backup_if_new_table_exists:
            logger.info(
                f"[DRY-RUN]   2. --no-backup-if-new-table-exists specified. Original table '{new_name}' will NOT be backed up. Rename might fail."
            )
        else:
            backup_name = f"qdb_cli_backup_{new_name}_{uuid.uuid4()}".replace("-", "_")[
                :250
            ]
            safe_backup_name = backup_name.replace("'", "''")
            logger.info(
                f"[DRY-RUN]   2. Target table '{new_name}' exists. Will attempt backup."
            )
            logger.info(f"[DRY-RUN]      Generated backup name: '{backup_name}'")
            # Simulate check if backup name exists
            logger.info(
                f"[DRY-RUN]      Checking if backup table '{backup_name}' exists... (Assuming No)"
            )
            # Simulate backup rename
            logger.info(
                f"[DRY-RUN]      Would execute: RENAME TABLE '{safe_new_name}' TO '{safe_backup_name}';"
            )
    else:
        logger.info(
            f"[DRY-RUN]   2. Target table '{new_name}' does not exist. No backup needed."
        )
    # Simulate the final rename
    logger.info("[DRY-RUN]   3. Renaming original table to target name.")
    logger.info(
        f"[DRY-RUN]      Would execute: RENAME TABLE '{safe_old_name}' TO '{safe_new_name}';"
    )
    # Output simulated success response
    result = {
        "dry_run": True,
        "operation": "rename",
        "status": "OK (Simulated)",
        "old_name": old_name,
        "new_name": new_name,
        "backup_of_new_name": backup_name
        if new_table_exists and (not args.no_backup_if_new_table_exists)
        else None,
    }
    print(json.dumps(result, indent=2))
    sys.exit(0)


# Update simulate_create_or_replace to reflect the new workflow


def simulate_create_or_replace(args, query):
    """Simulates the create-or-replace-table-from-query command (temp table workflow)."""
    target_table = args.table
    temp_table_name = f"__qdb_cli_temp_{target_table}_{uuid.uuid4()}".replace("-", "_")[
        :250
    ]  # Generate temp name
    logger.info(
        "[DRY-RUN] Simulating create-or-replace-table-from-query (using temp table):"
    )
    logger.info(f"[DRY-RUN]   Target Table: '{target_table}'")
    logger.info("[DRY-RUN]   Query Source: (provided via args/stdin)")  # Simplification
    logger.info(f"[DRY-RUN]   Temporary Table Name: '{temp_table_name}'")
    # Simulate CREATE TEMP TABLE statement construction
    create_parts = [
        f"CREATE TABLE {temp_table_name} AS ({query})"
    ]  # Use unquoted temp name
    if args.timestamp:
        create_parts.append(
            f"TIMESTAMP({args.timestamp})"
        )  # Assuming simple timestamp col name
    if args.partitionBy:
        create_parts.append(f"PARTITION BY {args.partitionBy}")
    # Simulate adding DEDUP clause if upsert keys are provided
    if args.upsert_keys:
        # Basic simulation validation: check if timestamp is included
        ts_col = args.timestamp
        if ts_col and ts_col not in args.upsert_keys:
            logger.warning(
                f"[DRY-RUN] Warning: Designated timestamp '{ts_col}' is not included in provided --upsert-keys {args.upsert_keys}. The actual command might fail."
            )
        keys_str = ", ".join(args.upsert_keys)
        create_parts.append(f"DEDUP UPSERT KEYS({keys_str})")
        logger.info(f"[DRY-RUN]   Including DEDUP UPSERT KEYS: {keys_str}")
    create_statement = " ".join(create_parts) + ";"
    logger.info(f"[DRY-RUN]   1. Would execute: {create_statement}")
    # Simulate checking if target table exists (assume it exists for backup simulation)
    logger.info(
        f"[DRY-RUN]   2. Checking if target table '{target_table}' exists... (Assuming Yes)"
    )
    original_exists = True  # Assume exists for simulation
    backup_name = None
    if original_exists:
        if args.no_backup_original_table:
            logger.info(
                f"[DRY-RUN]   3. --no-backup-original-table specified. Would DROP original table '{target_table}'."
            )
            # Use correct quoting (single quotes) for DROP TABLE identifier
            logger.info(f"[DRY-RUN]      Would execute: DROP TABLE '{target_table}';")
        else:
            if args.backup_table_name:
                backup_name = args.backup_table_name
                logger.info(
                    f"[DRY-RUN]   3. Using provided backup name: '{backup_name}'"
                )
            else:
                backup_name = f"qdb_cli_backup_{target_table}_{uuid.uuid4()}"[
                    :250
                ].replace("-", "_")
                logger.info(f"[DRY-RUN]   3. Generated backup name: '{backup_name}'")
            # Simulate checking if backup name exists (assume it doesn't)
            logger.info(
                f"[DRY-RUN]      Checking if backup table '{backup_name}' exists... (Assuming No)"
            )
            # Use correct quoting (single quotes) for RENAME TABLE identifiers
            logger.info(
                f"[DRY-RUN]      Would execute: RENAME TABLE '{target_table}' TO '{backup_name}';"
            )
    else:
        logger.info(
            f"[DRY-RUN]   3. Target table '{target_table}' does not exist. No backup/drop needed."
        )
    # Simulate final RENAME
    logger.info("[DRY-RUN]   4. Renaming temporary table to target table.")
    # Use correct quoting (single quotes) for RENAME TABLE identifiers
    logger.info(
        f"[DRY-RUN]      Would execute: RENAME TABLE '{temp_table_name}' TO '{target_table}';"
    )
    # Simulate success response
    # Add info about keys
    print(
        json.dumps(
            {
                "dry_run": True,
                "operation": "create_or_replace_table_from_query",
                "workflow": "temporary_table",
                "target_table": target_table,
                "upsert_keys_specified": args.upsert_keys,
                "status": "OK (Simulated)",
                "backup_table": backup_name
                if original_exists and (not args.no_backup_original_table)
                else None,
                "original_dropped_no_backup": original_exists
                and args.no_backup_original_table,
            },
            indent=2,
        )
    )


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
    if args.fmt == "json":  # Cannot simulate columns without parsing file
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
                    "columns": [],
                },
                indent=2,
            )
        )
    else:  # tabular
        print(f"+--- [DRY-RUN] Import Simulation for {table_name} ---+")
        print("| Status: OK (Simulated)")
        print("+----------------------------------------------------+")


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
    # Modify simulation if --extract-field is used
    if args.extract_field:
        field_arg = _get_real_extract_field(args)
        logger.info(f"[DRY-RUN]   Mode: Extract Field ('{field_arg}')")
        # Simulate extracted output (one value per line)
        print("simulated_extracted_value_1")
        if not args.one:
            print("simulated_extracted_value_2")
    else:
        logger.info("[DRY-RUN]   Mode: Standard Exec")
        logger.info(f"[DRY-RUN]   Params: {filtered_params}")
        headers = {}
        if args.statement_timeout:
            headers["Statement-Timeout"] = str(args.statement_timeout)
            logger.info(f"[DRY-RUN]   Headers: {headers}")
        # Simulate different outputs based on formatting flags
        if args.markdown or args.psql:
            fmt = "psql" if args.psql else "github"
            logger.info(f"[DRY-RUN]   Output: Simulated table ({fmt} format)")
            print("+-------------------+-------------------+")
            print("| dry_run_col1      | dry_run_col2      |")
            print("|-------------------+-------------------|")
            print("| simulated_val1    | simulated_val2    |")
            print("+-------------------+-------------------+")
        elif args.one:
            logger.info("[DRY-RUN]   Output: Simulated single value")
            print("simulated_single_value")
        else:
            # Default: Simulate a DDL OK response or simple JSON
            logger.info("[DRY-RUN]   Output: Simulated JSON")
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


def simulate_chk(args, table_name, index, total):
    """Simulates the chk command for a single table."""
    logger.info(f"[DRY-RUN] Simulating /chk request ({index}/{total}):")
    logger.info(f"[DRY-RUN]   Table Name: '{table_name}'")
    params = {"f": "json", "j": table_name, "version": "2"}
    logger.info(f"[DRY-RUN]   Params: {params}")
    # Simulate 'Exists' for predictability in dry-run
    print(
        json.dumps(
            {"dry_run": True, "tableName": table_name, "status": "Exists (Simulated)"},
            indent=2,
        )
    )


def simulate_schema(args, table_name, index, total):
    """Simulates the schema command for a single table."""
    logger.info(
        f"[DRY-RUN] Simulating schema fetch ({index}/{total}) for table '{table_name}':"
    )
    # Use double quotes for SHOW CREATE TABLE identifier
    safe_table_name_double_quoted = table_name.replace('"', '""')
    query = f'SHOW CREATE TABLE "{safe_table_name_double_quoted}";'
    logger.info(f"[DRY-RUN]   Would execute query: {query}")
    # Simulate the output format: just the CREATE TABLE statement string
    print(
        f'CREATE TABLE "{safe_table_name_double_quoted}" (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY; -- (Simulated)'
    )


# --- Command Handlers (Refactored to use QuestDBClient) ---


def handle_imp(args, client: QuestDBClient):
    """Handles the /imp (import) command using the client."""
    any_file_failed = False
    num_files = len(args.files)
    json_separator = "\n"
    # Process the shortcut flag for table name derivation
    if args.derive_table_name_from_filename_stem_and_replace_dash_with_underscore:
        # If the shortcut flag is set, configure its component options
        args.name_func = "stem"
        args.dash_to_underscore = True
        logger.info(
            "Using shortcut flag: Setting name_func=stem and dash_to_underscore=True"
        )
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
                        continue  # Skip this file  # Assign derived name as the final name for now
                final_table_name = derived_table_name
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
                )  # Pass filename explicitly
                # Pass prepared schema string
                # Pass prepared schema file obj
                # Use the final calculated name
                response = client.imp(
                    data_file_obj=data_file_obj_for_request,
                    data_file_name=file_path.name,
                    schema_json_str=schema_content,
                    schema_file_obj=schema_file_obj,
                    table_name=final_table_name,
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
                    if response_text and (not response_text.endswith("\n")):
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


def _get_real_extract_field(args: argparse.Namespace) -> Union[str, int]:
    if args.extract_field is _EXEC_EXTRACT_FIELD_SENTINEL:
        # extract first field if -x used but not specified
        field_arg = 0
    else:
        field_arg = args.extract_field
    return field_arg


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
    # Determine the separator based on the output format requested
    # Use newline for JSON, extracted fields, and --one
    # Use double newline for markdown/psql
    # No separator needed if only one statement
    output_separator = ""

    if len(statements) > 1:
        if args.markdown or (args.psql and (not args.extract_field)):
            output_separator = "\n\n"
        else:
            output_separator = "\n"
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
            f"Statement: {statement[:100]}{('...' if len(statement) > 100 else '')}"
        )
        # --- Dry Run Check ---
        if args.dry_run:
            simulate_exec(args, statement, i + 1, len(statements))
            if first_output_written:  # Print separator if not first dry-run statement
                sys.stdout.write(output_separator)
            first_output_written = True
            continue  # Skip actual execution
        try:
            # --- Choose execution method ---
            response_data = None
            if args.extract_field:
                field_arg = _get_real_extract_field(args)
                # Convert field name to int if it looks like one
                try:
                    field_identifier: Union[str, int] = int(field_arg)
                    logger.debug(f"Using field index: {field_identifier}")
                except ValueError:
                    field_identifier = field_arg
                    logger.debug(f"Using field name: {field_identifier}")
                response_data = client.exec_extract_field(
                    query=statement,
                    field=field_identifier,
                    limit=args.limit,
                    nm=args.nm,
                    quote_large_num=args.quoteLargeNum,
                    statement_timeout=args.statement_timeout,
                )
                # Check for errors within the extraction process (already logged by client)
                # The client's exec_extract_field raises QuestDBError on failure
            else:
                # Standard execution returning JSON dict
                response_data = client.exec(
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
                if isinstance(response_data, dict) and "error" in response_data:
                    logger.error(
                        f"Error executing statement {i + 1}: {response_data['error']}"
                    )
                    # Print simplified error to stderr
                    sys.stderr.write(
                        f"-- Statement {i + 1} Error --\nError: {response_data['error']}\n"
                    )
                    sys.stderr.write(
                        f"Query: {response_data.get('query', statement)}\n"
                    )
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
            if args.extract_field:
                # Response is a list of values
                if isinstance(response_data, list):
                    if args.one:
                        if response_data:
                            print(response_data[0])
                            output_written_this_statement = True
                        else:
                            logger.debug(
                                f"Statement {i + 1}: --extract-field and --one specified, but result list was empty."
                            )
                    else:
                        for value in response_data:
                            print(value)
                        output_written_this_statement = bool(response_data)
                else:
                    logger.error(
                        f"Statement {i + 1}: Expected a list from exec_extract_field, but got {type(response_data)}."
                    )
                    # Treat this as a failure
                    any_statement_failed = True
                    if args.stop_on_error:
                        sys.exit(1)
                    else:
                        continue
            elif args.explain_only:
                if isinstance(response_data, dict) and "dataset" in response_data:
                    explain_text = explain_output_to_text(response_data)
                    sys.stdout.write(explain_text + "\n")
            elif args.one:
                if isinstance(response_data, dict) and "dataset" in response_data:
                    if (
                        len(response_data["dataset"]) > 0
                        and len(response_data["dataset"][0]) > 0
                    ):
                        sys.stdout.write(f"{response_data['dataset'][0][0]}\n")
                        output_written_this_statement = True
                    else:
                        logger.debug(
                            f"Statement {i + 1}: --one specified, but dataset was empty or lacked rows/columns."
                        )
                else:
                    logger.debug(
                        f"Statement {i + 1}: --one specified, but response was not a dict or lacked 'dataset'."
                    )
            elif (args.markdown or args.psql) and isinstance(response_data, dict):
                if "columns" in response_data and "dataset" in response_data:
                    try:
                        from tabulate import tabulate

                        headers = [col["name"] for col in response_data["columns"]]
                        table = response_data["dataset"]
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
                    except ImportError:
                        sys.stderr.write(
                            "Tabulate library not installed. Please install 'tabulate'. Falling back to JSON.\n"
                        )
                        # Fallback to JSON dump if tabulate is missing
                        json.dump(response_data, sys.stdout, indent=2)
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
                        json.dump(response_data, sys.stdout, indent=2)
                        sys.stdout.write("\n")
                        output_written_this_statement = True
                else:
                    # Handle cases like simple DDL OK response when markdown/psql is requested
                    logger.debug(
                        f"Statement {i + 1}: --markdown/psql requested, but response lacks 'columns' or 'dataset'. Printing raw JSON."
                    )
                    json.dump(response_data, sys.stdout, indent=2)
                    sys.stdout.write("\n")
                    output_written_this_statement = True
            elif isinstance(response_data, dict):
                # Default: JSON output for non-DDL responses
                # Only print JSON if it's not just a simple DDL response (like {'ddl': 'OK'})
                # unless it's the *only* thing in the response.
                if not (
                    len(response_data) == 1
                    and "ddl" in response_data
                    and (response_data["ddl"] == "OK")
                ):
                    json.dump(response_data, sys.stdout, indent=2)
                    sys.stdout.write("\n")
                    output_written_this_statement = True
                else:
                    logger.debug(
                        f"Statement {i + 1}: Suppressing simple DDL OK response in default JSON output."
                    )
            else:
                # Fallback for unexpected response types
                logger.warning(
                    f"Statement {i + 1}: Received unexpected response data type {type(response_data)}. Printing representation."
                )
                print(repr(response_data))
                output_written_this_statement = True
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
        except QuestDBError as e:  # Catch other client errors (connection, extraction)
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
        # Tell client to stream if writing to file
        response = client.exp(
            query=args.query,
            limit=args.limit,
            nm=args.nm,
            stream_response=stream_enabled,
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
            if output_text and (not output_text.endswith("\n")):
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
    """Handles the /chk command using the client for multiple tables."""
    table_names_to_check = []
    source_description = ""
    # 1. Determine the source of table names (validation in get_args)
    if args.table_names:
        table_names_to_check = args.table_names
        source_description = "command line arguments"
    elif args.file:
        try:
            with open(args.file, "r", encoding="utf-8") as f:
                table_names_to_check = [
                    line.strip() for line in f if line.strip()
                ]  # Read non-empty lines
            source_description = f"file '{args.file}'"
        except IOError as e:
            logger.warning(f"Error reading table names file '{args.file}': {e}")
            sys.exit(1)
    elif not sys.stdin.isatty():
        logger.info("Reading table names from standard input (one per line)...")
        try:
            table_names_to_check = [
                line.strip() for line in sys.stdin if line.strip()
            ]  # Read non-empty lines
            source_description = "standard input"
        except Exception as e:
            logger.warning(f"Error reading table names from stdin: {e}")
            sys.exit(1)
    else:
        # This case should be caught by validation in get_args
        logger.error("Internal error: No table names source identified.")
        sys.exit(2)
    if not table_names_to_check:
        logger.warning(f"No valid table names found in {source_description}.")
        sys.exit(0)
    logger.info(
        f"Checking existence for {len(table_names_to_check)} table(s) from {source_description}."
    )
    any_check_failed = False
    num_tables = len(table_names_to_check)
    json_separator = "\n"
    first_output_written = False
    for i, table_name in enumerate(table_names_to_check):
        logger.info(f"--- Checking table {i + 1}/{num_tables}: '{table_name}' ---")
        # --- Dry Run Check ---
        if args.dry_run:
            simulate_chk(args, table_name, i + 1, num_tables)
            if first_output_written:  # Print separator if not first dry-run output
                sys.stdout.write(json_separator)
            first_output_written = True
            continue  # Skip actual execution
        try:
            exists = client.table_exists(table_name)
            status_message = "Exists" if exists else "Does not exist"
            logger.info(f"Result: Table '{table_name}' {status_message.lower()}.")
            # Output consistent JSON to stdout
            result = {"tableName": table_name, "status": status_message}
            if first_output_written:
                sys.stdout.write(json_separator)
            print(json.dumps(result, indent=2))
            first_output_written = True
            # Note: We don't exit based on existence here, only on errors.
        except QuestDBAPIError as e:
            logger.error(f"API Error checking table '{table_name}': {e}")
            result = {
                "tableName": table_name,
                "status": "Error",
                "detail": str(e),
                "api_details": e.response_data,
            }
            if first_output_written:
                sys.stdout.write(json_separator)
            print(json.dumps(result, indent=2))
            first_output_written = True
            any_check_failed = True
            if args.stop_on_error:
                logger.warning(
                    "Stopping execution due to error (stop-on-error enabled)."
                )
                sys.exit(1)
            # else: continue to next table is implicit loop continuation
        except QuestDBError as e:
            logger.error(f"Error checking table '{table_name}': {e}")
            result = {"tableName": table_name, "status": "Error", "detail": str(e)}
            if first_output_written:
                sys.stdout.write(json_separator)
            print(json.dumps(result, indent=2))
            first_output_written = True
            any_check_failed = True
            if args.stop_on_error:
                logger.warning(
                    "Stopping execution due to error (stop-on-error enabled)."
                )
                sys.exit(1)
            # else: continue to next table is implicit loop continuation
        except KeyboardInterrupt:
            logger.info("\nOperation cancelled by user.")
            sys.exit(130)
    # --- Final Exit Status ---
    if any_check_failed:
        logger.warning("One or more table checks failed.")
        sys.exit(2)  # Indicate partial failure if stop-on-error was false
    else:
        logger.info("All requested tables checked successfully.")
        sys.exit(0)  # Exit 0 on success, regardless of existence results


def handle_drop(args, client: QuestDBClient):
    """Handles the drop/drop-table command using the client's exec method."""
    table_names_to_drop = []
    source_description = ""
    # 1. Determine the source of table names and load them
    # (Validation moved to get_args)
    if args.table_names:
        table_names_to_drop = args.table_names
        source_description = "command line arguments"
    elif args.file:
        try:
            with open(args.file, "r", encoding="utf-8") as f:
                table_names_to_drop = [
                    line.strip() for line in f if line.strip()
                ]  # Read non-empty lines
            source_description = f"file '{args.file}'"
        except IOError as e:
            logger.warning(f"Error reading table names file '{args.file}': {e}")
            sys.exit(1)
    elif not sys.stdin.isatty():
        logger.info("Reading table names from standard input (one per line)...")
        try:
            table_names_to_drop = [
                line.strip() for line in sys.stdin if line.strip()
            ]  # Read non-empty lines
            source_description = "standard input"
        except Exception as e:  # Catch potential errors during stdin read
            logger.warning(f"Error reading table names from stdin: {e}")
            sys.exit(1)
    else:
        logger.warning(
            "No table names provided via arguments, --file, or standard input."
        )
        # Use exit code 2 for usage errors
        sys.exit(2)
    if not table_names_to_drop:
        logger.warning(f"No valid table names found in {source_description}.")
        sys.exit(0)
    logger.info(
        f"Found {len(table_names_to_drop)} table(s) to drop from {source_description}."
    )
    # 2. Process the tables
    any_real_table_failed = False  # Track actual failures, not "not exists"
    num_tables = len(table_names_to_drop)
    json_separator = "\n"
    first_output_written = False
    for i, table_name in enumerate(table_names_to_drop):
        logger.info(
            f"--- Processing DROP for table {i + 1}/{num_tables}: '{table_name}' ---"
        )
        table_skipped_not_exist = False  # Flag for this specific table
        # --- Dry Run Check ---
        if args.dry_run:
            simulate_drop(args, table_name, i + 1, num_tables)
            if first_output_written:  # Print separator if not first dry-run statement
                sys.stdout.write(json_separator)
            first_output_written = True
            continue  # Skip actual execution
        # Quote the table name for safety - QuestDB uses single quotes for table names in DROP
        safe_table_name = table_name.replace("'", "''")  # Basic escaping
        query = f"DROP TABLE '{safe_table_name}';"
        try:
            logger.info(f"Executing: {query}")
            response_json = client.exec(
                query=query, statement_timeout=args.statement_timeout
            )
            # Check for errors within the JSON response
            if isinstance(response_json, dict) and "error" in response_json:
                error_msg = response_json["error"]
                # Check if the error is "table does not exist"
                if "table does not exist" in error_msg.lower():
                    logger.warning(
                        f"Table '{table_name}' does not exist, skipping drop."
                    )
                    table_skipped_not_exist = True
                    # Print a specific JSON status for this
                    result = {
                        "status": "Skipped",
                        "table_name": table_name,
                        "message": f"Table '{table_name}' does not exist.",
                        "error_details": error_msg,
                    }
                    if first_output_written:
                        sys.stdout.write(json_separator)
                    print(json.dumps(result, indent=2))
                    first_output_written = True
                    # Continue to the next table without marking as failure
                    continue
                else:
                    # Handle other errors as real failures
                    logger.error(f"Error dropping table '{table_name}': {error_msg}")
                    sys.stderr.write(
                        f"-- Error dropping table '{table_name}' --\nError: {error_msg}\n"
                    )
                    sys.stderr.write(f"Query: {response_json.get('query', query)}\n")
                    any_real_table_failed = True  # Mark as a real failure
                    if args.stop_on_error:
                        logger.warning(
                            "Stopping execution due to error (stop-on-error enabled)."
                        )
                        sys.exit(1)
                    else:
                        logger.warning("Continuing execution (stop-on-error disabled).")
                        continue  # Skip to next table
            # --- Success Case ---
            # Print separator if this is not the first successful output
            if first_output_written:
                sys.stdout.write(json_separator)
            # Print confirmation JSON
            # Include DDL response if present
            result = {
                "status": "OK",
                "table_dropped": table_name,
                "message": f"Table '{table_name}' dropped successfully.",
                "ddl_response": response_json.get("ddl")
                if isinstance(response_json, dict)
                else None,
            }
            print(json.dumps(result, indent=2))
            first_output_written = True
            logger.info(f"Table '{table_name}' dropped successfully.")
        except QuestDBAPIError as e:
            # Check if the API error itself indicates "table does not exist"
            error_msg = str(e)
            if "table does not exist" in error_msg.lower():
                logger.warning(
                    f"Table '{table_name}' does not exist (API Error), skipping drop."
                )
                table_skipped_not_exist = True
                # Print a specific JSON status for this
                result = {
                    "status": "Skipped",
                    "table_name": table_name,
                    "message": f"Table '{table_name}' does not exist.",
                    "error_details": error_msg,
                }
                if first_output_written:
                    sys.stdout.write(json_separator)
                print(json.dumps(result, indent=2))
                first_output_written = True
                # Continue to the next table without marking as failure
                continue
            else:
                # Handle other API errors as real failures
                logger.warning(
                    f"Dropping table '{table_name}' failed with API error: {e}"
                )
                sys.stderr.write(
                    f"-- Error dropping table '{table_name}' --\nError: {e}\n"
                )
                any_real_table_failed = True  # Mark as a real failure
                if args.stop_on_error:
                    logger.warning(
                        "Stopping execution due to API error (stop-on-error enabled)."
                    )
                    sys.exit(1)
                else:
                    logger.warning("Continuing execution (stop-on-error disabled).")
        except QuestDBError as e:  # Catch other client errors (connection etc.)
            logger.warning(f"Dropping table '{table_name}' failed: {e}")
            sys.stderr.write(f"-- Error dropping table '{table_name}' --\nError: {e}\n")
            any_real_table_failed = True  # Mark as a real failure
            if args.stop_on_error:
                logger.warning(
                    "Stopping execution due to error (stop-on-error enabled)."
                )
                sys.exit(1)
            else:
                logger.warning("Continuing execution (stop-on-error disabled).")
        except KeyboardInterrupt:
            logger.info(
                f"\nOperation cancelled by user while dropping table '{table_name}'."
            )
            sys.exit(130)
    # --- Final Exit Status ---
    if any_real_table_failed:
        logger.warning(
            "One or more tables failed to drop (excluding 'table does not exist')."
        )
        sys.exit(2)  # Indicate partial failure if stop-on-error was false
    else:
        logger.info("All requested tables processed for dropping.")
        sys.exit(0)  # Exit 0 if only "not exists" skips occurred or all succeeded


def handle_create_or_replace_table_from_query(args, client: QuestDBClient):
    """
    Handles the create-or-replace-table-from-query command.
    Uses the temporary table workflow: Create Temp -> Rename/Drop Original -> Rename Temp.
    """
    import importlib  # For query from module

    target_table = args.table
    # Generate a unique temporary table name unlikely to collide
    # Replace hyphens from uuid as they might not be valid in unquoted identifiers
    temp_table_name = f"__qdb_cli_temp_{target_table}_{uuid.uuid4()}".replace("-", "_")
    temp_table_name = temp_table_name[:250]  # Ensure within length limits
    # Quote the temp table name for safety in RENAME/DROP later
    safe_temp_table_name_quoted = temp_table_name.replace("'", "''")
    logger.info(
        f"Starting create-or-replace operation for table '{target_table}' using temp table '{temp_table_name}'..."
    )
    # --- 1. Get SQL Query Content ---
    sql_content = ""
    source_description = ""
    # Logic copied and adapted from handle_exec to get query input
    # [ ... same query input logic as before ... ]
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
            sys.path.append(str(Path.cwd()))
            logger.info(f"Importing module: {module_spec}")
            mod = importlib.import_module(module_spec)
            query_str = getattr(mod, var_name, None)
            if not isinstance(query_str, str):
                logger.error("The specified variable from module is not a string.")
                sys.exit(1)
            sql_content = query_str
            source_description = args.get_query_from_python_module
            logger.info(f"Loaded SQL from module variable: {source_description}")
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
    # Extract the *single* defining query
    try:
        statements = extract_statements_from_sql(sql_content)
        if len(statements) == 0:
            raise ValueError("No SQL statements found in input.")
        if len(statements) > 1:
            logger.warning(
                f"Multiple SQL statements found in {source_description}. Only the first statement will be used for CREATE TABLE AS."
            )
        defining_query = statements[0]
        # --- VALIDATION (Optional but recommended): Check if it looks like SELECT ---
        # This is informational now, as we trust the user's assertion about syntax
        if not defining_query.strip().lower().startswith("select"):
            logger.warning(
                f"Input query from {source_description} does not start with SELECT. Assuming it's valid QuestDB shorthand."
            )
            logger.warning(f"Query: {defining_query}")
        # --- End Validation ---
        logger.info(f"Using query from {source_description} for table creation.")
        logger.debug(
            f"Query: {defining_query[:100]}{('...' if len(defining_query) > 100 else '')}"
        )
    except Exception as e:
        logger.error(f"Failed to parse SQL from {source_description}: {e}")
        sys.exit(1)
    # --- Dry Run Check ---
    if args.dry_run:
        simulate_create_or_replace(args, defining_query)  # Use updated simulation
        sys.exit(0)
    # --- Actual Execution ---
    original_exists = False
    backup_name = None
    safe_backup_name_quoted = None  # Store quoted backup name if created
    backup_created = False
    original_dropped_no_backup = False
    temp_table_created = False
    # Quote the target table name for use in RENAME/DROP
    safe_target_table_quoted = target_table.replace("'", "''")
    # --- Cleanup function in case of errors ---

    def cleanup_temp_table():
        if temp_table_created:
            logger.warning(
                f"Attempting to clean up temporary table '{temp_table_name}'..."
            )
            try:
                # Use single quotes for DROP identifier
                drop_temp_query = f"DROP TABLE '{safe_temp_table_name_quoted}';"
                response = client.exec(
                    query=drop_temp_query, statement_timeout=args.statement_timeout
                )
                if isinstance(response, dict) and "error" in response:
                    logger.error(
                        f"Cleanup failed: Could not drop temporary table '{temp_table_name}': {response['error']}"
                    )
                else:
                    logger.info(
                        f"Successfully cleaned up temporary table '{temp_table_name}'."
                    )
            except Exception as cleanup_err:
                logger.error(
                    f"Cleanup failed: Error dropping temporary table '{temp_table_name}': {cleanup_err}"
                )

    try:
        # --- 2. Create Temporary Table ---
        logger.info(f"Creating temporary table '{temp_table_name}' from query...")
        # don't quote, must use literal
        safe_timestamp_col = args.timestamp if args.timestamp else None
        create_parts = [
            f"CREATE TABLE {temp_table_name} AS ({defining_query})"
        ]  # Use unquoted temp table name
        if safe_timestamp_col:
            # Validate timestamp is in upsert keys if provided
            if args.upsert_keys and safe_timestamp_col not in args.upsert_keys:
                raise ValueError(
                    f"Designated timestamp column '{safe_timestamp_col}' must be included in --upsert-keys: {args.upsert_keys}"
                )
            create_parts.append(f"TIMESTAMP({safe_timestamp_col})")
        if args.partitionBy:
            create_parts.append(f"PARTITION BY {args.partitionBy}")
        # Add DEDUP clause if upsert keys are provided
        if args.upsert_keys:
            # Validation already done above if timestamp provided
            keys_str = ", ".join(args.upsert_keys)
            create_parts.append(f"DEDUP UPSERT KEYS({keys_str})")
            logger.info(f"Including DEDUP UPSERT KEYS: {keys_str}")
        create_query = " ".join(create_parts) + ";"
        logger.debug(f"Executing CREATE TEMP statement: {create_query}")
        try:
            response = client.exec(
                query=create_query, statement_timeout=args.statement_timeout
            )
            if isinstance(response, dict) and "error" in response:
                error_detail = response["error"]
                if "query" in response:
                    error_detail += f" (Query: {response['query']})"
                # Check specific errors like non-WAL
                if "table must be WAL" in error_detail.lower():
                    raise QuestDBAPIError(
                        f"Failed to create temporary table: DEDUP requires table to be WAL. Ensure the base table properties support WAL or remove --upsert-keys. Error: {error_detail}",
                        response_data=response,
                    )
                else:
                    raise QuestDBAPIError(
                        f"Failed to create temporary table: {error_detail}",
                        response_data=response,
                    )
            logger.info(f"Successfully created temporary table '{temp_table_name}'.")
            temp_table_created = True
        except (
            QuestDBError,
            ValueError,
        ) as create_err:  # Catch ValueError from validation
            logger.error(
                f"Error creating temporary table '{temp_table_name}': {create_err}"
            )
            # No cleanup needed here as temp table wasn't created
            sys.exit(1)
        # --- 3. Check if target table exists ---
        logger.info(f"Checking if target table '{target_table}' exists...")
        original_exists = client.table_exists(target_table)
        # --- 4. Handle Existing Table (Backup/Drop) ---
        rename_original_failed = False
        if original_exists:
            if args.no_backup_original_table:
                # Drop the original table directly
                logger.info(
                    f"--no-backup-original-table specified. Dropping original table '{target_table}'..."
                )
                # Use single quotes for DROP identifier
                drop_query = f"DROP TABLE '{safe_target_table_quoted}';"
                try:
                    response = client.exec(
                        query=drop_query, statement_timeout=args.statement_timeout
                    )
                    if isinstance(response, dict) and "error" in response:
                        error_detail = response["error"]
                        if "query" in response:
                            error_detail += f" (Query: {response['query']})"
                        raise QuestDBAPIError(
                            f"Failed to drop original table: {error_detail}"
                        )
                    logger.info(
                        f"Successfully dropped original table '{target_table}'."
                    )
                    original_dropped_no_backup = True
                except QuestDBError as e:
                    logger.error(f"Error dropping original table '{target_table}': {e}")
                    cleanup_temp_table()  # Clean up temp table as we can't proceed
                    sys.exit(1)
            else:
                # Determine backup name
                if args.backup_table_name:
                    backup_name = args.backup_table_name
                    logger.info(f"Using provided backup name: '{backup_name}'")
                else:
                    gen_name = f"qdb_cli_backup_{target_table}_{uuid.uuid4()}"
                    backup_name = gen_name[:250].replace("-", "_")
                    logger.info(f"Generated backup name: '{backup_name}'")
                safe_backup_name_quoted = backup_name.replace(
                    "'", "''"
                )  # Quote for potential rollback
                # Check if backup name already exists
                logger.info(f"Checking if backup table '{backup_name}' exists...")
                if client.table_exists(backup_name):
                    logger.error(
                        f"Backup table name '{backup_name}' already exists. Please choose a different name or remove the existing table."
                    )
                    cleanup_temp_table()  # Clean up temp table
                    sys.exit(1)
                logger.info(
                    f"Backup table '{backup_name}' does not exist. Proceeding with rename."
                )
                # Rename original to backup
                logger.info(
                    f"Renaming original table '{target_table}' to backup table '{backup_name}'..."
                )
                # Use single quotes for RENAME identifiers
                rename_query = f"RENAME TABLE '{safe_target_table_quoted}' TO '{safe_backup_name_quoted}';"
                try:
                    response = client.exec(
                        query=rename_query, statement_timeout=args.statement_timeout
                    )
                    if isinstance(response, dict) and "error" in response:
                        error_detail = response["error"]
                        if "query" in response:
                            error_detail += f" (Query: {response['query']})"
                        raise QuestDBAPIError(
                            f"Failed to rename original table: {error_detail}"
                        )
                    logger.info(
                        f"Successfully renamed '{target_table}' to '{backup_name}'."
                    )
                    backup_created = True
                except QuestDBError as e:
                    logger.error(
                        f"Error renaming original table '{target_table}' to '{backup_name}': {e}"
                    )
                    rename_original_failed = True  # Mark failure for rollback check
                    # Don't exit yet, attempt final rename, but report this error at the end
        # --- 5. Rename Temporary Table to Target Table ---
        logger.info(
            f"Renaming temporary table '{temp_table_name}' to target table '{target_table}'..."
        )
        # Use single quotes for RENAME identifiers
        rename_final_query = f"RENAME TABLE '{safe_temp_table_name_quoted}' TO '{safe_target_table_quoted}';"
        try:
            response = client.exec(
                query=rename_final_query, statement_timeout=args.statement_timeout
            )
            if isinstance(response, dict) and "error" in response:
                error_detail = response["error"]
                if "query" in response:
                    error_detail += f" (Query: {response['query']})"
                raise QuestDBAPIError(
                    f"Failed to rename temporary table to target: {error_detail}",
                    response_data=response,
                )
            # If the original rename failed, report it now even if final rename succeeded
            if rename_original_failed:
                logger.error(
                    f"!!! Previous error occurred: Failed to rename original table '{target_table}' to '{backup_name}'. The temporary table was still renamed to '{target_table}', potentially overwriting data if the original rename partially succeeded."
                )
                cleanup_temp_table()  # Temp table *should* be gone now, but try just in case
                sys.exit(1)
            logger.info(
                f"Successfully renamed temporary table '{temp_table_name}' to '{target_table}'."
            )
            temp_table_created = False  # Mark temp table as successfully renamed/gone
            # --- 6. Success Reporting ---
            success_message = f"Successfully created/replaced table '{target_table}'."
            if args.upsert_keys:
                success_message += f" DEDUP enabled with keys: {args.upsert_keys}."
            if backup_created:
                success_message += f" Original table backed up as '{backup_name}'."
            elif original_dropped_no_backup:
                success_message += " Original table was dropped (no backup)."
            elif not original_exists:
                success_message += " (Original table did not exist)."
            print(
                json.dumps(
                    {
                        "status": "OK",
                        "message": success_message,
                        "target_table": target_table,
                        "upsert_keys_set": args.upsert_keys,
                        "backup_table": backup_name if backup_created else None,
                        "original_dropped_no_backup": original_dropped_no_backup,
                    },
                    indent=2,
                )
            )
            sys.exit(0)
        except QuestDBError as final_rename_err:
            logger.error(
                f"Error renaming temporary table '{temp_table_name}' to '{target_table}': {final_rename_err}"
            )
            # --- Rollback Attempt ---
            if backup_created:
                logger.warning("Attempting to roll back by renaming backup table...")
                # Use single quotes for RENAME identifiers
                rollback_query = f"RENAME TABLE '{safe_backup_name_quoted}' TO '{safe_target_table_quoted}';"
                try:
                    response = client.exec(
                        query=rollback_query, statement_timeout=args.statement_timeout
                    )
                    if isinstance(response, dict) and "error" in response:
                        logger.error(
                            f"!!! ROLLBACK FAILED: Could not rename '{backup_name}' back to '{target_table}': {response['error']}"
                        )
                        logger.error(
                            f"!!! The original table data might be in the backup table: '{backup_name}'."
                        )
                        logger.error(
                            f"!!! The new data might be in the temporary table: '{temp_table_name}'."
                        )
                    else:
                        logger.info(
                            f"Successfully rolled back: Renamed '{backup_name}' back to '{target_table}'."
                        )
                        # Backup is restored, but temp table still exists
                except QuestDBError as rollback_err:
                    logger.error(
                        f"!!! ROLLBACK FAILED: Error renaming '{backup_name}' back to '{target_table}': {rollback_err}"
                    )
                    logger.error(
                        f"!!! The original table data might be in the backup table: '{backup_name}'."
                    )
                    logger.error(
                        f"!!! The new data might be in the temporary table: '{temp_table_name}'."
                    )
            elif original_dropped_no_backup:
                logger.error(
                    "!!! Final rename failed after original table was dropped (no backup)."
                )
                logger.error(
                    f"!!! The new data might be in the temporary table: '{temp_table_name}'."
                )
            elif rename_original_failed:
                logger.error(
                    "!!! Final rename failed AND the previous attempt to rename the original table also failed."
                )
                logger.error(
                    f"!!! State is uncertain. Original table '{target_table}' might still exist."
                )
                logger.error(
                    f"!!! The new data might be in the temporary table: '{temp_table_name}'."
                )
            else:  # Original didn't exist or rename didn't happen
                logger.error("!!! Final rename failed.")
                logger.error(
                    f"!!! The new data might be in the temporary table: '{temp_table_name}'."
                )
            cleanup_temp_table()  # Attempt to remove the temp table regardless of rollback outcome
            sys.exit(1)  # Exit with error after handling final rename failure
    except QuestDBError as e:
        logger.error(f"An unexpected QuestDB error occurred during the operation: {e}")
        cleanup_temp_table()  # Attempt cleanup
        # Check if state is inconsistent due to error before final rename
        if backup_created and (
            not temp_table_created
        ):  # Error happened after backup rename but before/during final rename
            logger.error(
                f"!!! An error occurred after the original table was renamed to '{backup_name}'. Manual check required. Temporary table '{temp_table_name}' may or may not exist."
            )
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user.")
        cleanup_temp_table()  # Attempt cleanup
        # Check state
        if backup_created:
            logger.warning(
                f"!!! Operation cancelled. Original table might be renamed to '{backup_name}'. Temporary table '{temp_table_name}' might exist."
            )
        elif temp_table_created:
            logger.warning(
                f"!!! Operation cancelled. Temporary table '{temp_table_name}' might exist."
            )
        sys.exit(130)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        cleanup_temp_table()  # Attempt cleanup
        # Check state
        if backup_created:
            logger.error(
                f"!!! An unexpected error occurred. Original table might be renamed to '{backup_name}'. Temporary table '{temp_table_name}' might exist."
            )
        elif temp_table_created:
            logger.error(
                f"!!! An unexpected error occurred. Temporary table '{temp_table_name}' might exist."
            )
        sys.exit(1)


def explain_output_to_text(data: Dict[str, Any]) -> str:
    """Convert query plan dict to plain text output."""
    lines = [html.unescape(row[0]) for row in data.get("dataset", [])]
    return "\n".join(lines)


# --- NEW: handle_schema ---


def handle_schema(args, client: QuestDBClient):
    """Handles the schema command, fetching CREATE TABLE statements for multiple tables."""
    table_names_to_process = []
    source_description = ""
    # 1. Determine the source of table names (validation in get_args)
    if args.table_names:
        table_names_to_process = args.table_names
        source_description = "command line arguments"
    elif args.file:
        try:
            with open(args.file, "r", encoding="utf-8") as f:
                table_names_to_process = [
                    line.strip() for line in f if line.strip()
                ]  # Read non-empty lines
            source_description = f"file '{args.file}'"
        except IOError as e:
            logger.warning(f"Error reading table names file '{args.file}': {e}")
            sys.exit(1)
    elif not sys.stdin.isatty():
        logger.info("Reading table names from standard input (one per line)...")
        try:
            table_names_to_process = [
                line.strip() for line in sys.stdin if line.strip()
            ]  # Read non-empty lines
            source_description = "standard input"
        except Exception as e:
            logger.warning(f"Error reading table names from stdin: {e}")
            sys.exit(1)
    else:
        # This case should be caught by validation in get_args
        logger.error("Internal error: No table names source identified.")
        sys.exit(2)
    if not table_names_to_process:
        logger.warning(f"No valid table names found in {source_description}.")
        sys.exit(0)
    logger.info(
        f"Fetching schema for {len(table_names_to_process)} table(s) from {source_description}."
    )
    # --- Process Tables ---
    any_table_failed = False
    output_separator = "\n\n"  # Separate multiple CREATE TABLE statements
    num_tables = len(table_names_to_process)
    first_output_written = False
    # Suppress info/debug logs from client during schema fetch unless requested globally
    original_client_log_level = logging.getLogger("questdb_rest").getEffectiveLevel()
    if not args.debug and (not args.info):
        logging.getLogger("questdb_rest").setLevel(logging.WARNING)
    for i, table_name in enumerate(table_names_to_process):
        logger.info(
            f"--- Fetching schema for table {i + 1}/{num_tables}: '{table_name}' ---"
        )
        # --- Dry Run Check ---
        if args.dry_run:
            simulate_schema(args, table_name, i + 1, num_tables)
            if first_output_written:
                sys.stdout.write(output_separator)
            first_output_written = True
            continue  # Skip actual execution
        # --- Actual Execution ---
        # Use double quotes for SHOW CREATE TABLE identifier
        safe_table_name_double_quoted = table_name.replace('"', '""')
        statement = f'SHOW CREATE TABLE "{safe_table_name_double_quoted}";'
        try:
            # Intentionally don't pass most exec args, only statement_timeout
            response_json = client.exec(
                query=statement, statement_timeout=args.statement_timeout
            )
            # Check for errors within the JSON response
            if isinstance(response_json, dict) and "error" in response_json:
                error_msg = response_json["error"]
                # Check if the error is "table does not exist"
                if "table does not exist" in error_msg.lower():
                    logger.warning(
                        f"Table '{table_name}' does not exist, cannot fetch schema."
                    )
                    sys.stderr.write(
                        f"-- Info for table '{table_name}' --\nTable does not exist.\n"
                    )
                    # Optionally treat 'not exists' as a failure or just skip output
                    # Let's treat it as a skippable non-failure for schema command
                    continue  # Skip output for this table, but don't mark as failed
                else:
                    # Handle other errors as real failures
                    logger.error(
                        f"Error fetching schema for '{table_name}': {error_msg}"
                    )
                    sys.stderr.write(
                        f"-- Error for table '{table_name}' --\nError: {error_msg}\n"
                    )
                    any_table_failed = True
                    if args.stop_on_error:
                        logger.warning(
                            "Stopping execution due to error (stop-on-error enabled)."
                        )
                        # Restore original log level before exiting
                        logging.getLogger("questdb_rest").setLevel(
                            original_client_log_level
                        )
                        sys.exit(1)
                    else:
                        logger.warning("Continuing execution (stop-on-error disabled).")
                        continue  # Skip to next table
            # Extract the CREATE TABLE statement
            create_statement = None
            if isinstance(response_json, dict) and "dataset" in response_json:
                if (
                    len(response_json["dataset"]) > 0
                    and len(response_json["dataset"][0]) > 0
                ):
                    create_statement = response_json["dataset"][0][0]
                else:
                    # This case might indicate an issue if the table was expected to exist
                    logger.warning(
                        f'''Received empty dataset for 'SHOW CREATE TABLE "{safe_table_name_double_quoted}"'. Table might be empty or query failed silently.'''
                    )
                    sys.stderr.write(
                        f"-- Warning for table '{table_name}' --\nReceived empty result for SHOW CREATE TABLE.\n"
                    )
                    # Treat as failure for schema command if we expected a result
                    any_table_failed = True
                    if args.stop_on_error:
                        logging.getLogger("questdb_rest").setLevel(
                            original_client_log_level
                        )
                        sys.exit(1)
                    else:
                        continue
            else:
                logger.error(
                    f'''Unexpected response format for 'SHOW CREATE TABLE "{safe_table_name_double_quoted}"': {response_json}'''
                )
                sys.stderr.write(
                    f"-- Error for table '{table_name}' --\nUnexpected response format from server.\n"
                )
                any_table_failed = True
                if args.stop_on_error:
                    logging.getLogger("questdb_rest").setLevel(
                        original_client_log_level
                    )
                    sys.exit(1)
                else:
                    continue
            # Print separator if this is not the first successful output
            if first_output_written:
                sys.stdout.write(output_separator)
            # Print the extracted CREATE TABLE statement
            if create_statement:
                sys.stdout.write(create_statement)
                # Ensure trailing newline
                if not create_statement.endswith("\n"):
                    sys.stdout.write("\n")
                first_output_written = True
                logger.info(f"Successfully fetched schema for '{table_name}'.")
        except QuestDBAPIError as e:
            # Check if API error itself indicates "table does not exist"
            error_msg = str(e)
            if "table does not exist" in error_msg.lower():
                logger.warning(
                    f"Table '{table_name}' does not exist (API Error), cannot fetch schema."
                )
                sys.stderr.write(
                    f"-- Info for table '{table_name}' --\nTable does not exist (API Error).\n"
                )
                continue  # Skip output, not a failure for this command
            else:
                logger.warning(
                    f"Fetching schema for '{table_name}' failed with API error: {e}"
                )
                sys.stderr.write(f"-- Error for table '{table_name}' --\nError: {e}\n")
                any_table_failed = True
                if args.stop_on_error:
                    logging.getLogger("questdb_rest").setLevel(
                        original_client_log_level
                    )
                    sys.exit(1)
                else:
                    logger.warning("Continuing execution (stop-on-error disabled).")
        except QuestDBError as e:  # Catch other client errors (connection etc.)
            logger.warning(f"Fetching schema for '{table_name}' failed: {e}")
            sys.stderr.write(f"-- Error for table '{table_name}' --\nError: {e}\n")
            any_table_failed = True
            if args.stop_on_error:
                logging.getLogger("questdb_rest").setLevel(original_client_log_level)
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
                logging.getLogger("questdb_rest").setLevel(original_client_log_level)
                sys.exit(1)
            else:
                continue
        except KeyboardInterrupt:
            logger.info(
                f"\nOperation cancelled by user while fetching schema for '{table_name}'."
            )
            logging.getLogger("questdb_rest").setLevel(original_client_log_level)
            sys.exit(130)
    # Restore original client log level
    logging.getLogger("questdb_rest").setLevel(original_client_log_level)
    # --- Final Exit Status ---
    if any_table_failed:
        logger.warning("One or more table schemas could not be fetched.")
        sys.exit(2)  # Indicate partial failure if stop-on-error was false
    else:
        logger.info("All requested schemas fetched successfully.")
        sys.exit(0)


def handle_rename(args, client: QuestDBClient):
    """Handles the rename command using the client's exec method."""
    old_name = args.old_table_name
    new_name = args.new_table_name
    logger.info(f"Preparing to rename table '{old_name}' to '{new_name}'...")
    # --- Dry Run Check ---
    if args.dry_run:
        # Pass client to simulation in case it needs it in the future (though not currently)
        simulate_rename(args, client)  # simulate_rename handles sys.exit(0)
    # Quote the table names for safety - QuestDB uses single quotes for table names in RENAME
    safe_old_name = old_name.replace("'", "''")  # Basic escaping
    safe_new_name = new_name.replace("'", "''")  # Basic escaping
    backup_name = None
    backup_created = False
    new_table_originally_existed = False
    try:
        # 1. Check if the new table name already exists
        logger.info(f"Checking if target table name '{new_name}' already exists...")
        new_table_originally_existed = client.table_exists(new_name)
        if new_table_originally_existed:
            logger.warning(f"Target table name '{new_name}' already exists.")
            if args.no_backup_if_new_table_exists:
                logger.warning(
                    "--no-backup-if-new-table-exists specified. Proceeding without backup. The final rename might fail."
                )
            else:
                # No backup action needed, proceed to final rename
                # Generate backup name for the *existing* new_name table
                backup_name = f"qdb_cli_backup_{new_name}_{uuid.uuid4()}".replace(
                    "-", "_"
                )[:250]
                safe_backup_name = backup_name.replace("'", "''")
                logger.info(
                    f"Attempting to back up existing table '{new_name}' to '{backup_name}'..."
                )
                # Check if generated backup name clashes (highly unlikely)
                if client.table_exists(backup_name):
                    logger.error(
                        f"Generated backup name '{backup_name}' already exists. Cannot proceed."
                    )
                    sys.exit(1)
                # Execute backup rename
                backup_query = (
                    f"RENAME TABLE '{safe_new_name}' TO '{safe_backup_name}';"
                )
                try:
                    response = client.exec(
                        query=backup_query, statement_timeout=args.statement_timeout
                    )
                    if isinstance(response, dict) and "error" in response:
                        error_detail = response["error"]
                        if "query" in response:
                            error_detail += f" (Query: {response['query']})"
                        raise QuestDBAPIError(
                            f"Failed to back up existing table: {error_detail}"
                        )
                    logger.info(
                        f"Successfully backed up existing table '{new_name}' to '{backup_name}'."
                    )
                    backup_created = True
                except QuestDBError as backup_err:
                    logger.error(
                        f"Error backing up existing table '{new_name}': {backup_err}"
                    )
                    # If backup fails, we should not proceed with the final rename
                    sys.exit(1)
        else:
            logger.info(
                f"Target table name '{new_name}' does not exist. No backup needed."
            )
        # 2. Execute the final rename operation
        logger.info(f"Renaming table '{old_name}' to '{new_name}'...")
        final_rename_query = f"RENAME TABLE '{safe_old_name}' TO '{safe_new_name}';"
        # Default exec options are fine
        response_json = client.exec(
            query=final_rename_query, statement_timeout=args.statement_timeout
        )
        # Check for errors within the JSON response of the final rename
        if isinstance(response_json, dict) and "error" in response_json:
            logger.error(f"Error renaming table: {response_json['error']}")
            sys.stderr.write(f"Error: {response_json['error']}\n")
            sys.stderr.write(
                f"Query: {response_json.get('query', final_rename_query)}\n"
            )
            # Attempt to roll back the backup if one was created
            if backup_created:
                logger.warning("Attempting to roll back backup...")
                rollback_query = (
                    f"RENAME TABLE '{safe_backup_name}' TO '{safe_new_name}';"
                )
                try:
                    rb_response = client.exec(
                        query=rollback_query, statement_timeout=args.statement_timeout
                    )
                    if isinstance(rb_response, dict) and "error" in rb_response:
                        logger.error(
                            f"!!! ROLLBACK FAILED: Could not rename '{backup_name}' back to '{new_name}': {rb_response['error']}"
                        )
                    else:
                        logger.info(
                            f"Successfully rolled back backup: '{backup_name}' renamed back to '{new_name}'."
                        )
                except Exception as rb_err:
                    logger.error(
                        f"!!! ROLLBACK FAILED: Error during rollback rename: {rb_err}"
                    )
            sys.exit(1)  # Exit after error and potential rollback attempt
        # Success Case
        success_message = f"Table '{old_name}' successfully renamed to '{new_name}'."
        if backup_created:
            success_message += (
                f" Existing table at '{new_name}' was backed up as '{backup_name}'."
            )
        elif new_table_originally_existed and args.no_backup_if_new_table_exists:
            success_message += (
                f" Existing table at '{new_name}' was overwritten (no backup)."
            )
        logger.info(success_message)
        print(
            json.dumps(
                {
                    "status": "OK",
                    "message": success_message,
                    "old_name": old_name,
                    "new_name": new_name,
                    "backup_of_new_name": backup_name if backup_created else None,
                },
                indent=2,
            )
        )
        sys.exit(0)
    except QuestDBAPIError as e:
        # Catch errors during existence checks or backup rename not caught above
        logger.error(f"API Error during rename process: {e}")
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)
    except QuestDBError as e:
        # Catch connection errors etc.
        logger.error(f"Error during rename process: {e}")
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user.")
        # Note: If cancelled after backup but before final rename, state is inconsistent.
        if backup_created:
            logger.warning(
                f"!!! Operation cancelled after creating backup '{backup_name}'. The original table '{old_name}' was not renamed."
            )
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


def detect_scheme_in_host(host_str):
    """
    Detect if the host string already includes a URL scheme (http:// or https://).
    Returns a tuple of (scheme, actual_host) if scheme is detected, or (None, host_str) if not.
    """
    if not host_str:
        return (None, host_str)
    if host_str.startswith("http://"):
        return ("http", host_str[7:])  # Remove "http://" prefix
    elif host_str.startswith("https://"):
        return ("https", host_str[8:])  # Remove "https://" prefix
    return (None, host_str)  # No scheme detected in host string


def _add_parser_global(parser: argparse.ArgumentParser):
    """Adds global arguments to the main parser."""
    # "-V",
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )  # Default handled by client init (checks config file first)
    parser.add_argument(
        "-H", "--host", default=None, help="QuestDB server host."
    )  # Default handled by client init
    parser.add_argument("--port", type=int, default=None, help="QuestDB REST API port.")
    parser.add_argument(
        "-u", "--user", default=None, help="Username for basic authentication."
    )
    parser.add_argument(
        "-p",
        "--password",
        default=None,
        help="Password for basic authentication. If -u is given but -p is not, will prompt securely unless password is in config.",
    )  # Default handled by client init
    parser.add_argument(
        "--timeout", type=int, default=None, help="Request timeout in seconds."
    )  # Default handled by client init
    parser.add_argument(
        "--scheme",
        default=None,
        choices=["http", "https"],
        help="Connection scheme (http or https).",
    )
    log_level_group = (
        parser.add_mutually_exclusive_group()
    )  # Changed from -v to -i for INFO
    log_level_group.add_argument(
        "-i",
        "--info",
        action="store_true",
        help="Use info level logging (default is WARNING).",
    )  # Global Debug flag - MUST NOT clash with subcommand flags
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
        help="Path to a specific config JSON file (overrides default ~/.questdb-rest/config.json).",
        default=None,
    )
    # Shared stop-on-error argument for commands that process multiple items
    # Note: create-or-replace implicitly stops on error due to its nature
    # Default to stopping on error
    parser.add_argument(
        "--stop-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stop execution immediately if any item (file/statement/table) fails (where applicable).",
    )


def _add_parser_imp(subparsers: argparse._SubParsersAction):
    """Adds arguments for the 'imp' subcommand."""
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
    group_imp_table_name = parser_imp.add_argument_group(
        "Table Name Convenience Options"
    )
    group_imp_table_name.add_argument(
        "--name-func",
        choices=list(TABLE_NAME_FUNCTIONS.keys()),
        help=f"Function to generate table name from filename (ignored if --name set). Available: {', '.join(TABLE_NAME_FUNCTIONS.keys())}",
        default=None,
    )
    group_imp_table_name.add_argument(
        "--name-func-prefix",
        help="Prefix string for 'add_prefix' name function.",
        default="",
    )  # Default to empty string
    # Reuse -d from imp for consistency, but different meaning
    group_imp_table_name.add_argument(
        "-d",
        "--dash-to-underscore",
        action="store_true",
        help="If table name is derived from filename (i.e., --name not set), convert dashes (-) to underscores (_). Compatible with --name-func.",
    )
    group_imp_table_name.add_argument(
        "-z",
        "--derive-table-name-from-filename-stem-and-replace-dash-with-underscore",
        action="store_true",
        help="Shortcut for --name-func=stem and --dash-to-underscore.",
    )
    schema_group = parser_imp.add_mutually_exclusive_group()
    schema_group.add_argument(
        "--schema-file", help="Path to JSON schema file. Applied to ALL files."
    )
    schema_group.add_argument(
        "-s", "--schema", help="JSON schema string. Applied to ALL files. Use quotes."
    )  # Keep -P for partitionBy
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
    # Reuse -d from imp for delimiter
    parser_imp.add_argument("--delimiter", help="Specify CSV delimiter character.")
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
    )  # Keep -O for o3MaxLag
    parser_imp.add_argument(
        "-O",
        "--o3MaxLag",
        type=int,
        help="Set O3 max lag (microseconds, if table created).",
    )  # Keep -M for maxUncommittedRows
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
    # Inherits global --stop-on-error
    parser_imp.set_defaults(func=handle_imp)


def _add_parser_exec(subparsers: argparse._SubParsersAction):
    """Adds arguments for the 'exec' subcommand."""
    parser_exec = subparsers.add_parser(
        "exec",
        help="Execute SQL statement(s) using /exec (returns JSON/text).\nReads SQL from --query, --file, --get-query-from-python-module, or stdin.",
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
    query_input_group = parser_exec.add_mutually_exclusive_group(
        required=False
    )  # Changed to False - stdin is implicit
    query_input_group.add_argument("-q", "--query", help="SQL query string to execute.")
    query_input_group.add_argument(
        "-f", "--file", help="Path to file containing SQL statements."
    )
    # New option: get query from python module (e.g. a_module.b_module:my_sql_statement)
    # Keep -G
    query_input_group.add_argument(
        "-G",
        "--get-query-from-python-module",
        help="Get query from a Python module in the format 'module_path:variable_name'.",
    )
    parser_exec.add_argument(
        "-l",
        "--limit",
        help='Limit results (e.g., "10", "10,20"). Applies per statement.',
    )  # Keep -C for count
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
    )  # Keep -T for timings
    parser_exec.add_argument(
        "-T",
        "--timings",
        action=argparse.BooleanOptionalAction,
        help="Include execution timings.",
    )  # Keep -E for explain
    parser_exec.add_argument(
        "-E",
        "--explain",
        action=argparse.BooleanOptionalAction,
        help="Include execution plan details.",
    )  # Keep -Q for quoteLargeNum
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
        help="Create a new table from the query result(s). Requires --new-table-name.",
    )
    group_query_modifier.add_argument(
        "--new-table-name",
        help="Name of the new table to create from query result(s). Required if --create-table is used.",
    )
    # Inherits global --stop-on-error
    # Output formatting options
    # Keep -x for extract-field
    parser_exec.add_argument(
        "-x",
        "--extract-field",
        metavar="FIELD_NAME_OR_INDEX",
        default=0,
        const=_EXEC_EXTRACT_FIELD_SENTINEL,
        nargs="?",
        help="Extract only the specified column/field (by name or 0-based index) and print each value on a new line. If -x is used but no value provided, the first col will be extracted. Overrides --markdown/--psql/--count/--timings/--explain.",
    )
    exec_format_group = (
        parser_exec.add_mutually_exclusive_group()
    )  # Keep -1 for --one (can be combined with -x)
    exec_format_group.add_argument(
        "-1",
        "--one",
        action="store_true",
        help="Output only the value of the first column of the first row (or first value if combined with --extract-field).",
    )  # Keep -m for markdown
    exec_format_group.add_argument(
        "-m",
        "--markdown",
        action="store_true",
        help="Display query result(s) in Markdown table format using tabulate (ignored if --extract-field is used).",
    )
    # Keep -P (uppercase) for psql format, distinct from global -p password
    exec_format_group.add_argument(
        "-P",
        "--psql",
        action="store_true",
        help="Display query result(s) in PostgreSQL table format using tabulate (ignored if --extract-field is used).",
    )
    parser_exec.set_defaults(func=handle_exec)


def _add_parser_exp(subparsers: argparse._SubParsersAction):
    """Adds arguments for the 'exp' subcommand."""
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


def _add_parser_chk(subparsers: argparse._SubParsersAction):
    """Adds arguments for the 'chk' subcommand."""
    parser_chk = subparsers.add_parser(
        "chk",
        help="Check if one or more tables exist using /chk (returns JSON per table).\nReads table names from arguments, --file, or stdin.",
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )
    parser_chk.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    # Allow zero or more positional args, or --file, or stdin
    parser_chk.add_argument(
        "table_names",
        nargs="*",
        metavar="TABLE_NAME",
        help="Name(s) of the table(s) to check (provided as arguments).",
    )
    parser_chk.add_argument(
        "-f",
        "--file",
        metavar="FILE_PATH",
        help="Path to file containing table names (one per line). Cannot be used with positional arguments.",
    )
    # Implicit stdin reading if neither table_names nor --file is given
    # Inherits global --stop-on-error
    parser_chk.set_defaults(func=handle_chk)


def _add_parser_schema(subparsers: argparse._SubParsersAction):
    """Adds arguments for the 'schema' subcommand."""
    parser_schema = subparsers.add_parser(
        "schema",
        help="Fetch CREATE TABLE statement(s) for one or more tables.\nReads table names from arguments, --file, or stdin.",
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )
    parser_schema.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    # Allow zero or more positional args, or --file, or stdin
    parser_schema.add_argument(
        "table_names",
        nargs="*",
        metavar="TABLE_NAME",
        help="Name(s) of the table(s) to get schema for (provided as arguments).",
    )
    parser_schema.add_argument(
        "-f",
        "--file",
        metavar="FILE_PATH",
        help="Path to file containing table names (one per line). Cannot be used with positional arguments.",
    )
    # Inherits global --stop-on-error
    # Allow timeout per table schema fetch
    parser_schema.add_argument(
        "--statement-timeout",
        type=int,
        help="Query timeout in milliseconds (per table).",
    )
    parser_schema.set_defaults(func=handle_schema)


def _add_parser_rename(subparsers: argparse._SubParsersAction):
    """Adds arguments for the 'rename' subcommand."""
    parser_rename = subparsers.add_parser(
        "rename",
        help="Rename a table using RENAME TABLE. Backs up target name by default if it exists.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        add_help=False,
    )
    parser_rename.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    parser_rename.add_argument("old_table_name", help="Current name of the table.")
    parser_rename.add_argument("new_table_name", help="New name for the table.")
    parser_rename.add_argument(
        "--no-backup-if-new-table-exists",
        action="store_true",
        help="If the new table name already exists, do not back it up first. Rename might fail.",
    )  # Allow timeout for rename operation(s)
    parser_rename.add_argument(
        "--statement-timeout",
        type=int,
        help="Query timeout in milliseconds (per RENAME statement).",
    )
    parser_rename.set_defaults(func=handle_rename)


def _add_parser_cor(subparsers: argparse._SubParsersAction):
    """Adds arguments for the 'create-or-replace-table-from-query' subcommand."""  # Add alias
    parser_cor = subparsers.add_parser(
        "create-or-replace-table-from-query",
        aliases=["cor"],
        help="Atomically replace a table with the result of a query, with optional backup.",
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )
    parser_cor.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    parser_cor.add_argument(
        "table", help="Name of the target table to create or replace."
    )
    # Query input (copied from exec)
    query_input_group_cor = parser_cor.add_mutually_exclusive_group(required=False)
    query_input_group_cor.add_argument(
        "-q", "--query", help="SQL query string defining the new table content."
    )
    query_input_group_cor.add_argument(
        "-f", "--file", help="Path to file containing the SQL query."
    )
    query_input_group_cor.add_argument(
        "-G",
        "--get-query-from-python-module",
        help="Get query from a Python module (format 'module_path:variable_name').",
    )
    # Backup options
    backup_group = parser_cor.add_argument_group(
        "Backup Options (if target table exists)"
    )
    backup_opts_exclusive = (
        backup_group.add_mutually_exclusive_group()
    )  # Ensure consistent dest
    backup_opts_exclusive.add_argument(
        "-B",
        "--backup-table-name",
        "--rename-original-table-to",
        dest="backup_table_name",
        help="Specify a name for the backup table (if target exists). Default: generated name.",
    )
    backup_opts_exclusive.add_argument(
        "--no-backup-original-table",
        action="store_true",
        help="DROP the original table directly instead of renaming it to a backup.",
    )
    # Create options (copied from imp)
    create_opts_group = parser_cor.add_argument_group("New Table Creation Options")
    create_opts_group.add_argument(
        "-P",
        "--partitionBy",
        choices=["NONE", "YEAR", "MONTH", "DAY", "HOUR", "WEEK"],
        help="Partitioning strategy for the new table.",
    )
    create_opts_group.add_argument(
        "-t", "--timestamp", help="Designated timestamp column name for the new table."
    )
    # NEW: Upsert keys option
    create_opts_group.add_argument(
        "-k",
        "--upsert-keys",
        nargs="+",
        metavar="COLUMN",
        help="List of column names to use as UPSERT KEYS when creating the new table. Must include the designated timestamp (if specified via -t). Requires WAL.",
    )
    # Allow timeout for underlying DDL/query
    parser_cor.add_argument(
        "--statement-timeout",
        type=int,
        help="Query timeout in milliseconds for underlying operations.",
    )
    parser_cor.set_defaults(func=handle_create_or_replace_table_from_query)


def _add_parser_drop(subparsers: argparse._SubParsersAction):
    """Adds arguments for the 'drop' subcommand."""  # Add alias
    parser_drop = subparsers.add_parser(
        "drop",
        aliases=["drop-table"],
        help="Drop one or more tables using DROP TABLE.\nReads table names from arguments, --file, or stdin (one per line).",
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )
    parser_drop.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    # Remove the mutually exclusive group for inputs, validation will be done after parsing.
    # Accepts zero or more table names as positional arguments.
    parser_drop.add_argument(
        "table_names",
        nargs="*",
        help="Name(s) of the table(s) to drop (provided as arguments).",
        metavar="TABLE_NAME",
    )
    parser_drop.add_argument(
        "-f",
        "--file",
        help="Path to file containing table names (one per line). Cannot be used if table names are provided as arguments.",
        metavar="FILE_PATH",
    )
    # Implicit stdin reading if neither table_names nor --file is given
    # Inherits global --stop-on-error
    # Allow timeout per table drop
    parser_drop.add_argument(
        "--statement-timeout",
        type=int,
        help="Query timeout in milliseconds (per table).",
    )
    parser_drop.set_defaults(func=handle_drop)


def _add_parser_dedupe(subparsers: argparse._SubParsersAction):
    """Adds arguments for the 'dedupe' subcommand."""
    parser_dedupe = subparsers.add_parser(
        "dedupe",
        help="Enable, disable, or check deduplication for one or more WAL tables.\nReads table names from arguments, --file, or stdin.",
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )
    parser_dedupe.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    # Allow zero or more positional args, or --file, or stdin
    parser_dedupe.add_argument(
        "table_names",
        nargs="*",
        metavar="TABLE_NAME",
        help="Name(s) of the target WAL table(s) (provided as arguments).",
    )
    parser_dedupe.add_argument(
        "-f",
        "--file",
        metavar="FILE_PATH",
        help="Path to file containing table names (one per line). Cannot be used with positional arguments.",
    )
    # Action flags (apply to all tables)
    dedupe_action_group = parser_dedupe.add_mutually_exclusive_group()
    dedupe_action_group.add_argument(
        "--enable",
        action="store_true",
        help="Enable deduplication. Requires --upsert-keys.",
    )
    dedupe_action_group.add_argument(
        "--disable", action="store_true", help="Disable deduplication."
    )
    dedupe_action_group.add_argument(
        "--check",
        action="store_true",
        help="Check current deduplication status and keys (default action).",
    )
    # Upsert keys (apply to all tables if enabling)
    # Accept multiple space-separated keys
    parser_dedupe.add_argument(
        "-k",
        "--upsert-keys",
        nargs="+",
        metavar="COLUMN",
        help="List of column names for UPSERT KEYS when enabling. Must include the designated timestamp. Applies to all tables.",
    )
    # Inherits global --stop-on-error
    # Allow timeout for ALTER TABLE
    parser_dedupe.add_argument(
        "--statement-timeout",
        type=int,
        help="Query timeout in milliseconds for the ALTER TABLE statement (per table).",
    )
    parser_dedupe.set_defaults(func=handle_dedupe)


def _add_parser_gen_config(subparsers: argparse._SubParsersAction):
    """Adds arguments for the 'gen-config' subcommand."""
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


def get_args():
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="QuestDB REST API Command Line Interface.\nLogs to stderr, outputs data to stdout.\n\nUses QuestDB REST API via questdb_rest library.",
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
        epilog="This CLI can also be used as a Python library.\n\nLinks:\n- Write-up and demo: https://teddysc.me/blog/questdb-rest\n- Interactive QuestDB Shell: https://teddysc.me/blog/rlwrap-questdb-shell\n- GitHub: https://github.com/tddschn/questdb-rest\n- PyPI: https://pypi.org/project/questdb-rest/",
    )
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    # Add global arguments
    _add_parser_global(parser)
    # Add subparsers
    subparsers = parser.add_subparsers(
        dest="command", required=True, help="Available sub-commands"
    )
    # Add subcommand arguments
    _add_parser_imp(subparsers)
    _add_parser_exec(subparsers)
    _add_parser_exp(subparsers)
    _add_parser_chk(subparsers)
    _add_parser_schema(subparsers)
    _add_parser_rename(subparsers)
    _add_parser_cor(subparsers)
    _add_parser_drop(subparsers)
    _add_parser_dedupe(subparsers)
    _add_parser_gen_config(subparsers)
    # Parse arguments
    try:
        args = parser.parse_args()
        # Add requires_client default if not set by a specific command (like gen-config)
        if not hasattr(args, "requires_client"):
            args.requires_client = True
        # --- Post-parsing validation ---
        # Validation for commands accepting multiple table name inputs
        multi_table_commands = ["drop", "chk", "schema", "dedupe"]
        if args.command in multi_table_commands:
            has_positional_args = bool(getattr(args, "table_names", None))
            has_file_arg = bool(getattr(args, "file", None))
            is_stdin_piped = not sys.stdin.isatty()
            input_sources = sum([has_positional_args, has_file_arg, is_stdin_piped])
            if input_sources > 1:
                parser.error(
                    f"For '{args.command}', provide table names via positional arguments, --file, OR stdin, not multiple."
                )
            if input_sources == 0:
                # If no input is provided via args, file, or stdin, it's an error
                parser.error(
                    f"For '{args.command}', table names must be provided via positional arguments, --file, or stdin."
                )
            # Specific validation for file vs positional args
            if has_positional_args and has_file_arg:
                parser.error(
                    f"argument -f/--file: not allowed with positional table name arguments for command '{args.command}'"
                )
        # Set default action for dedupe if none specified
        if args.command == "dedupe":
            if not (args.enable or args.disable or args.check):
                args.check = True  # Default to check
            # Validate --upsert-keys usage
            if args.enable and (not args.upsert_keys):
                parser.error("argument --enable: requires --upsert-keys to be set.")
            if (args.disable or args.check) and args.upsert_keys:
                parser.error(
                    "argument --upsert-keys: only allowed when using --enable."
                )
        # Validation for exec --create-table
        if args.command == "exec":
            if args.create_table and (not args.new_table_name):
                parser.error("--new-table-name is required when using --create-table.")
        # Validation for rename old == new
        if args.command == "rename":
            if args.old_table_name == args.new_table_name:
                parser.error("Old and new table names cannot be the same.")
        # Validation for cor query input (check happens in handler now)
        return args
    except SystemExit as e:  # Catch argparse errors specifically
        # Argparse already prints help/errors, just exit
        sys.exit(e.code if e.code is not None else 1)
    except Exception as e:
        parser.print_usage(sys.stderr)
        logger.error(f"Argument parsing error: {e}")
        sys.exit(
            2
        )  # Use exit code 2 for CLI usage errors  # Use exit code 2 for CLI usage errors


def main():
    """Main entry point for the CLI."""
    # Build the parser first
    parser = build_parser()
    # --- Enable argcomplete ---
    # Call this *before* parsing arguments
    argcomplete.autocomplete(parser)
    # Now parse the arguments
    try:
        args = parser.parse_args()
        # Add requires_client default if not set by a specific command (like gen-config)
        if not hasattr(args, "requires_client"):
            args.requires_client = True
    except SystemExit as e:  # Catch argparse errors specifically
        # Argparse already prints help/errors, just exit
        sys.exit(e.code if e.code is not None else 1)
    except Exception as e:
        parser.print_usage(sys.stderr)
        logger.error(f"Argument parsing error: {e}")
        sys.exit(2)  # Use exit code 2 for CLI usage errors
    # --- Post-parsing validation ---
    # (Moved from the original get_args function)
    multi_table_commands = ["drop", "chk", "schema", "dedupe"]
    if args.command in multi_table_commands:
        has_positional_args = bool(getattr(args, "table_names", None))
        has_file_arg = bool(getattr(args, "file", None))
        is_stdin_piped = not sys.stdin.isatty()
        input_sources = sum([has_positional_args, has_file_arg, is_stdin_piped])
        if input_sources > 1:
            parser.error(
                f"For '{args.command}', provide table names via positional arguments, --file, OR stdin, not multiple."
            )
        if input_sources == 0:
            # If no input is provided via args, file, or stdin, it's an error
            parser.error(
                f"For '{args.command}', table names must be provided via positional arguments, --file, or stdin."
            )
        # Specific validation for file vs positional args
        if has_positional_args and has_file_arg:
            parser.error(
                f"argument -f/--file: not allowed with positional table name arguments for command '{args.command}'"
            )
    # Set default action for dedupe if none specified
    if args.command == "dedupe":
        if not (args.enable or args.disable or args.check):
            args.check = True  # Default to check
        # Validate --upsert-keys usage
        if args.enable and (not args.upsert_keys):
            parser.error("argument --enable: requires --upsert-keys to be set.")
        if (args.disable or args.check) and args.upsert_keys:
            parser.error("argument --upsert-keys: only allowed when using --enable.")
    # Validation for exec --create-table
    if args.command == "exec":
        if args.create_table and (not args.new_table_name):
            parser.error("--new-table-name is required when using --create-table.")
    # Validation for rename old == new
    if args.command == "rename":
        if args.old_table_name == args.new_table_name:
            parser.error("Old and new table names cannot be the same.")
    # Validation for cor query input (check happens in handler now)
    # --- Set logging level based on args ---
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
        # Match the CLI's formatter for consistency
        # Simpler format like CLI logger
        formatter = logging.Formatter("%(levelname)s: %(message)s")
        handler.setFormatter(formatter)
        library_logger.addHandler(handler)
    if (
        log_level <= logging.INFO
    ):  # Print info/debug startup messages only if level appropriate
        logger.info(f"Log level set to {logging.getLevelName(log_level)}")
    if log_level == logging.DEBUG:
        logger.debug("Debug logging enabled for CLI and library.")
    # --- Handle Password Prompting ---
    # This needs to happen *before* client initialization, but *after* parsing args
    # Only prompt if a user is provided, no password is given, not dry run, and client is needed
    actual_password = args.password
    if (
        args.requires_client
        and args.user
        and (not args.password)
        and (not args.dry_run)
    ):
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
                # Determine host for prompt (use default if not specified)
                host_for_prompt = DEFAULT_HOST
                if args.host:
                    _, actual_host_for_prompt = detect_scheme_in_host(args.host)
                    host_for_prompt = actual_host_for_prompt
                elif os.path.exists(
                    config_to_check
                ):  # Check config host if no CLI host
                    try:
                        with open(config_to_check, "r") as cf:
                            config = json.load(cf)
                        host_for_prompt = config.get("host", DEFAULT_HOST)
                    except Exception:
                        pass  # Stick with default host if config load fails here
                prompt_str = f"Password for user '{args.user}' at {host_for_prompt}: "
                actual_password = getpass(prompt_str)
                if not actual_password:  # Handle empty input during prompt
                    logger.warning("Password required but not provided.")
                    sys.exit(1)
            except (EOFError, KeyboardInterrupt):
                logger.info("\nOperation cancelled during password input.")
                sys.exit(130)
    # --- Further Argument Validation (Moved from old get_args) ---
    if args.command == "imp":
        if args.name_func == "add_prefix" and (not args.name_func_prefix):
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
    elif args.command == "exec":
        # Check if query source is missing (stdin check happens in handler)
        if (
            not args.query
            and (not args.file)
            and (not args.get_query_from_python_module)
            and sys.stdin.isatty()
        ):
            # This validation should ideally be in get_args or using argparse logic
            logger.error(
                "No SQL query provided via --query, --file, --get-query-from-python-module, or stdin. Exiting."
            )
            sys.exit(2)
    # --- Instantiate Client (if needed and not dry run) ---
    client = None
    if args.requires_client and (not args.dry_run):
        try:
            # Check if host already contains a scheme
            detected_scheme = None
            actual_host = args.host
            final_scheme = args.scheme  # Start with CLI arg scheme
            if args.host:
                detected_scheme, actual_host = detect_scheme_in_host(args.host)
                if detected_scheme:
                    logger.debug(
                        f"Detected scheme '{detected_scheme}://' in host parameter: '{args.host}'"
                    )
                    # Override scheme if detected in host, unless explicitly provided via --scheme
                    if final_scheme is None:  # Only override if --scheme wasn't given
                        final_scheme = detected_scheme  # Use the host without scheme
            # Use potentially prompted/config password
            # Use the finally decided scheme
            client_kwargs = {
                "host": actual_host,
                "port": args.port,
                "user": args.user,
                "password": actual_password,
                "timeout": args.timeout,
                "scheme": final_scheme,
            }
            # Filter out None values so client uses its defaults/config loading
            filtered_kwargs = {k: v for k, v in client_kwargs.items() if v is not None}
            if args.config:
                # If a specific config file is given via --config, use from_config_file
                # We prioritize command-line args over the config file if both are present.
                # Use from_config_file first, then override with CLI args.
                try:
                    logger.info(
                        f"Loading configuration from specified file: {args.config}"
                    )
                    # Load base settings from the specified config file
                    base_client = QuestDBClient.from_config_file(args.config)
                    # Prepare overrides from CLI args (only if they were actually provided)
                    # Use filtered_kwargs which already has the resolved CLI args + password + scheme
                    # Get the base client's values to compare
                    base_url_parts = base_client.base_url.split("://")
                    base_scheme = base_url_parts[0]
                    host_port_parts = base_url_parts[1].split(":")
                    base_host = host_port_parts[0]
                    base_port = (
                        int(host_port_parts[1].split("/")[0])
                        if len(host_port_parts) > 1
                        and host_port_parts[1].split("/")[0].isdigit()
                        else QuestDBClient.DEFAULT_PORT
                    )
                    base_user = base_client.auth[0] if base_client.auth else None
                    base_password = base_client.auth[1] if base_client.auth else None
                    base_timeout = base_client.timeout
                    # Build final kwargs prioritizing CLI args (already in filtered_kwargs) over config file
                    final_kwargs_from_config = {
                        "host": filtered_kwargs.get("host", base_host),
                        "port": filtered_kwargs.get("port", base_port),
                        "user": filtered_kwargs.get("user", base_user),
                        "password": filtered_kwargs.get("password", base_password),
                        "timeout": filtered_kwargs.get("timeout", base_timeout),
                        "scheme": filtered_kwargs.get("scheme", base_scheme),
                    }
                    client = QuestDBClient(**final_kwargs_from_config)
                    logger.debug(
                        f"Client initialized from {args.config} and potentially updated with CLI args."
                    )
                except FileNotFoundError:
                    logger.error(f"Config file not found: {args.config}")
                    sys.exit(1)
                except (json.JSONDecodeError, KeyError) as conf_err:
                    logger.error(f"Error parsing config file {args.config}: {conf_err}")
                    sys.exit(1)
                except Exception as conf_err:
                    logger.error(f"Error loading config file {args.config}: {conf_err}")
                    sys.exit(1)
            else:
                # Standard initialization: uses CLI args > ~/.questdb-rest/config.json > defaults
                logger.debug(
                    "Initializing client using command-line arguments and/or default config (~/.questdb-rest/config.json)."
                )
                client = QuestDBClient(**filtered_kwargs)
            # Log final connection details (mask password)
            log_host = client.base_url.split("://")[1].split(":")[0]
            # Correctly extract port even without trailing slash
            port_part = client.base_url.split(":")[-1]
            log_port = (
                int(port_part.split("/")[0])
                if port_part.split("/")[0].isdigit()
                else QuestDBClient.DEFAULT_PORT
            )
            log_scheme = client.base_url.split("://")[0]
            log_user_info = f" as user '{client.auth[0]}'" if client.auth else ""
            logger.info(
                f"Connecting to {log_scheme}://{log_host}:{log_port}{log_user_info}"
            )
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
        # Pass the client instance (or None for dry run/gen-config) and args to the handler
        args.func(args, client)
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user.")
        sys.exit(130)
    except SystemExit as e:
        # Allow sys.exit calls from handlers to propagate
        sys.exit(
            e.code if e.code is not None else 0
        )  # Default to exit 0 if code is None
    except (QuestDBConnectionError, QuestDBAPIError, QuestDBError):
        # Catch specific client errors that might not be caught in handlers
        # Logged by client already, just exit non-zero
        # logger.error(f"QuestDB Error: {e}") # Redundant logging
        sys.exit(1)
    except Exception as e:
        # Catch-all for unexpected errors in command handlers
        # Use exception to log traceback for unexpected errors
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

        def ic(*args):  # Define a dummy ic function
            return args[0] if args else None

        pass  # Keep native Python logging
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        # Final fallback
        import traceback

        print(f"An unexpected error occurred at the top level: {e}", file=sys.stderr)
        traceback.print_exc()  # Print traceback for unexpected top-level errors
        sys.exit(1)


def build_parser():
    """Builds the argument parser."""
    parser = argparse.ArgumentParser(
        description="QuestDB REST API Command Line Interface.\nLogs to stderr, outputs data to stdout.\n\nUses QuestDB REST API via questdb_rest library.",
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
        epilog=CLI_EPILOG,
    )
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    # Add global arguments
    _add_parser_global(parser)
    # Add subparsers
    subparsers = parser.add_subparsers(
        dest="command", required=True, help="Available sub-commands"
    )
    # Add subcommand arguments
    _add_parser_imp(subparsers)
    _add_parser_exec(subparsers)
    _add_parser_exp(subparsers)
    _add_parser_chk(subparsers)
    _add_parser_schema(subparsers)
    _add_parser_rename(subparsers)
    _add_parser_cor(subparsers)
    _add_parser_drop(subparsers)
    _add_parser_dedupe(subparsers)
    _add_parser_gen_config(subparsers)
    return parser
