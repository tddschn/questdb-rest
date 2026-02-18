"""MCP (Model Context Protocol) server for QuestDB REST API.

This module exposes QuestDB operations as MCP tools, allowing LLMs like Claude
to interact with QuestDB via stdio transport.

Usage:
    # Via CLI subcommand
    qdb-cli mcp

    # Via direct entry point
    qdb-mcp
"""

from typing import Any, Literal, Optional

from questdb_rest import QuestDBClient, QuestDBError

# Default limits to avoid burning through LLM context
DEFAULT_LIST_TABLES_LIMIT = 100
DEFAULT_EXECUTE_SQL_LIMIT = "100"

# Regex matching UUID-4 with either dashes or underscores
UUID_REGEX = r"[0-9a-f]{8}([-_][0-9a-f]{4}){3}[-_][0-9a-f]{12}"


def _get_client() -> QuestDBClient:
    """Create a QuestDBClient with config loaded from ~/.questdb-rest/config.json."""
    return QuestDBClient()


def _safe_exec(
    query: str,
    limit: Optional[str] = None,
    statement_timeout: Optional[int] = None,
) -> dict[str, Any]:
    """Execute a query and return result dict, catching errors."""
    try:
        client = _get_client()
        return client.exec(query=query, limit=limit, statement_timeout=statement_timeout)
    except QuestDBError as e:
        return {"error": str(e), "success": False}
    except Exception as e:
        return {"error": f"Unexpected error: {e}", "success": False}


def run_server() -> None:
    """Run the MCP server with stdio transport."""
    try:
        from mcp.server import FastMCP
    except ImportError:
        raise ImportError(
            "MCP package not installed. Install with: pip install questdb-rest[mcp]"
        )

    mcp = FastMCP(name="questdb")

    # --- Tools ---

    @mcp.tool()
    def execute_sql(
        query: str,
        limit: Optional[str] = DEFAULT_EXECUTE_SQL_LIMIT,
        statement_timeout: Optional[int] = None,
        output_format: Literal["json", "csv", "psql", "markdown"] = "json",
    ) -> dict[str, Any]:
        """Execute any SQL query against QuestDB.

        Args:
            query: The SQL query to execute (SELECT, INSERT, CREATE, DROP, etc.)
            limit: Limit for results (e.g., "10" or "10,20" for offset,limit). Default: "100". Use "0" or empty string for unlimited.
            statement_timeout: Optional query timeout in milliseconds
            output_format: Output format - "json" (default), "csv", "psql" (ASCII table), or "markdown"

        Returns:
            Query result with columns, dataset, and count (json), or formatted string (csv/psql/markdown), or error info on failure
        """
        # Handle limit: "0" or "" means unlimited
        effective_limit = limit if limit and limit != "0" else None

        if output_format == "csv":
            # Use the exp endpoint for CSV output
            try:
                client = _get_client()
                response = client.exp(query=query, limit=effective_limit)
                return {"csv": response.text, "format": "csv", "success": True}
            except QuestDBError as e:
                return {"error": str(e), "success": False}
            except Exception as e:
                return {"error": f"Unexpected error: {e}", "success": False}

        # For json/psql/markdown, use exec endpoint
        result = _safe_exec(query, effective_limit, statement_timeout)

        if not result.get("success", True) or "error" in result:
            return result

        # Format output if needed
        if output_format in ("psql", "markdown") and "columns" in result and "dataset" in result:
            try:
                from tabulate import tabulate

                headers = [col["name"] for col in result["columns"]]
                table_data = result["dataset"]
                fmt = "psql" if output_format == "psql" else "github"
                formatted = tabulate(table_data, headers=headers, tablefmt=fmt)
                return {
                    "table": formatted,
                    "format": output_format,
                    "count": result.get("count", len(table_data)),
                    "success": True,
                }
            except ImportError:
                # Fallback to JSON if tabulate not installed
                result["warning"] = "tabulate not installed, returning JSON instead"
                return result

        return result

    @mcp.tool()
    def list_tables(
        pattern: Optional[str] = None,
        exclude_pattern: Optional[str] = None,
        has_uuid: Optional[bool] = None,
        limit: Optional[int] = DEFAULT_LIST_TABLES_LIMIT,
    ) -> dict[str, Any]:
        """List tables in QuestDB with optional filtering.

        Args:
            pattern: Optional regex pattern to match table names (e.g., "trades", "cme_.*")
            exclude_pattern: Optional regex pattern to exclude table names (e.g., "backup", "test_.*")
            has_uuid: If True, only tables with UUID-4 in name; if False, only tables without UUID-4
            limit: Maximum number of tables to return (default: 100, set to None for unlimited)

        Returns:
            Dict with 'tables' list containing table names, 'count', 'total_count' (before limit), or error info on failure
        """
        try:
            client = _get_client()
            # Build the WHERE clause
            conditions = []
            if pattern:
                safe_pattern = pattern.replace("'", "''")
                conditions.append(f"table_name ~ '{safe_pattern}'")
            if exclude_pattern:
                safe_exclude = exclude_pattern.replace("'", "''")
                conditions.append(f"table_name !~ '{safe_exclude}'")
            if has_uuid is True:
                conditions.append(f"table_name ~ '{UUID_REGEX}'")
            elif has_uuid is False:
                conditions.append(f"table_name !~ '{UUID_REGEX}'")

            where_clause = ""
            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)

            query = f"SELECT table_name FROM tables(){where_clause} ORDER BY table_name"
            result = client.exec(query=query)
            if "error" in result:
                return {"error": result["error"], "success": False}
            all_tables = [row[0] for row in result.get("dataset", [])]
            total_count = len(all_tables)

            # Apply limit
            if limit is not None and limit > 0:
                tables = all_tables[:limit]
            else:
                tables = all_tables

            return {
                "tables": tables,
                "count": len(tables),
                "total_count": total_count,
                "truncated": len(tables) < total_count,
                "success": True,
            }
        except QuestDBError as e:
            return {"error": str(e), "success": False}
        except Exception as e:
            return {"error": f"Unexpected error: {e}", "success": False}

    @mcp.tool()
    def describe_table(table_name: str) -> dict[str, Any]:
        """Get column information for a table.

        Args:
            table_name: Name of the table to describe

        Returns:
            Dict with 'columns' list containing column details, or error info on failure
        """
        try:
            client = _get_client()
            # Use double quotes for table name identifier (required for SQL keywords)
            safe_name = table_name.replace('"', '""')
            result = client.exec(
                query=f'SELECT column, type, indexed, indexBlockCapacity, symbolCached, symbolCapacity, designated, upsertKey '
                f'FROM table_columns("{safe_name}")'
            )
            if "error" in result:
                return {"error": result["error"], "success": False}

            columns = []
            col_names = [col["name"] for col in result.get("columns", [])]
            for row in result.get("dataset", []):
                col_info = dict(zip(col_names, row))
                columns.append(col_info)

            return {"table_name": table_name, "columns": columns, "success": True}
        except QuestDBError as e:
            return {"error": str(e), "success": False}
        except Exception as e:
            return {"error": f"Unexpected error: {e}", "success": False}

    @mcp.tool()
    def get_table_schema(table_name: str) -> dict[str, Any]:
        """Get the CREATE TABLE statement for a table.

        Args:
            table_name: Name of the table

        Returns:
            Dict with 'create_statement' string, or error info on failure
        """
        try:
            client = _get_client()
            # Use double quotes for SHOW CREATE TABLE identifier
            safe_name = table_name.replace('"', '""')
            result = client.exec(query=f'SHOW CREATE TABLE "{safe_name}"')
            if "error" in result:
                return {"error": result["error"], "success": False}

            if result.get("count", 0) > 0 and result.get("dataset"):
                create_statement = result["dataset"][0][0]
                return {
                    "table_name": table_name,
                    "create_statement": create_statement,
                    "success": True,
                }
            return {"error": "Could not retrieve CREATE TABLE statement", "success": False}
        except QuestDBError as e:
            return {"error": str(e), "success": False}
        except Exception as e:
            return {"error": f"Unexpected error: {e}", "success": False}

    @mcp.tool()
    def check_table_exists(table_name: str) -> dict[str, Any]:
        """Check if a table exists in QuestDB.

        Args:
            table_name: Name of the table to check

        Returns:
            Dict with 'exists' boolean, or error info on failure
        """
        try:
            client = _get_client()
            exists = client.table_exists(table_name)
            return {"table_name": table_name, "exists": exists, "success": True}
        except QuestDBError as e:
            return {"error": str(e), "success": False}
        except Exception as e:
            return {"error": f"Unexpected error: {e}", "success": False}

    @mcp.tool()
    def export_csv(
        query: str,
        limit: Optional[str] = DEFAULT_EXECUTE_SQL_LIMIT,
    ) -> dict[str, Any]:
        """Export query results as CSV.

        Args:
            query: The SQL query to execute
            limit: Limit for results (e.g., "10" or "10,20"). Default: "100". Use "0" or empty string for unlimited.

        Returns:
            Dict with 'csv' string containing the CSV data, or error info on failure
        """
        try:
            client = _get_client()
            # Handle limit: "0" or "" means unlimited
            effective_limit = limit if limit and limit != "0" else None
            response = client.exp(query=query, limit=effective_limit)
            return {"csv": response.text, "success": True}
        except QuestDBError as e:
            return {"error": str(e), "success": False}
        except Exception as e:
            return {"error": f"Unexpected error: {e}", "success": False}

    # --- Resources ---

    @mcp.resource("questdb://tables")
    def get_tables_resource() -> str:
        """Get list of all tables as a resource."""
        try:
            client = _get_client()
            result = client.exec(query="SELECT table_name FROM tables() ORDER BY table_name")
            if "error" in result:
                return f"Error: {result['error']}"
            tables = [row[0] for row in result.get("dataset", [])]
            return "\n".join(tables)
        except Exception as e:
            return f"Error: {e}"

    @mcp.resource("questdb://table/{name}/schema")
    def get_table_schema_resource(name: str) -> str:
        """Get CREATE TABLE statement as a resource."""
        try:
            client = _get_client()
            safe_name = name.replace('"', '""')
            result = client.exec(query=f'SHOW CREATE TABLE "{safe_name}"')
            if "error" in result:
                return f"Error: {result['error']}"
            if result.get("count", 0) > 0 and result.get("dataset"):
                return result["dataset"][0][0]
            return "Error: Could not retrieve schema"
        except Exception as e:
            return f"Error: {e}"

    # Run with stdio transport (default)
    mcp.run()


if __name__ == "__main__":
    run_server()
