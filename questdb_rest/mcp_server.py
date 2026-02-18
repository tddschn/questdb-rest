"""MCP (Model Context Protocol) server for QuestDB REST API.

This module exposes QuestDB operations as MCP tools, allowing LLMs like Claude
to interact with QuestDB via stdio transport.

Usage:
    # Via CLI subcommand
    qdb-cli mcp

    # Via direct entry point
    qdb-mcp
"""

from typing import Any, Optional

from questdb_rest import QuestDBClient, QuestDBError


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
        limit: Optional[str] = None,
        statement_timeout: Optional[int] = None,
    ) -> dict[str, Any]:
        """Execute any SQL query against QuestDB.

        Args:
            query: The SQL query to execute (SELECT, INSERT, CREATE, DROP, etc.)
            limit: Optional limit for results (e.g., "10" or "10,20" for offset,limit)
            statement_timeout: Optional query timeout in milliseconds

        Returns:
            Query result with columns, dataset, and count, or error info on failure
        """
        return _safe_exec(query, limit, statement_timeout)

    @mcp.tool()
    def list_tables() -> dict[str, Any]:
        """List all tables in QuestDB.

        Returns:
            Dict with 'tables' list containing table names, or error info on failure
        """
        try:
            client = _get_client()
            result = client.exec(query="SELECT table_name FROM tables() ORDER BY table_name")
            if "error" in result:
                return {"error": result["error"], "success": False}
            tables = [row[0] for row in result.get("dataset", [])]
            return {"tables": tables, "count": len(tables), "success": True}
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
            # Escape single quotes in table name
            safe_name = table_name.replace("'", "''")
            result = client.exec(
                query=f"SELECT column, type, indexed, indexBlockCapacity, symbolCached, symbolCapacity, designated, upsertKey "
                f"FROM table_columns('{safe_name}')"
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
    def export_csv(query: str, limit: Optional[str] = None) -> dict[str, Any]:
        """Export query results as CSV.

        Args:
            query: The SQL query to execute
            limit: Optional limit for results (e.g., "10" or "10,20")

        Returns:
            Dict with 'csv' string containing the CSV data, or error info on failure
        """
        try:
            client = _get_client()
            response = client.exp(query=query, limit=limit)
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
