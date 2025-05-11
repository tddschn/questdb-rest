# questdb_rest.py
import requests
import json
import logging
from urllib.parse import urlencode, urljoin
from typing import List, Optional, Dict, Any, Union, IO, Tuple
from questdb_rest.utils import _qdb_exec_result_dict_extract_field

# --------------------
# consts
# --------------------

__version__ = "4.4.3"
CLI_EPILOG = """This CLI can also be used as a Python library.

Links:
- Write-up and demo: https://teddysc.me/blog/questdb-rest
- Interactive QuestDB Shell: https://teddysc.me/blog/rlwrap-questdb-shell
- GitHub: https://github.com/tddschn/questdb-rest
- PyPI: https://pypi.org/project/questdb-rest/

Enable shell completion with this command:
    eval "$(uvx --from argcomplete register-python-argcomplete %(prog)s)"
"""


# --------------------
# logger
# --------------------

logger = logging.getLogger(__name__)

# --- Custom Exceptions ---


class QuestDBError(Exception):
    """Base exception for questdb_rest errors."""

    pass


class QuestDBConnectionError(QuestDBError):
    """Raised for network-related errors (connection, timeout)."""

    pass


class QuestDBAPIError(QuestDBError):
    """Raised for errors reported by the QuestDB API (e.g., bad query, import failure)."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_data: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data

    def __str__(self) -> str:
        if self.status_code:
            return f"HTTP {self.status_code}: {super().__str__()}"
        return super().__str__()


# --- Client Class ---


class QuestDBClient:
    """
    A client for interacting with the QuestDB REST API.
    """

    DEFAULT_PORT = 9000  # Default REST API port documented is 9000
    DEFAULT_TIMEOUT = 60  # Default request timeout in seconds

    def __init__(
        self,
        host: str = "localhost",
        port: int = DEFAULT_PORT,
        user: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
        scheme: str = "http",  # Allow overriding scheme if needed (e.g., https)
    ):
        """
        Initializes the QuestDB REST API client.

        Args:
            host: QuestDB server host.
            port: QuestDB REST API port.
            user: Username for basic authentication (optional).
            password: Password for basic authentication (optional).
            timeout: Request timeout in seconds.
            scheme: URL scheme (http or https).
        """
        # --- Load config file (if exists) ---
        import os

        config_file = os.path.expanduser("~/.questdb-rest/config.json")
        if os.path.exists(config_file):
            try:
                with open(config_file, "r") as cf:
                    config = json.load(cf)
            except Exception as e:
                logger.warning(f"Error loading config file {config_file}: {e}")
                config = {}
        else:
            config = {}
        # Override parameters with config values if still at default
        if host == "localhost" and "host" in config:
            host = config["host"]
        if port == QuestDBClient.DEFAULT_PORT and "port" in config:
            port = config["port"]
        if user is None and "user" in config:
            user = config["user"]
        if password is None and "password" in config:
            password = config["password"]
        if timeout == QuestDBClient.DEFAULT_TIMEOUT and "timeout" in config:
            timeout = config["timeout"]
        if scheme == "http" and "scheme" in config:
            scheme = config["scheme"]
        # --- End config loading ---
        if not host:
            raise ValueError("Host cannot be empty")
        if not isinstance(port, int) or port <= 0:
            raise ValueError("Port must be a positive integer")

        self.base_url = f"{scheme}://{host}:{port}/"
        self.timeout = timeout
        self.auth = (user, password) if user else None
        logger.debug(f"QuestDBClient initialized for {self.base_url}")

    @classmethod
    def from_config_file(cls, config_path: str) -> "QuestDBClient":
        with open(config_path, "r") as cf:
            config = json.load(cf)
        host = config.get("host", "localhost")
        port = config.get("port", cls.DEFAULT_PORT)
        user = config.get("user", None)
        password = config.get("password", None)
        timeout = config.get("timeout", cls.DEFAULT_TIMEOUT)
        scheme = config.get("scheme", "http")
        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            timeout=timeout,
            scheme=scheme,
        )

    def _build_url(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Builds the full URL for an API endpoint."""
        url = urljoin(self.base_url, endpoint.lstrip("/"))
        if params:
            # Filter out None values before encoding
            filtered_params = {k: v for k, v in params.items() if v is not None}
            if filtered_params:
                url += "?" + urlencode(filtered_params)
        return url

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        json_payload: Optional[Dict[str, Any]] = None,
        files: Optional[
            Dict[str, Tuple[Optional[str], Union[bytes, IO[bytes]], Optional[str]]]
        ] = None,
        headers: Optional[Dict[str, str]] = None,
        stream: bool = False,
    ) -> requests.Response:
        """
        Makes an HTTP request to the QuestDB API.

        Args:
            method: HTTP method (GET, POST, etc.).
            endpoint: API endpoint path (e.g., '/exec').
            params: URL query parameters.
            data: Request body data.
            json_payload: JSON request body data.
            files: Dictionary for multipart/form-data uploads.
                   Format: {'name': ('filename', file_content, 'content_type')}
            headers: Custom HTTP headers.
            stream: Whether to stream the response.

        Returns:
            requests.Response object.

        Raises:
            QuestDBConnectionError: If a connection or timeout error occurs.
            QuestDBAPIError: If the API returns an error status code.
            QuestDBError: For other unexpected errors during the request.
        """
        full_url = self._build_url(endpoint, params)
        req_headers = headers or {}
        # Ensure default content type is not interfering with 'files' upload
        if files and "Content-Type" in req_headers:
            # requests handles multipart Content-Type correctly when 'files' is used
            pass
        elif json_payload and "Content-Type" not in req_headers:
            req_headers["Content-Type"] = "application/json"

        logger.debug(f"Request: {method} {full_url}")
        if self.auth:
            logger.debug("Using basic authentication.")
        if req_headers:
            logger.debug(f"Headers: {req_headers}")
        if params:
            logger.debug(f"Params: {params}")
        if json_payload:
            logger.debug(f"JSON Payload: {json_payload}")
        if files:
            # Log file names, not contents
            log_files = {k: v[0] if v else None for k, v in files.items()}
            logger.debug(f"Files: {log_files}")

        try:
            response = requests.request(
                method,
                full_url,
                auth=self.auth,
                data=data,
                json=json_payload,
                files=files,
                headers=req_headers,
                timeout=self.timeout,
                stream=stream,
            )
            logger.debug(f"Response Status: {response.status_code}")
            response.raise_for_status()  # Raise HTTPError for 4xx/5xx
            return response

        except requests.exceptions.ConnectionError as e:
            msg = f"Could not connect to QuestDB at {self.base_url}. Details: {e}"
            logger.warning(msg)
            raise QuestDBConnectionError(msg) from e
        except requests.exceptions.Timeout as e:
            msg = f"Request timed out after {self.timeout} seconds."
            logger.warning(msg)
            raise QuestDBConnectionError(msg) from e
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            reason = e.response.reason
            error_data = None
            err_msg = f"HTTP {status_code}: {reason}"
            try:
                error_data = e.response.json()
                # Try to extract more specific error message
                if isinstance(error_data, dict):
                    if "message" in error_data:  # Common error format
                        err_msg = f"HTTP {status_code}: {error_data['message']}"
                    elif "error" in error_data:  # /exec error format
                        err_msg = f"HTTP {status_code}: {error_data['error']}"
                    elif (
                        "status" in error_data and error_data["status"] != "OK"
                    ):  # /imp error format
                        err_msg = f"HTTP {status_code}: Import failed - {error_data['status']}"
                    elif (
                        "status" in error_data and "Exists" in error_data["status"]
                    ):  # /chk error format
                        err_msg = f"HTTP {status_code}: Check failed - {error_data['status']}"  # Should not happen on 200 OK for chk

                logger.warning(f"QuestDB API Error: {err_msg}")
                logger.warning(
                    f"Response Body: {json.dumps(error_data)}"
                )  # Log full JSON error
            except json.JSONDecodeError:
                logger.warning(f"QuestDB API Error: {err_msg} (Non-JSON response)")
                logger.warning(f"Raw Response Body: {e.response.text}")

            raise QuestDBAPIError(
                err_msg, status_code=status_code, response_data=error_data
            ) from e
        except requests.exceptions.RequestException as e:
            msg = f"An unexpected request error occurred: {e}"
            logger.warning(msg)
            raise QuestDBError(msg) from e
        except Exception as e:
            # Catch any other unexpected errors during request processing
            msg = f"An unexpected internal error occurred during request: {e}"
            logger.exception(msg)  # Log with traceback
            raise QuestDBError(msg) from e

    def imp(
        self,
        data_file_path: Optional[str] = None,
        data_file_obj: Optional[IO[bytes]] = None,
        data_file_name: Optional[str] = "data.csv",  # Default filename if obj is used
        schema_json_str: Optional[str] = None,
        schema_file_path: Optional[str] = None,
        schema_file_obj: Optional[IO[bytes]] = None,
        table_name: Optional[str] = None,
        partition_by: Optional[str] = None,
        timestamp_col: Optional[str] = None,
        overwrite: Optional[bool] = None,
        atomicity: Optional[str] = None,  # skipCol, skipRow, abort
        delimiter: Optional[str] = None,
        force_header: Optional[bool] = None,
        skip_lev: Optional[bool] = None,
        fmt: Optional[str] = None,  # tabular, json
        o3_max_lag: Optional[int] = None,
        max_uncommitted_rows: Optional[int] = None,
        create_table: Optional[bool] = None,
    ) -> requests.Response:
        """
        Imports data using the /imp endpoint. Provide data via one of
        `data_file_path` or `data_file_obj`. Provide schema via one of
        `schema_json_str`, `schema_file_path`, or `schema_file_obj`.

        Args:
            data_file_path: Path to the data file (e.g., CSV).
            data_file_obj: An open file-like object (in binary mode) containing the data.
            data_file_name: Filename to use when `data_file_obj` is provided.
            schema_json_str: JSON string defining the schema.
            schema_file_path: Path to a JSON file defining the schema.
            schema_file_obj: An open file-like object (in binary mode) for the schema.
            table_name: Name of the table to import into. If None, derived from data filename.
            partition_by: Partitioning strategy (e.g., 'MONTH', 'DAY').
            timestamp_col: Name of the designated timestamp column.
            overwrite: Whether to overwrite existing table data/structure.
            atomicity: Behavior on data errors ('skipCol', 'skipRow', 'abort').
            delimiter: CSV delimiter character.
            force_header: Force treating the first line as a header.
            skip_lev: Skip Line Extra Values.
            fmt: Response format ('tabular' or 'json').
            o3_max_lag: Set O3 max lag for the created table (microseconds).
            max_uncommitted_rows: Set max uncommitted rows for the created table.
            create_table: Automatically create table if it does not exist (default: True).

        Returns:
            requests.Response object containing the import status.

        Raises:
            ValueError: If data source or schema source is ambiguous or missing.
            QuestDBError: For API or connection issues.
        """
        if not data_file_path and not data_file_obj:
            raise ValueError("Either data_file_path or data_file_obj must be provided.")
        if data_file_path and data_file_obj:
            raise ValueError("Provide only one of data_file_path or data_file_obj.")

        # Determine effective table name if not provided explicitly
        effective_table_name = table_name
        if not effective_table_name:
            if data_file_path:
                # Basic stem extraction, might need refinement for complex paths
                effective_table_name = (
                    data_file_path.split("/")[-1].split("\\")[-1].rsplit(".", 1)[0]
                )
            elif data_file_name:
                effective_table_name = data_file_name.rsplit(".", 1)[0]
            if not effective_table_name:
                # Fallback if name couldn't be derived
                effective_table_name = "imported_table"
            logger.info(f"Table name not specified, derived '{effective_table_name}'")

        params = {
            "name": effective_table_name,
            "partitionBy": partition_by,
            "timestamp": timestamp_col,
            "overwrite": str(overwrite).lower() if overwrite is not None else None,
            "atomicity": atomicity,
            "delimiter": delimiter,
            "forceHeader": str(force_header).lower()
            if force_header is not None
            else None,
            "skipLev": str(skip_lev).lower() if skip_lev is not None else None,
            "fmt": fmt,
            "o3MaxLag": o3_max_lag,
            "maxUncommittedRows": max_uncommitted_rows,
            "create": str(create_table).lower() if create_table is not None else None,
        }

        files_for_request: Dict[
            str, Tuple[Optional[str], Union[bytes, IO[bytes]], Optional[str]]
        ] = {}
        data_f = None
        schema_f = None

        try:
            # Prepare Data Part
            if data_file_path:
                actual_filename = data_file_path.split("/")[-1].split("\\")[-1]
                data_f = open(data_file_path, "rb")
                files_for_request["data"] = (
                    actual_filename,
                    data_f,
                    "application/octet-stream",
                )
            else:  # data_file_obj must be set
                files_for_request["data"] = (
                    data_file_name,
                    data_file_obj,
                    "application/octet-stream",
                )  # type: ignore

            # Prepare Schema Part (if provided)
            schema_sources = sum(
                1
                for src in [schema_json_str, schema_file_path, schema_file_obj]
                if src is not None
            )
            if schema_sources > 1:
                raise ValueError(
                    "Provide only one of schema_json_str, schema_file_path, or schema_file_obj."
                )

            schema_content: Optional[Union[bytes, IO[bytes]]] = None
            if schema_json_str:
                schema_content = schema_json_str.encode("utf-8")
                files_for_request["schema"] = (
                    "schema.json",
                    schema_content,
                    "application/json",
                )
            elif schema_file_path:
                schema_f = open(schema_file_path, "rb")
                files_for_request["schema"] = (
                    "schema.json",
                    schema_f,
                    "application/json",
                )
            elif schema_file_obj:
                files_for_request["schema"] = (
                    "schema.json",
                    schema_file_obj,
                    "application/json",
                )

            # Make the request
            return self._request("POST", "/imp", params=params, files=files_for_request)

        finally:
            # Ensure files opened by this method are closed
            if data_f:
                data_f.close()
            if schema_f:
                schema_f.close()

    def exec(
        self,
        query: str,
        limit: Optional[str] = None,
        count: Optional[bool] = None,
        nm: Optional[bool] = None,  # skip metadata
        timings: Optional[bool] = None,
        explain: Optional[bool] = None,
        quote_large_num: Optional[bool] = None,
        statement_timeout: Optional[int] = None,  # in milliseconds
    ) -> Dict[str, Any]:
        """
        Executes a SQL query using the /exec endpoint.

        Args:
            query: The SQL query string to execute.
            limit: Limit results (e.g., "10", "10,20").
            count: Include row count in the response.
            nm: Skip metadata section in the response.
            timings: Include execution timings in the response.
            explain: Include execution plan details in the response.
            quote_large_num: Return LONG numbers as quoted strings.
            statement_timeout: Query timeout in milliseconds (sent as header).

        Returns:
            A dictionary containing the parsed JSON response from the API.

        Raises:
            QuestDBError: For API, connection, or JSON parsing issues.
        """
        if not query or not isinstance(query, str):
            raise ValueError("Query must be a non-empty string.")

        params = {
            "query": query,
            "limit": limit,
            "count": str(count).lower() if count is not None else None,
            "nm": str(nm).lower() if nm is not None else None,
            "timings": str(timings).lower() if timings is not None else None,
            "explain": str(explain).lower() if explain is not None else None,
            "quoteLargeNum": str(quote_large_num).lower()
            if quote_large_num is not None
            else None,
        }
        headers = {}
        if statement_timeout is not None:
            if not isinstance(statement_timeout, int) or statement_timeout < 0:
                raise ValueError("statement_timeout must be a non-negative integer.")
            headers["Statement-Timeout"] = str(statement_timeout)

        response = self._request("GET", "/exec", params=params, headers=headers)

        try:
            return response.json()
        except json.JSONDecodeError as e:
            msg = f"Failed to decode JSON response from /exec. Content: {response.text[:200]}"
            logger.error(msg)
            raise QuestDBError(msg) from e

    def exp(
        self,
        query: str,
        limit: Optional[str] = None,
        nm: Optional[bool] = None,  # skip header row
        stream_response: bool = False,  # Allow caller to handle streaming
    ) -> requests.Response:
        """
        Exports data using the /exp endpoint (typically returns CSV).

        Args:
            query: The SQL query for data export.
            limit: Limit results (e.g., "10", "10,20", "-20").
            nm: Skip header row in the CSV output.
            stream_response: If True, returns the response object immediately
                             for streaming. Caller is responsible for handling
                             the response content and closing. If False,
                             the response content is loaded into memory.

        Returns:
            requests.Response object. The caller should handle reading the
            content (e.g., response.text or iterate response.iter_content()).

        Raises:
            QuestDBError: For API or connection issues.
        """
        if not query or not isinstance(query, str):
            raise ValueError("Query must be a non-empty string.")

        params = {
            "query": query,
            "limit": limit,
            "nm": str(nm).lower() if nm is not None else None,
        }
        # Set stream=True if the caller wants to handle streaming
        return self._request("GET", "/exp", params=params, stream=stream_response)

    def chk(self, table_name: str) -> Dict[str, str]:
        """
        Checks if a table exists using the (undocumented) /chk endpoint.

        Args:
            table_name: The name of the table to check.

        Returns:
            A dictionary containing the status, e.g., {"status": "Exists"}
            or {"status": "Does not exist"}.

        Raises:
             ValueError: If table_name is empty.
             QuestDBError: For API, connection, or JSON parsing issues.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("table_name must be a non-empty string.")

        params = {
            "f": "json",  # Force JSON response
            "j": table_name,  # Table name parameter for /chk
            "version": "2",  # Version parameter seems required
        }

        response = self._request("GET", "/chk", params=params)

        try:
            return response.json()
        except json.JSONDecodeError as e:
            msg = f"Failed to decode JSON response from /chk. Content: {response.text[:200]}"
            logger.error(msg)
            raise QuestDBError(msg) from e

    def table_exists(self, table_name: str) -> bool:
        """
        Convenience method to check if a table exists.

        Args:
            table_name: The name of the table to check.

        Returns:
            True if the table exists, False otherwise.

        Raises:
             QuestDBError: For underlying API, connection, or JSON parsing issues.
        """
        try:
            result = self.chk(table_name)
            # Check the specific string QuestDB returns
            return result.get("status") == "Exists"
        except QuestDBAPIError as e:
            # Handle case where /chk might return 400/500 for some reason
            logger.warning(f"API error during table check for '{table_name}': {e}")
            return False
        except QuestDBError as e:
            # Handle connection errors etc.
            logger.error(f"Failed to check table existence for '{table_name}': {e}")
            raise  # Re-raise other QuestDB errors

    # questdb_rest/__init__.py
    def exec_extract_field(
        self,
        query: str,
        field: Union[str, int],
        limit: Optional[str] = None,
        count: Optional[bool] = None,
        nm: Optional[bool] = None,  # skip metadata
        timings: Optional[bool] = None,
        explain: Optional[bool] = None,
        quote_large_num: Optional[bool] = None,
        statement_timeout: Optional[int] = None,  # in milliseconds
    ) -> List[Any]:
        """
        Executes a SQL query and extracts a specific field from the result set.

        Args:
            query: The SQL query string to execute.
            field: The column name (str) or 0-based index (int) to extract.
            limit: Limit results (e.g., "10", "10,20").
            count: Include row count in the response (ignored for extraction).
            nm: Skip metadata section in the response.
            timings: Include execution timings in the response (ignored for extraction).
            explain: Include execution plan details in the response (ignored for extraction).
            quote_large_num: Return LONG numbers as quoted strings.
            statement_timeout: Query timeout in milliseconds.

        Returns:
            A list containing the values from the specified column in the dataset.

        Raises:
            QuestDBError: For API, connection, JSON parsing, or extraction issues.
            ValueError: If the specified field is not found or invalid.
            KeyError: If the response format is unexpected.
            TypeError: If arguments are of the wrong type.
        """
        if explain:
            logger.warning("Ignoring 'explain=True' when using 'exec_extract_field'.")
            explain = False
        if count:
            logger.warning("Ignoring 'count=True' when using 'exec_extract_field'.")
            count = False
        if timings:
            logger.warning("Ignoring 'timings=True' when using 'exec_extract_field'.")
            timings = False

        # Call the standard exec method first
        result_dict = self.exec(
            query=query,
            limit=limit,
            nm=nm,
            quote_large_num=quote_large_num,
            statement_timeout=statement_timeout,
            count=False,  # Force count=False
            timings=False,  # Force timings=False
            explain=False,  # Force explain=False
        )

        # Now extract the field using the utility function
        try:
            extracted_values = _qdb_exec_result_dict_extract_field(
                result_dict=result_dict, field=field
            )
            return extracted_values
        except (ValueError, KeyError, TypeError, IndexError) as e:
            # Re-raise extraction errors as a QuestDBError for consistency
            msg = f"Failed to extract field '{field}' from query result: {e}"
            logger.error(msg)
            raise QuestDBError(msg) from e


# Example Usage (can be removed or kept for basic testing)
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # --- Configuration ---
    # Replace with your QuestDB host/port if different
    TEST_HOST = "localhost"
    TEST_PORT = 9000
    # Add user/password if authentication is enabled
    TEST_USER = None
    TEST_PASSWORD = None
    # -------------------

    print(f"Attempting to connect to QuestDB at {TEST_HOST}:{TEST_PORT}")
    client = QuestDBClient(
        host=TEST_HOST, port=TEST_PORT, user=TEST_USER, password=TEST_PASSWORD
    )

    try:
        # --- Test /exec ---
        print("\n--- Testing /exec (CREATE TABLE) ---")
        create_sql = "CREATE TABLE IF NOT EXISTS rest_client_test_table (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY;"
        exec_response = client.exec(query=create_sql)
        print(f"Exec Response (CREATE): {json.dumps(exec_response, indent=2)}")

        # --- Test /chk ---
        print("\n--- Testing /chk ---")
        chk_response = client.chk("rest_client_test_table")
        print(f"Check Response (chk): {json.dumps(chk_response, indent=2)}")
        exists = client.table_exists("rest_client_test_table")
        print(f"Table 'rest_client_test_table' exists: {exists}")
        exists_nonexistent = client.table_exists("non_existent_table_xyz123")
        print(f"Table 'non_existent_table_xyz123' exists: {exists_nonexistent}")

        # --- Test /imp ---
        print("\n--- Testing /imp ---")
        # Create dummy CSV data
        csv_data = (
            "ts,val\n2024-01-01T00:00:00.000Z,10.5\n2024-01-01T00:00:01.000Z,11.2"
        )
        import io

        data_obj = io.BytesIO(csv_data.encode("utf-8"))
        imp_response = client.imp(
            data_file_obj=data_obj,
            data_file_name="dummy.csv",  # Provide a name when using object
            table_name="rest_client_test_table",
            fmt="json",
            overwrite=False,  # Append data
        )
        print(f"Import Response Status Code: {imp_response.status_code}")
        try:
            print(
                f"Import Response Body (JSON): {json.dumps(imp_response.json(), indent=2)}"
            )
        except json.JSONDecodeError:
            print(f"Import Response Body (Text): {imp_response.text}")

        # --- Test /exec (SELECT) ---
        print("\n--- Testing /exec (SELECT) ---")
        select_sql = "SELECT * FROM rest_client_test_table ORDER BY ts DESC LIMIT 5"
        select_response = client.exec(query=select_sql)
        print(f"Exec Response (SELECT): {json.dumps(select_response, indent=2)}")

        # --- Test /exp ---
        print("\n--- Testing /exp ---")
        exp_response = client.exp(query=select_sql)
        print(f"Export Response Status Code: {exp_response.status_code}")
        print(f"Export Response Body (CSV):\n{exp_response.text}")

        # --- Test /exec (DROP TABLE) ---
        print("\n--- Testing /exec (DROP TABLE) ---")
        drop_sql = "DROP TABLE IF EXISTS rest_client_test_table;"
        drop_response = client.exec(query=drop_sql)
        print(f"Exec Response (DROP): {json.dumps(drop_response, indent=2)}")

    except QuestDBConnectionError as e:
        print(f"\n*** Connection Error: {e}")
        print("*** Please ensure QuestDB is running and accessible.")
    except QuestDBAPIError as e:
        print(f"\n*** API Error: {e}")
        if e.response_data:
            print(f"*** Details: {json.dumps(e.response_data, indent=2)}")
    except QuestDBError as e:
        print(f"\n*** General QuestDB Client Error: {e}")
    except Exception as e:
        print(f"\n*** An unexpected error occurred: {e}")
