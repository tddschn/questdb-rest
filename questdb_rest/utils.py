from typing import List, Dict, Any, Union


def _qdb_exec_result_dict_extract_field(
    result_dict: Dict[str, Any], field: Union[str, int]
) -> List[Any]:
    """
    Extracts values of a specified field from a QuestDB exec result dictionary.

    The dictionary is expected to follow the structure of the QuestDB /exec endpoint
    JSON response, containing 'columns' and 'dataset' keys. If 'field' is a string
    and an exact match isn't found, it attempts a case-insensitive match.

    Args:
        result_dict: A dictionary representing the QuestDB JSON response.
        field: The field to extract, either by column name (str) or
               0-based index (int).

    Returns:
        A list containing the values from the specified column in the dataset.

    Raises:
        TypeError: If 'result_dict' is not a dictionary or 'field' is not a
                   string or an integer.
        KeyError: If 'result_dict' is missing 'columns' or 'dataset' keys.
        ValueError: If 'field' is a string but the column name is not found (neither
                    exactly nor case-insensitively), or if 'field' is an integer
                    index that is out of range.
        IndexError: If the dataset rows have inconsistent lengths and the
                    determined index is out of bounds for a specific row.
    """
    if not isinstance(result_dict, dict):
        raise TypeError("Input 'result_dict' must be a dictionary.")

    # Check for essential keys
    if "columns" not in result_dict:
        raise KeyError("Input dictionary missing required key: 'columns'")
    if "dataset" not in result_dict:
        raise KeyError("Input dictionary missing required key: 'dataset'")

    columns_info = result_dict["columns"]
    dataset = result_dict["dataset"]
    num_columns = len(columns_info)

    if not isinstance(columns_info, list):
        raise TypeError("Key 'columns' must be a list.")
    if not isinstance(dataset, list):
        raise TypeError("Key 'dataset' must be a list.")

    column_index = -1
    column_name_found = None  # Store the actual found column name for clarity

    # Determine the column index based on the type of 'field'
    if isinstance(field, str):
        column_name_to_find = field
        # 1. Try exact match first
        for i, col_info in enumerate(columns_info):
            if isinstance(col_info, dict):
                current_col_name = col_info.get("name")
                if current_col_name == column_name_to_find:
                    column_index = i
                    column_name_found = current_col_name
                    break  # Found exact match

        # 2. If no exact match, try case-insensitive match
        if column_index == -1:
            for i, col_info in enumerate(columns_info):
                if isinstance(col_info, dict):
                    current_col_name = col_info.get("name")
                    # Check if name exists and compare lowercased versions
                    if (
                        current_col_name is not None
                        and current_col_name.lower() == column_name_to_find.lower()
                    ):
                        # Ensure we don't overwrite an exact match if it somehow failed the first check
                        # Or handle multiple case-insensitive matches (here, we take the first one)
                        if column_index == -1:
                            column_index = i
                            column_name_found = current_col_name
                            # Decide if you want to break after the first case-insensitive match
                            # break
                        else:
                            # Optional: Handle ambiguity if multiple case-insensitive matches exist
                            # For now, we just keep the first one found.
                            pass

        # 3. If still not found, raise error
        if column_index == -1:
            available_columns = [
                c.get("name", "[Missing Name]")
                for c in columns_info
                if isinstance(c, dict)
            ]
            raise ValueError(
                f"Column name '{column_name_to_find}' not found (case-insensitive search also failed). Available columns: {available_columns}"
            )

    elif isinstance(field, int):
        requested_index = field
        if 0 <= requested_index < num_columns:
            column_index = requested_index
            if requested_index < len(columns_info) and isinstance(
                columns_info[requested_index], dict
            ):
                column_name_found = columns_info[requested_index].get(
                    "name", f"[Index {requested_index}]"
                )
            else:
                column_name_found = f"[Index {requested_index}]"
        else:
            # Handle case where there are no columns gracefully
            if num_columns == 0:
                raise ValueError(
                    f"Cannot access index {requested_index}: There are no columns defined."
                )
            else:
                raise ValueError(
                    f"Column index {requested_index} is out of range (must be between 0 and {num_columns - 1})."
                )

    else:
        raise TypeError(
            f"Input 'field' must be a string (column name) or an integer (index), but got {type(field).__name__}."
        )

    # Extract the data using the determined column index
    extracted_values = []
    for i, row in enumerate(dataset):
        if not isinstance(row, list):
            raise TypeError(f"Dataset item at index {i} is not a list: {row}")
        try:
            extracted_values.append(row[column_index])
        except IndexError:
            # This error implies the specific row doesn't have enough elements
            raise IndexError(
                f"Row {i} (value: {row}) has length {len(row)}, but tried to access index {column_index} (for column '{column_name_found}')."
            )

    return extracted_values
