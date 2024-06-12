import re
def extract_delete_info(delete_query):
    # Regular expressions to extract table name and where columns
    table_name_pattern = r'DELETE\s+FROM\s+(\w+)'
    where_columns_pattern = r'WHERE\s+(.*)'

    # Extract table name
    table_match = re.search(table_name_pattern, delete_query, re.IGNORECASE)
    if not table_match:
        return None, None
    table_name = table_match.group(1)

    # Extract where columns
    where_columns_match = re.search(where_columns_pattern, delete_query, re.IGNORECASE | re.DOTALL)
    if not where_columns_match:
        return table_name, None
    where_columns_str = where_columns_match.group(1)
    where_columns = [col.split('=')[0].strip() for col in re.split(r'\s+AND\s+', where_columns_str)]

    return table_name, where_columns
