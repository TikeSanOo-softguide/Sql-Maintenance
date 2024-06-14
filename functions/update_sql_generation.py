import re
def update_table_column_names(update_query):
    # Regular expressions to extract table name, set columns, and where columns
    table_name_pattern = r'UPDATE\s+(\w+)\s+SET'
    set_columns_pattern = r'SET\s+(.*?)(?:\s+WHERE|$)'
    where_columns_pattern = r'WHERE\s+(.*)$'
    # Regular expression to find <cfif ...> ... </cfif>
    cfif_pattern = re.compile(r"<cfif[^>]*>.*?</cfif>", re.DOTALL)

    # Extract table name
    table_match = re.search(table_name_pattern, update_query, re.IGNORECASE | re.DOTALL)
    if not table_match:
        return None, None, None
    table_name = table_match.group(1)

    # Extract set columns
    set_columns_match = re.search(set_columns_pattern, update_query, re.IGNORECASE | re.DOTALL)
    if not set_columns_match:
        return table_name, None, None
    set_columns_str = set_columns_match.group(1)
    columns_name = cfif_pattern.sub('', set_columns_str)
    set_columns = [col.split('=')[0].strip() for col in re.split(r',\s*\n|\s*,\s*', columns_name)]

    # Extract where columns
    where_columns_match = re.search(where_columns_pattern, update_query, re.IGNORECASE | re.DOTALL)
    if not where_columns_match:
        return table_name, set_columns, None
    where_columns_str = where_columns_match.group(1)
    # print(where_columns_str)
    where_columns = [col.split('=')[0].strip().lstrip('(') for col in re.split(r'\s+AND\s+', where_columns_str)]

    return table_name, set_columns, where_columns