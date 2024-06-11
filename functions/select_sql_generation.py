import re
from collections import defaultdict

def extract_table_column_names(sql_query):
      # Regular expressions to match table aliases, columns, and table names
    table_alias_regex = re.compile(r'\b(\w+)\s+AS\s+(\w+)\b', re.IGNORECASE)
    table_name_regex = re.compile(r'FROM\s+(\w+)', re.IGNORECASE)
    column_regex = re.compile(r'(\w+)\.(\w+)')
    simple_column_regex = re.compile(r'\b(\w+)\b')

    # Extract table aliases
    tables = table_alias_regex.findall(sql_query)
    if tables:
        table_dict = {alias: table for table, alias in tables}
    else:
        # Extract table names directly if no aliases are found
        table_names = table_name_regex.findall(sql_query)
        table_dict = {table: table for table in table_names}

    # Initialize the column map
    column_map = defaultdict(lambda: {'select': [], 'where': [], 'order': []})

    # Extract SELECT columns
    select_clause_regex = re.compile(r'SELECT\s+(.*?)\s+FROM', re.IGNORECASE | re.DOTALL)
    select_clause = select_clause_regex.findall(sql_query)
    if select_clause:
        if column_regex.findall(select_clause[0]):
            select_columns = column_regex.findall(select_clause[0])
        else:
            select_columns = simple_column_regex.findall(select_clause[0])
            select_columns = [(list(table_dict.keys())[0], column) for column in select_columns]
        for alias, column in select_columns:
            if alias in table_dict:
                table_name = table_dict[alias]
                column_map[table_name]['select'].append(column)

    # Extract WHERE clause columns
    is_order_by = re.compile(r'ORDER\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    is_order_by_exit = is_order_by.findall(sql_query)

    is_group_by = re.compile(r'GROUP\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    is_group_by_exit = is_group_by.findall(sql_query)
    not_order_group = False
    
    if is_order_by_exit:
        where_clause_regex = re.compile(r'WHERE\s+(.*?)(ORDER\s+BY|$)', re.IGNORECASE | re.DOTALL)
    elif is_group_by_exit:
        where_clause_regex = re.compile(r'WHERE\s+(.*?)(GROUP\s+BY|$)', re.IGNORECASE | re.DOTALL)
    else :
        where_clause_regex = re.compile(r'WHERE\s+(.*)', re.IGNORECASE | re.DOTALL)
        not_order_group = True

    where_clause = where_clause_regex.findall(sql_query)
    if where_clause:
        if not_order_group == False:
            where_columns = where_clause[0][0]
        else:
            where_columns = where_clause[0]
        pattern = re.compile(r'\b(\w+)\s*=')
        columns_with_where = pattern.findall(where_columns)
        where_columns = [(list(table_dict.keys())[0], column) for column in columns_with_where]
   
        for alias, column in where_columns:
            if alias in table_dict:
                table_name = table_dict[alias]
                column_map[table_name]['where'].append(column)

    # Extract ORDER BY columns
    order_by_clause_regex = re.compile(r'ORDER\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    order_by_clause = order_by_clause_regex.findall(sql_query)
    if order_by_clause:

        if column_regex.findall(order_by_clause[0]):
            order_columns = column_regex.findall(order_by_clause[0])
        else:
            order_columns = simple_column_regex.findall(order_by_clause[0])
            order_columns = [(list(table_dict.keys())[0], column) for column in order_columns]
        for alias, column in order_columns:
            if alias in table_dict:
                table_name = table_dict[alias]
                column_map[table_name]['order'].append(column)

    # Removing duplicates and preserving the order in select columns
    for table_name in column_map:
        column_map[table_name]['select'] = list(dict.fromkeys(column_map[table_name]['select']))
        column_map[table_name]['where'] = list(dict.fromkeys(column_map[table_name]['where']))
        column_map[table_name]['order'] = list(dict.fromkeys(column_map[table_name]['order']))
    return column_map

