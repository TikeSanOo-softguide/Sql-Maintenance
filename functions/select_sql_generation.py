import re
from collections import defaultdict

def aliase_table_column_names(sql_query):
    # Regular expressions to match table aliases and columns
    table_alias_regex = re.compile(r'\b(\w+)\s+AS\s+(\w+)\b', re.IGNORECASE)
    column_regex = re.compile(r'(\w+)\.(\w+)')
    
    # Extract table aliases
    tables = table_alias_regex.findall(sql_query)
    table_dict = {alias: table for table, alias in tables}
    
    # Initialize the column map
    column_map = defaultdict(lambda: {'select': [], 'where': [], 'order': []})
    
    # Extract SELECT columns
    select_clause_regex = re.compile(r'SELECT\s+(.*?)\s+FROM', re.IGNORECASE | re.DOTALL)
    select_clause = select_clause_regex.findall(sql_query)
    if select_clause:
        select_columns = column_regex.findall(select_clause[0])
        for alias, column in select_columns:
            if alias in table_dict:
                table_name = table_dict[alias]
                column_map[table_name]['select'].append(column)
    
    # Extract WHERE clause columns
    where_clause_regex = re.compile(r'WHERE\s+(.*?)(ORDER\s+BY|$)', re.IGNORECASE | re.DOTALL)
    where_clause = where_clause_regex.findall(sql_query)
    if where_clause:
        where_columns = column_regex.findall(where_clause[0][0])
        for alias, column in where_columns:
            if alias in table_dict:
                table_name = table_dict[alias]
                column_map[table_name]['where'].append(column)
    
    # Extract ORDER BY columns
    order_by_clause_regex = re.compile(r'ORDER\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    order_by_clause = order_by_clause_regex.findall(sql_query)
    if order_by_clause:
        order_columns = column_regex.findall(order_by_clause[0])
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

def not_aliase_table_column_names(sql_query):
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
    where_clause_regex = re.compile(r'WHERE\s+(.*?)(ORDER\s+BY|$)', re.IGNORECASE | re.DOTALL)
    where_clause = where_clause_regex.findall(sql_query)
    if where_clause:
        if column_regex.findall(where_clause[0][0]):
            where_columns = column_regex.findall(where_clause[0][0])
        else:
            where_columns = simple_column_regex.findall(where_clause[0][0])
            where_columns = [(list(table_dict.keys())[0], column) for column in where_columns]
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

