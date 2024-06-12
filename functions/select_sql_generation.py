import re
from collections import defaultdict
def extract_where_column_names(sql_query,is_group_by_exit,is_order_by_exit):
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
        pattern = re.compile(r'(\b[\w.]+)\s*=')
        columns_with_where = pattern.findall(where_columns)

    return columns_with_where 

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
    column_map = defaultdict(lambda: {'select': [], 'where': [], 'order': [], 'group': []})

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
    order_by_regex = re.compile(r'ORDER\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    order_by_clause = order_by_regex.findall(sql_query)

    group_by_regex = re.compile(r'GROUP\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    group_by_clause = group_by_regex.findall(sql_query)

    columns_with_where = extract_where_column_names(sql_query,order_by_clause,group_by_clause) 
    where_columns = [(list(table_dict.keys())[0], column) for column in columns_with_where]    

    for alias, column in where_columns:
        if alias in table_dict:
            table_name = table_dict[alias]
            column_map[table_name]['where'].append(column)

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

    if group_by_clause:    
        if column_regex.findall(group_by_clause[0]):
            group_columns = column_regex.findall(group_by_clause[0])
        else:
            group_columns = simple_column_regex.findall(group_by_clause[0])
            group_columns = [(list(table_dict.keys())[0], column) for column in group_columns]
        for alias, column in group_columns:
            if alias in table_dict:
                table_name = table_dict[alias]
                column_map[table_name]['group'].append(column)

    # Removing duplicates and preserving the order in select columns
    for table_name in column_map:
        column_map[table_name]['select'] = list(dict.fromkeys(column_map[table_name]['select']))
        column_map[table_name]['where'] = list(dict.fromkeys(column_map[table_name]['where']))
        column_map[table_name]['order'] = list(dict.fromkeys(column_map[table_name]['order']))
        column_map[table_name]['group'] = list(dict.fromkeys(column_map[table_name]['group']))

    return column_map


def extract_table_column_names_with_join(sql_query):
    table_name_regex = re.compile(r'(?:FROM)\s+([\w\.]+(?:\s+AS\s+\w+)?)', re.IGNORECASE)
    join_table_name_regex = re.compile(r'(JOIN)\s+(.+?)\s+(ON|$)', re.IGNORECASE)
    select_table_name = table_name_regex.findall(sql_query)
    join_table_names = join_table_name_regex.findall(sql_query)

    join_table_list = []
    column_list = []
    column_map = defaultdict(lambda: {'select': [], 'where': [], 'join': [], 'order': [], 'group': []})
    
    select_clause_regex = re.compile(r'SELECT\s+(.*?)\s+FROM', re.IGNORECASE | re.DOTALL)
    select_clause = select_clause_regex.findall(sql_query)
    select_regex = re.compile(r'([^,]+)')
    select_column = select_regex.findall(select_clause[0])

    for match in select_column:
        cleaned_match = re.sub(r'\s+AS\s+\w+', '', match, flags=re.IGNORECASE).strip()  
        column_map[select_table_name[0]]['select'].append(cleaned_match)
        
    if join_table_names:
       for table_name in join_table_names:
           join_table_list.append(table_name[1])
           column_map[select_table_name[0]]['join'].append(table_name[1])

    join_column_regex = re.compile(r'ON\s*\((.*?)\)', re.IGNORECASE | re.DOTALL)
    join_clauses = join_column_regex.findall(sql_query)
    if len(join_clauses) == 0:
        join_condition_regex = re.compile(r'ON\s+([^\s].*?)(?=\s*(?:JOIN|WHERE|GROUP BY|ORDER BY|$))', re.IGNORECASE | re.DOTALL)
        join_clauses = join_condition_regex.findall(sql_query)

    for join_clause in join_clauses:
            pattern = re.compile(r'(\b[\w\.]+)\s*=\s*([\w\.]+\b)')
            matches = pattern.findall(join_clause)
            for match in matches:
                if match[0] not in column_list:
                    column_map[select_table_name[0]]['select'].append(match[0])
                if match[1] not in column_list:
                    column_map[select_table_name[0]]['select'].append(match[1])

    # Extract WHERE clause columns
    order_by_regex = re.compile(r'ORDER\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    order_by_clauses = order_by_regex.findall(sql_query)

    group_by_regex = re.compile(r'GROUP\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    group_by_clauses = group_by_regex.findall(sql_query)

    columns_with_where = extract_where_column_names(sql_query,order_by_clauses,group_by_clauses)    
    for column in columns_with_where:
        column_map[select_table_name[0]]['where'].append(column)
   
    # Extract Order By Or Group By clause columns    
    if order_by_clauses:
        for column in order_by_clauses:
            column_map[select_table_name[0]]['order'].append(column)
    if group_by_clauses:
        for column in group_by_clauses:
            column_map[select_table_name[0]]['group'].append(column)

    return column_map         
