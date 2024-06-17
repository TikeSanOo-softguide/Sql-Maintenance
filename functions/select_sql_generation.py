import re
from collections import defaultdict
import functions as fnc

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
    where_columns = ''
    if where_clause:
        if not_order_group == False:
            where_columns = where_clause[0][0]
        else:
            where_columns = where_clause[0]

    return where_columns 

def extract_table_column_names(sql_query):
      # Regular expressions to match table aliases, columns, and table names
    table_alias_regex = re.compile(r'\b(\w+)\s+AS\s+(\w+)\b', re.IGNORECASE)
    table_name_regex = re.compile(r'FROM\s+(\w+)', re.IGNORECASE)
    column_regex = re.compile(r'(\w+)\.(\w+)')
    simple_column_regex = re.compile(r'\b(\w+)\b')

    # case_when_regex = re.compile(r"CASE\s+(.*?)\s+(?:WHEN.*?(?=CASE|\s+END)|AS\s+\w+|END)", re.IGNORECASE | re.DOTALL)
    case_when_regex = re.compile(r"CASE\s+(.*?)\s+(?:WHEN.*?(?=CASE|\bEND\b)|AS\s+\w+|END\s+AS\s+\w+|END)", re.IGNORECASE | re.DOTALL)
    case_when_sql = case_when_regex.findall(sql_query)
    no_case_when_sql= case_when_regex.sub("", sql_query)
    built_in_functions_regex = re.compile(r"\b(SOME|COUNT|SUM|AVERAGE|MIN|MAX|ISNULL)\b", re.IGNORECASE)

    # sql_query_res = built_in_functions_regex.sub("", case_when_sql[0])
    select_columns = []
 
    # if sql_query_res:
    #     sql_query = no_case_when_sql
    #     sql_pattern = remove_parentheses(sql_query_res)
    #     comparison_pattern = re.compile(r'(\b[\w.]+)\s*(=|!=|<>|>|<|>=|<=)\s*([\w.]+)(?:\s*(?:THEN\s*([\w.]+)\s*(=|!=|<>|>|<|>=|<=)\s*([\w.]+)|\s*(?:ELSE\s*([\w.]+)\s*))|$)')
    #     matches = comparison_pattern.findall(sql_pattern)
    #     if matches:
    #         for match in matches[0]:
    #             is_words = fnc.is_word(match)

    #             if is_words and match != '':
    #                 select_columns.append(match)

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
            select_columns1 = column_regex.findall(select_clause[0])
        else:
            select_columns1 = simple_column_regex.findall(select_clause[0])
            select_columns1 = [(list(table_dict.keys())[0], column) for column in select_columns1]
        
        # select_columns1[0] += tuple(select_columns)
        # print(select_columns1[0])
        # exit()
 
        for alias, column in select_columns1:
            if alias in table_dict:
                table_name = table_dict[alias]
                column_map[table_name]['select'].append(column)          


    # Extract WHERE clause columns
    asc_desc_regex =  re.compile(r"(?i)\bASC\b|\bDESC\b", re.IGNORECASE | re.DOTALL)
    no_asc_desc_query = re.sub(asc_desc_regex, '', sql_query)
    order_by_regex = re.compile(r'ORDER\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    order_by_clause = order_by_regex.findall(no_asc_desc_query)

    group_by_regex = re.compile(r'GROUP\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    group_by_clause = group_by_regex.findall(sql_query)
    columns_with_where = []

    where_columns = extract_where_column_names(sql_query,order_by_clause,group_by_clause)

    pattern = re.compile(r'\b(\w+)\s*=')
    columns_with_where = pattern.findall(where_columns)
    columns = [(list(table_dict.keys())[0], column) for column in columns_with_where]

    for alias, column in columns:
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

    result = extract_where_column_names(sql_query,order_by_clauses,group_by_clauses)
    pattern = re.compile(r'(\b[\w.]+)\s*=')
    columns_with_where = pattern.findall(result) 
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
         
def ensure_unique_values(data_dict):
    for table, columns in data_dict.items():
        for key, values in columns.items():
            seen = set()
            unique_ordered_values = [x for x in values if not (x in seen or seen.add(x))]
            data_dict[table][key] = unique_ordered_values

    return data_dict

def combine_data_list(data_list):
    combined_data = defaultdict(lambda: defaultdict(list))
    key_name = ''
    for data_dict in data_list:
        for key in data_dict:
            if key_name == '':
                key_name = key
            else:   
                key_name += ',' + key

    for data_dict in data_list:        
        for alias, columns_info in data_dict.items():
            for key, value in columns_info.items():
                combined_data[key_name][key].extend(value)

    combined_data[key_name]['where'] = list(set(combined_data[key_name]['where']))
    combined_data[key_name]['order'] = list(set(combined_data[key_name]['order']))
    column_map = ensure_unique_values(combined_data)
    return column_map

def remove_parentheses(content):
    parentheses_pattern = re.compile(r'[\(\)]')
    while parentheses_pattern.search(content):
        content = parentheses_pattern.sub('', content)
    return content

def extract_select_query(sql_query):
    select_pattern = re.compile(r"SELECT\s.*?FROM\s.*?(?=SELECT|$)", re.IGNORECASE | re.DOTALL)
    select_matches = select_pattern.findall(sql_query)
    return select_matches

def extract_table_column_names_with_sub_pat1(sql_query):
    join_regex = re.compile(r'\bjoin\b', re.IGNORECASE)
    join_matches = join_regex.findall(sql_query)
    column_map = {}

    parts = re.split(r"(?i)SELECT", sql_query)
    res_query = "SELECT " + parts[2]
    if join_matches:
        column_map = extract_table_column_names_with_join(res_query)
        column_map = ensure_unique_values(column_map)
    else:
        column_map = extract_table_column_names(sql_query)

    return column_map

def extract_table_column_names_with_sub_pat2(sql_query):
    join_regex = re.compile(r'\bjoin\b', re.IGNORECASE)
    join_matches = join_regex.findall(sql_query)
    select_matches = extract_select_query(sql_query)
    temp_column_map = []

    if join_matches:
        for select_match in select_matches:
            column_map = extract_table_column_names_with_join(select_match)
            temp_column_map.append(column_map)
        column_map = combine_data_list(temp_column_map)
    else:
        column_map = extract_table_column_names(sql_query)

    return column_map

