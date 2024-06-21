import re
from collections import defaultdict
import functions as fnc
import sqlparse
from sqlparse.sql import IdentifierList,TokenList, Identifier, Parenthesis, Token
from sqlparse.tokens import Keyword, DML, Punctuation

subqueries = []

def analysis_case_when(sql_query):
    case_pattern = re.compile(r'''
        CASE\s+                   # Match the opening CASE
        WHEN\s+                   # Match the WHEN clause
        .*?                       # Non-greedy match of any characters
        END\s+AS\s+\w+            # Match the END AS column_name
        ,                         # Match the comma at the end
    ''', re.IGNORECASE | re.DOTALL | re.VERBOSE)

    parsed_query = sqlparse.format(sql_query, reindent=True, keyword_case='upper')
    cleaned_query = re.sub(case_pattern, '', parsed_query)
    return cleaned_query

def query_analysis(sql_query):
    case_when_regex = re.compile(r"CASE\s+(.*?)\s+(?:WHEN.*?(?=CASE|\bEND\b)|AS\s+\w+|END\s+AS\s+\w+|END)", re.IGNORECASE | re.DOTALL)
    check_case_when_regex = re.compile(r"WHEN.*?END", re.IGNORECASE | re.DOTALL)
    case_when_count_check = check_case_when_regex.findall(sql_query)
    case_when_sql = []
    
    if len(case_when_count_check) == 1:
        case_when_sql = case_when_regex.findall(sql_query)
    elif len(case_when_count_check) > 1:
        sql_query = analysis_case_when(sql_query)
    
    built_in_functions_regex = re.compile(r"\b(SOME|COUNT|SUM|AVERAGE|MIN|MAX|ISNULL|MONTH|DAY|ISNUMERIC|LEFT)\b", re.IGNORECASE)
    
    select_columns = [] 
    if case_when_sql:
        sql_query_res = built_in_functions_regex.sub("", case_when_sql[0])
        sql_pattern = remove_parentheses(sql_query_res)
        sql_query= case_when_regex.sub("", sql_query)    
        if sql_query_res:
            pattern = re.compile(r'(\b[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\b)', re.IGNORECASE | re.DOTALL)
            comparison_pattern = re.compile(r'(\b[\w.]+)\s*(=|!=|<>|>|<|>=|<=)\s*([\w.]+)(?:\s*(?:THEN\s*([\w.]+)\s*(=|!=|<>|>|<|>=|<=)\s*([\w.]+)|\s*(?:ELSE\s*([\w.]+)\s*))|$)', re.IGNORECASE | re.DOTALL)
            matches_query = pattern.findall(sql_pattern)
            comparison_pattern2 = re.compile(r'(\b[\w.]+)\s*(=|<>|!=|>=|<=|>|<|!<|!>|-=|\*=|/=|%=|&=|\^=|\|=|\+)\s*(\b[\w.]+)')
            matches = []
            if not matches_query:
                matches = comparison_pattern.findall(sql_pattern)
            if not matches_query:
                matches = comparison_pattern2.findall(sql_pattern)

            if matches:
                for match in matches:
                    if isinstance(match, tuple):
                        for result in match:          
                            if result.strip().isidentifier() and result.strip() != '':
                                select_columns.append(result)
                    else:
                        if not match.isdigit() and match != '':
                            select_columns.append(match)
    else:
        sql_query_res = built_in_functions_regex.sub("", sql_query)
                    
    sql_query = built_in_functions_regex.sub("", sql_query)
    return sql_query,select_columns

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

def remove_row_number_segment(sql_query):
    pattern1 = re.compile(r"ROW_NUMBER\(\) OVER\(ORDER BY .*?\) AS rownum,", re.IGNORECASE | re.DOTALL)
    pattern2 = re.compile(r'ROW_NUMBER\(\) OVER\(.*?\) AS rownum', re.IGNORECASE | re.DOTALL)
    result = pattern1.findall(sql_query)
    result2 = pattern2.findall(sql_query)
    modified_query = sql_query
    
    if result:
        modified_query = pattern1.sub('', sql_query)
    if result2:
        modified_query = pattern2.sub('', sql_query)

    return modified_query


def extract_table_column_names(sql_query):
    sql_query, columns= query_analysis(sql_query)
    sql_query = remove_row_number_segment(sql_query)
    table_name_regex = re.compile(r'(?:FROM)\s+([\w\.]+(?:\s+AS\s+\w+)?)', re.IGNORECASE)
    join_table_name_regex = re.compile(r'(JOIN)\s+(.+?)\s+(ON|$)', re.IGNORECASE)
    select_table_name = table_name_regex.findall(sql_query)
    built_in_functions_regex = re.compile(r"\b(SOME|COUNT|SUM|AVERAGE|MIN|MAX|ISNULL|MONTH|DAY|ISNUMERICLEFT|LEFT|IIF)\b", re.IGNORECASE)
    
    join_table_names = join_table_name_regex.findall(sql_query)
    join_table_list = []
    column_list = []
    
    column_map = defaultdict(lambda: {'select': [], 'where': [], 'join': [], 'order': [], 'group': []})

    if select_table_name:
        for col in columns:
            no_built_in_col = built_in_functions_regex.sub("", col)
            if no_built_in_col:
                col = no_built_in_col
            col = remove_parentheses(col)
            column = col.strip().replace(' ', '').replace('\n', '').replace('\t', '')
            column_map[select_table_name[0]]['select'].append(column)

        select_clause_regex = re.compile(r'SELECT\s+(.*?)\s+FROM', re.IGNORECASE | re.DOTALL)
        select_clause = select_clause_regex.findall(sql_query)
        if select_clause:
            select_regex = re.compile(r'([^,]+)')
            select_column = select_regex.findall(select_clause[0])
            if select_column:
                for match in select_column:
                    cleaned_match = re.sub(r'\s+AS\s+\w+', '', match, flags=re.IGNORECASE).strip()
                    no_built_in_col = built_in_functions_regex.sub("", cleaned_match)
                    if no_built_in_col:
                       cleaned_match = no_built_in_col 
                    cleaned_match = remove_parentheses(cleaned_match) 
                    cleaned_match = cleaned_match.strip().replace(' ', '').replace('\n', '').replace('\t', '')
                    column_map[select_table_name[0]]['select'].append(cleaned_match)

    if join_table_names:
       for table_name in join_table_names:
           join_table_list.append(table_name[1])
           column_map[select_table_name[0]]['join'].append(table_name[1])

    join_column_regex = re.compile(r'ON\s*\((.*?)\)', re.IGNORECASE | re.DOTALL)
    join_clauses = join_column_regex.findall(sql_query)

    if len(join_clauses) > 0:
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
    asc_desc_regex =  re.compile(r"(?i)\b ASC\b|\b DESC\b", re.IGNORECASE | re.DOTALL)
    no_asc_desc_query = re.sub(asc_desc_regex, '', sql_query)
    order_by_regex = re.compile(r'ORDER\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    order_by_clauses = order_by_regex.findall(no_asc_desc_query)
    # order_cloumn_regex = re.compile(r'(\b[\w.]+)\s*(=|!=|<>|>|<|>=|<=|IS NULL|IS NOT NULL)\s*([\w.]+)')
    # result = order_cloumn_regex.findall(order_by_clauses[0])

    group_by_regex = re.compile(r'GROUP\s+BY\s+(.*)', re.IGNORECASE | re.DOTALL)
    group_by_clauses = group_by_regex.findall(sql_query)

    result = extract_where_column_names(sql_query,order_by_clauses,group_by_clauses)
    where_regex = re.compile(r'(\b[\w\.]+)\s*(?=\s*=\s*|<>|!=|>=|<=|>|<|!<|!>|\+=|-=|\*=|/=|%=|&=|\^=|\|=)')
    between_regex = re.compile(r"(.*?)\s+BETWEEN\s+(.*?)\s+AND\s+(.*)")
    # pattern = re.compile(r"\s*AND\s*\([\w\.]+\s+BETWEEN\s+'.*?'\s+AND\s+'.*?'\)", re.IGNORECASE | re.DOTALL)
    
    columns_with_between = between_regex.findall(result)
    no_between_sql = between_regex.sub('',result)
    columns_with_where = where_regex.findall(no_between_sql)
    # columns_with_between2 = pattern.findall(result)
    split_between_regex = re.compile(r"\b(\w+\.\w+)\b")

# Extracting the matching parts
    extracted_data = []
    for col in columns_with_between:
        matches = split_between_regex.findall(col[0])
        if matches:
            extracted_data.extend(matches)

    for column in columns_with_where:
        column_map[select_table_name[0]]['where'].append(column.strip())
    for column in extracted_data:
        column_map[select_table_name[0]]['where'].append(column.strip())
   
    # Extract Order By Or Group By clause columns    
    if order_by_clauses:

        for column in order_by_clauses:
            # order_cloumn_regex = re.compile(r'(\b[\w.]+)\s*(=|!=|<>|>|<|>=|<=|IS NULL|IS NOT NULL)\s*([\w.]+)')
            order_column_regex = re.compile(r'(\b[\w.]+)\s*(=|!=|<>|>|<|>=|<=|IS NULL|IS NOT NULL)\s*', re.IGNORECASE)
            pattern = r'\bAND\b|\bOR\b|\bORDER BY\b'
            col_parts = re.split(pattern, column, flags=re.IGNORECASE)
            
            if col_parts:
                for col in col_parts:
                    col_results = order_column_regex.search(col.strip())
                    if col_results:
                        col_result = col_results.group(1).replace(')', '')
                        column_map[select_table_name[0]]['order'].append(col_result.strip())
                    else:
                        column_map[select_table_name[0]]['order'].append(col.strip())
            else:
                column = column[0].replace(')', '')
                column_map[select_table_name[0]]['order'].append(column.strip())
                    
    if group_by_clauses:
        for column in group_by_clauses:
            column = column.replace(')', '')
            column = column.strip().replace(' ', '').replace('\n', '').replace('\t', '')
            column_map[select_table_name[0]]['group'].append(column)    

    column_map = ensure_unique_values(column_map)
    return column_map
        
def ensure_unique_values(data_dict):
    for table, columns in data_dict.items():
        for key, values in columns.items():
            seen = set()
            unique_ordered_values = []
            for value in values:
                split_values = value.split('+')
                if len(split_values) > 1:
                    for value in split_values:
                        value = value.replace("=", '')
                        value = value.replace("'", "")
                        if value and not value.isdigit() and value.strip() and value != "''" and value != "':'" and value != ":" and value != '*' and value not in seen:
                            seen.add(value)
                            unique_ordered_values.append(value)
                else:
                    value = value.replace("=", "")
                    value = value.replace("'", "")
                    if value and not value.isdigit() and value.strip() and value != "''" and value != '*' and value != "':'" and value != ":"  and value not in seen:
                        seen.add(value) 
                        unique_ordered_values.append(value)          
            data_dict[table][key] = unique_ordered_values

    return data_dict

def combine_data_list(data_list):
    remove_pattern = re.compile(r'\b\d+\+[^\s]+\b', re.IGNORECASE | re.DOTALL)
 
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
                cleaned_values = [remove_pattern.sub('', v) for v in value]
                if cleaned_values:
                    combined_data[key_name][key].extend(cleaned_values)
                else:    
                    combined_data[key_name][key].extend(value)

    combined_data[key_name]['select'] = list(set(combined_data[key_name]['select']))
    combined_data[key_name]['where'] = list(set(combined_data[key_name]['where']))
    combined_data[key_name]['order'] = list(set(combined_data[key_name]['order']))
    combined_data[key_name]['group'] = list(set(combined_data[key_name]['group']))
    column_map = ensure_unique_values(combined_data)
    return column_map

def remove_parentheses(content):
    parentheses_pattern = re.compile(r'[\(\)]')
    while parentheses_pattern.search(content):
        content = parentheses_pattern.sub('', content)
    return content
                
def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def extract_subqueries(tokens):
    """
    Recursively extract subqueries from the token list.
    """
    subqueries = []
    for token in tokens:
        if is_subselect(token):
            subqueries.append(token)
            subqueries.extend(extract_subqueries(token.tokens))
        elif token.is_group:
            subqueries.extend(extract_subqueries(token.tokens))
    return subqueries

def extract_nested_subqueries(sql):
    """
    Parse the SQL and extract all nested subqueries.
    """
    parsed = sqlparse.parse(sql)
    statement = parsed[0]
    return extract_subqueries(statement.tokens)

def remove_nested_subqueries(remove_subqueries,subquery_str):
    for remove_subquery in remove_subqueries:
        remove_subquery = str(remove_subquery).strip()
        
        if remove_subquery != subquery_str:  
            remove_subquery = remove_subquery
            subquery_str = subquery_str.replace(remove_subquery, "")
            
    return subquery_str

def generate_sql_fragments(sql):
    nested_subqueries = extract_nested_subqueries(sql)
    select_count_regex = re.compile(r'\bselect\b', re.IGNORECASE)
    nested_subqueries.append(sql)
    sql_fragments = []
    
    # Remove subqueries from SQL and collect fragments without subqueries
    for subquery in nested_subqueries:
        subquery_str = str(subquery).strip()    
        select_matches = select_count_regex.findall(subquery_str)

        if len(select_matches) != 1:     
            subquery_str = remove_nested_subqueries(nested_subqueries,subquery_str)  

        if subquery_str not in sql_fragments:
            sql_fragments.append(subquery_str)
            sql = sql.replace(subquery_str, '')
        
    return sql_fragments

def extract_table_column_names_with_sub_pat(sql_query):
    column_map = {}
    temp_column_map = []
    sql_fragments  = generate_sql_fragments(sql_query)
    if sql_fragments:          
        for subquery in sql_fragments:
            is_exists_table_name = fnc.is_table_name_present(subquery)
            if is_exists_table_name:
                column_map1 = extract_table_column_names(subquery)
                temp_column_map.append(column_map1)

    if temp_column_map:
        temp_column_map.append(column_map)
        column_map = combine_data_list(temp_column_map)
    subqueries.clear()

    return column_map
