import re

def has_select_query(query):
    select_pattern = r'^\s*SELECT\b'
    return bool(re.match(select_pattern, query, re.IGNORECASE))

def has_insert_query(query):
    insert_pattern = r'^\s*INSERT\s+INTO\b'
    return bool(re.match(insert_pattern, query, re.IGNORECASE))

def has_update_query(query):
    update_pattern = r'^\s*UPDATE\b'
    return bool(re.match(update_pattern, query, re.IGNORECASE))

def has_delete_query(query):
    delete_pattern = r'^\s*DELETE\s+FROM\b'
    return bool(re.match(delete_pattern, query, re.IGNORECASE))

def validate_sql_pattern1(sql_query):
    # Check for exactly one occurrence of SELECT/select (case-insensitive)
    select_pattern = re.compile(r'\bselect\b', re.IGNORECASE)
    select_matches = select_pattern.findall(sql_query)
    # Check for absence of JOIN/join (case-insensitive)
    join_pattern = re.compile(r'\bjoin\b', re.IGNORECASE)
    join_matches = join_pattern.findall(sql_query)

    from_select_pattern = re.compile(r"FROM\s*\(\s*SELECT\s.*?\)\s*AS\s", re.IGNORECASE | re.DOTALL)
    from_select_matches = from_select_pattern.findall(sql_query)
   
    # Validate conditions
    query_pattern = ''
    if len(select_matches) == 1 and len(join_matches) == 0:
        query_pattern = 'simple select'
    elif len(select_matches) == 1 and len(join_matches) != 0:
        query_pattern = 'simple join'     
    elif len(select_matches) == 2 and len(from_select_matches) == 1:
        query_pattern = 'subquery from select'     
    return query_pattern
    
def validate_sql_pattern2(sql_query):
    # Check for exactly one occurrence of SELECT/select (case-insensitive)
    select_pattern = re.compile(r'\bselect\b', re.IGNORECASE)
    select_matches = select_pattern.findall(sql_query)
    # Check for absence of JOIN/join (case-insensitive)
    join_pattern = re.compile(r'\bjoin\b', re.IGNORECASE)
    join_matches = join_pattern.findall(sql_query)
    
    # Validate conditions
    if len(select_matches) == 1 and len(join_matches) == 0:
        return True
    else:
        return False 
