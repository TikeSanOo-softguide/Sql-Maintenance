import re

def has_select_query(query):
    select_pattern = re.compile(r'\bselect\b', re.IGNORECASE)
    return bool(select_pattern.search(query))

def has_insert_query(query):
    insert_pattern = r'^\s*INSERT\s+INTO\b'
    return bool(re.match(insert_pattern, query, re.IGNORECASE))

def has_update_query(query):
    update_pattern = r'^\s*UPDATE\b'
    return bool(re.match(update_pattern, query, re.IGNORECASE))

def has_delete_query(query):
    delete_pattern = r'^\s*DELETE\s+FROM\b'
    return bool(re.match(delete_pattern, query, re.IGNORECASE))

def is_table_name_present(query):
    # Regex to find the table name after the FROM clause
    pattern = r"\bFROM\s+([a-zA-Z_][\w]*)\b"
    match = re.search(pattern, query, re.IGNORECASE)
    
    if match:
        return True
    
    return False

def validate_sql_pattern(query):
    # Check for exactly one occurrence of SELECT/select (case-insensitive)
    select_pattern = re.compile(r'\bselect\b', re.IGNORECASE)
    select_matches = select_pattern.findall(query)
    
    # Check for absence of JOIN/join (case-insensitive)
    join_pattern = re.compile(r'\bjoin\b', re.IGNORECASE)
    join_matches = join_pattern.findall(query)

    # Validate conditions
    query_pattern = ''
    if len(select_matches) == 1 and len(join_matches) == 0:
        query_pattern = 'simple select'
    elif len(select_matches) == 1 and len(join_matches) != 0:
        query_pattern = 'simple join'     
    elif len(select_matches) > 1:
        query_pattern = 'subquery select'
    else:
        query_pattern = 'Not sql exists' 
    
    return query_pattern

# Function to extract column names
def extract_column_name(col):
    # Split by dot and take the last part
    parts = col.split('.')
    column_name = parts[-1]
    return column_name