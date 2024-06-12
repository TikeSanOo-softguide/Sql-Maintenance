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