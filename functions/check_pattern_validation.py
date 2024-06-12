import re

def has_aliases(sql_query):
    # Extract the part of the query after the SELECT and before the FROM
    select_clause = re.search(r"SELECT(.*?)FROM", sql_query, re.IGNORECASE | re.DOTALL)
    if select_clause:
        select_clause_text = select_clause.group(1)
        # Check if there's any "AS" in the select clause, indicating an alias
        return bool(re.search(r"\bAS\b", select_clause_text, re.IGNORECASE))
    return False

def validate_sql_pattern1(sql_query):
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