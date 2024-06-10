import re

def has_aliases(sql_query):
    # Extract the part of the query after the SELECT and before the FROM
    select_clause = re.search(r"SELECT(.*?)FROM", sql_query, re.IGNORECASE | re.DOTALL)
    if select_clause:
        select_clause_text = select_clause.group(1)
        # Check if there's any "AS" in the select clause, indicating an alias
        return bool(re.search(r"\bAS\b", select_clause_text, re.IGNORECASE))
    return False