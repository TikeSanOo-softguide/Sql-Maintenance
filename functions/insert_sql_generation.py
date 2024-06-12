import logging

def insert_table_column_names(insert_query):
    # Extract table name
    table_name_start = insert_query.index("INTO") + len("INTO")
    table_name_end = insert_query.index("(")
    table_name = insert_query[table_name_start:table_name_end].strip()

    # Extract column names
    columns_start = insert_query.index("(") + 1
    columns_end = insert_query.index(")")
    columns_str = insert_query[columns_start:columns_end].strip()
    columns = [col.strip() for col in columns_str.split(",")]
    return table_name,columns

