import os
import re
import time
import logging
import configparser
import functions as fnc
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Create a folder if it doesn't exist
folder_path = "logs"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Set up logging
log_file = os.path.join(folder_path, "query_analysis.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(message)s', filemode='w')

config = configparser.ConfigParser()
config.read('config.ini')
rootDir = Path(config['DEFAULT']['rootDir'])
logDir = Path("logfile")
cfquery_pattern = re.compile(r'<cfquery\b.*?</cfquery>', re.DOTALL | re.IGNORECASE)
file_name = []

logDir.mkdir(exist_ok=True)
error_log_path = logDir / 'error_log.txt'

def remove_all_comments(content):
    # Regular expression pattern to match nested HTML comments
    comment_pattern = re.compile(r'<!---(.*?)--->', re.DOTALL)
    nested_comment_pattern = re.compile(r'<!---(?:(?!<!---)(?:.|\\n))*?--->', re.DOTALL)
    
    # Remove nested comments
    cleaned_nested_content = nested_comment_pattern.sub('', content)
    cleaned_content = comment_pattern.sub('', cleaned_nested_content)
    return cleaned_content

def remove_coldfusion_syntax(sql_query):
    cf_tags_pattern = re.compile(r'<\/?cf[^>]*>', re.IGNORECASE)
    cf_comments_pattern = re.compile(r'<!---.*?--->', re.DOTALL)
    cf_expressions_pattern = re.compile(r'#[^#]*#')
    sql_query = re.sub(cf_tags_pattern, '', sql_query)
    sql_query = re.sub(cf_comments_pattern, '', sql_query)
    sql_query = re.sub(cf_expressions_pattern, '', sql_query)
    return sql_query

def process_file(file_path):
    try:
        with file_path.open('r', encoding='latin-1') as file:
            file_contents = file.read()
            uncommented_contents = remove_all_comments(file_contents)  # Remove comments first
            matches = cfquery_pattern.findall(uncommented_contents)  # Find <cfquery> tags
            
            if matches:
                file_name.append(file_path.name)
                logging.info(file_path)
                for match in matches:
                    #Remove <cfquery> tags from query
                    query_pattern = re.compile(r'<cfquery.*?>(.*?)</cfquery>', re.DOTALL)
                    result = query_pattern.search(match)
                    if result:
                        sql_query = result.group(1).strip()
                        is_select = fnc.has_select_query(sql_query)
                        is_insert = fnc.has_insert_query(sql_query)
                        is_update = fnc.has_update_query(sql_query)
                        is_delete = fnc.has_delete_query(sql_query)
                        if is_select:
                            cleaned_sql_query = remove_coldfusion_syntax(sql_query)
                            pattern1 = fnc.validate_sql_pattern1(cleaned_sql_query)
                            if pattern1 == 'simple select':
                                column_map = fnc.extract_table_column_names(cleaned_sql_query)
                            elif pattern1 == 'simple join':
                                column_map = fnc.extract_table_column_names_with_join(cleaned_sql_query)
                            elif pattern1 == 'subquery from select':
                                column_map = fnc.extract_table_column_names_with_sub_pat1(cleaned_sql_query)
                            for table, columns in column_map.items():
                                if pattern1 == 'simple join':
                                    join_tables = ', '.join(columns['join'])
                                    table += ', ' + join_tables

                                logging.info(f"Select Table Name: {table}")                                
                                logging.info(f"Select Columns: {columns['select']}")
                                logging.info(f"Where Columns: {columns['where']}")
                                logging.info(f"Order Columns: {columns['order']}")
                                logging.info(f"Group Columns: {columns['group']}\n")
                        if is_insert:
                            table_name, columns = fnc.insert_table_column_names(sql_query)
                            if table_name and columns:
                                # Log table name and column names
                                logging.info("Insert Table Name: %s", table_name)
                                logging.info("Insert Columns: %s \n", columns)
                        if is_update:
                            # Extract table name, set columns, and where columns
                            table_name, set_columns, where_columns = fnc.update_table_column_names(sql_query)
                            if table_name and set_columns and where_columns:
                                # Log table name, set columns, and where columns
                                logging.info("Update Table Name: %s", table_name)
                                logging.info("Set Columns: %s", set_columns)
                                logging.info("Where Columns: %s \n", where_columns)
                        if is_delete:
                            # Extract table name and where columns
                            table_name, where_columns = fnc.extract_delete_info(sql_query)
                            if table_name and where_columns:
                                # Log table name and where columns
                                logging.info("Delete Table Name: %s", table_name)
                                logging.info("Where Columns: %s \n", where_columns)
                    #     sql_query = result.group(1).strip()
                    #     cleaned_sql_query = remove_coldfusion_syntax(sql_query)
                    #     pattern1 = fnc.validate_sql_pattern1(cleaned_sql_query)
                    #     if pattern1:
                    #         column_map = fnc.extract_table_column_names(cleaned_sql_query)
                    #     elif not pattern1:
                    #         column_map = fnc.extract_table_column_names_with_join(cleaned_sql_query)

                    #     for table, columns in column_map.items():
                    #         if not pattern1:
                    #             join_tables = ', '.join(columns['join'])
                    #             table += ', ' + join_tables

                    #         logging.info(f"Table Name: {table}")                                
                    #         logging.info(f"Select Columns: {columns['select']}")
                    #         logging.info(f"Where Columns: {columns['where']}")
                    #         logging.info(f"Order Columns: {columns['order']}")
                    #         logging.info(f"Group Columns: {columns['group']}\n")
    except UnicodeDecodeError:
        print(f'File: {file_path} - Unable to decode with latin-1')
        with error_log_path.open('a', encoding='utf-8') as error_log:
            error_log.write(f'File: {file_path} - Unable to decode with latin-1\n')

def main():
    start_time = time.time()
    print(f"Script started at: {time.ctime(start_time)}")
    files = [file for file in rootDir.rglob('*') if file.is_file()]
    for file in files:
         process_file(file)
    end_time = time.time()

    print(f"Number of files containing <cfquery> tags: {len(file_name)}")
    print(f"Script ended at: {time.ctime(end_time)}")
    print(f"Total processing time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
