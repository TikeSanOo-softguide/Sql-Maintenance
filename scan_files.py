import os
import re
import time
from datetime import datetime
import logging
import configparser
import functions as fnc
from pathlib import Path

# Create a folder if it doesn't exist
folder_path = "logs"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

today_date = datetime.today().strftime('%Y_%m_%d_%H_%M_%S')
# Set up logging
log_file = os.path.join(folder_path, f"query_analysis_{today_date}.log")
require_log_file = os.path.join(folder_path, f"require_query_{today_date}.log")

# Configure the logger for query_analysis.log
query_analysis_logger = logging.getLogger('query_analysis_logger')
query_analysis_logger.setLevel(logging.INFO)
query_analysis_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
query_analysis_handler.setFormatter(logging.Formatter('%(message)s'))
query_analysis_logger.addHandler(query_analysis_handler)

# Configure a separate logger for require_files_query_analysis.log
require_logger = logging.getLogger('require_files_logger')
require_logger.setLevel(logging.INFO)
require_handler = logging.FileHandler(require_log_file, mode='w', encoding='utf-8')
require_handler.setFormatter(logging.Formatter('%(message)s'))
require_logger.addHandler(require_handler)

config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')
rootDir = Path(config['DEFAULT']['rootDir'])

logDir = Path("logfile")
cfquery_pattern = re.compile(r'<cfquery\b.*?</cfquery>', re.DOTALL | re.IGNORECASE)
file_name = []

logDir.mkdir(exist_ok=True)
error_log_path = logDir / 'error_log.log'

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
        with file_path.open('r', encoding='utf-8') as file:
            file_contents = file.read()
            uncommented_contents = remove_all_comments(file_contents)  # Remove comments first
            matches = cfquery_pattern.findall(uncommented_contents)  # Find <cfquery> tags
            
            if matches:
                file_path_list = []
                file_name.append(file_path.name)
                query_analysis_logger.info(file_path)
                for match in matches:
                    #Remove <cfquery> tags from query
                    sql_regex = re.compile(r'<cfquery.*?>(.*?)</cfquery>', re.DOTALL)
                    result = sql_regex.search(match)
                    if result:
                        sql_query = result.group(1).strip()
                        is_select = fnc.has_select_query(sql_query)
                        is_insert = fnc.has_insert_query(sql_query)
                        is_update = fnc.has_update_query(sql_query)
                        is_delete = fnc.has_delete_query(sql_query)
                        column_map = {}
                    
                        if is_select:
                            cleaned_sql_query = remove_coldfusion_syntax(sql_query)
                            query_pattern = fnc.validate_sql_pattern(cleaned_sql_query)
                            if query_pattern == 'simple select':
                                column_map = fnc.extract_table_column_names(cleaned_sql_query)
                            elif query_pattern == 'simple join':
                                column_map = fnc.extract_table_column_names_with_join(cleaned_sql_query)
                            elif query_pattern == 'subquery from select':
                                column_map = fnc.extract_table_column_names_with_sub_pat1(cleaned_sql_query)
                            elif query_pattern == 'subquery where select':
                                column_map = fnc.extract_table_column_names_with_sub_pat2(cleaned_sql_query)
                            elif query_pattern == 'Not sql exists':
                                if len(file_path_list) == 0:
                                    require_logger.info(file_path)
                                    file_path_list.append(file_path)

                                if file_path not in file_path_list:
                                    file_path_list = []
                                require_logger.info(cleaned_sql_query + "\n")       
                            
                            for table, columns in column_map.items():
                                if 'join' in columns:
                                    if columns['join']:
                                        join_tables = ', '.join(columns['join'])
                                        table += ', ' + join_tables

                                query_analysis_logger.info(f"Select Table Name: {table}")                                
                                query_analysis_logger.info(f"Select Columns: {columns['select']}")
                                query_analysis_logger.info(f"Where Columns: {columns['where']}")
                                query_analysis_logger.info(f"Order Columns: {columns['order']}")
                                query_analysis_logger.info(f"Group Columns: {columns['group']}\n")
                        if is_insert:
                            table_name, columns = fnc.insert_table_column_names(sql_query)
                            if table_name and columns:
                                # Log table name and column names
                                query_analysis_logger.info("Insert Table Name: %s", table_name)
                                query_analysis_logger.info("Insert Columns: %s \n", columns)
                        if is_update:
                            # Extract table name, set columns, and where columns
                            table_name, set_columns, where_columns = fnc.update_table_column_names(sql_query)
                            if table_name and set_columns and where_columns:
                                # Log table name, set columns, and where columns
                                query_analysis_logger.info("Update Table Name: %s", table_name)
                                query_analysis_logger.info("Set Columns: %s", set_columns)
                                query_analysis_logger.info("Where Columns: %s \n", where_columns)
                        if is_delete:
                            # Extract table name and where columns
                            table_name, where_columns = fnc.extract_delete_info(sql_query)
                            if table_name and where_columns:
                                # Log table name and where columns
                                query_analysis_logger.info("Delete Table Name: %s", table_name)
                                query_analysis_logger.info("Where Columns: %s \n", where_columns)

    except UnicodeDecodeError:
        with error_log_path.open('a', encoding='utf-8') as error_log:
            error_log.write(f'File: {file_path} - Unable to decode with utf-8\n')

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
