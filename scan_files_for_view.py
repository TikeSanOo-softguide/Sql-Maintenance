import os
import re
import time
from datetime import datetime
import logging
import configparser
import functions as fnc
from pathlib import Path
from functions import process_excel_file

current_root = Path().resolve()
# Create a folder if it doesn't exist
folder_path = "logs"

if not os.path.exists(folder_path):
    os.makedirs(folder_path)

today_date = datetime.today().strftime('%Y_%m_%d_%H_%M_%S')
# Set up logging
log_file = os.path.join(folder_path, f"query_analysis_for_view_{today_date}.log")
require_log_file = os.path.join(folder_path, f"require_query_for_view_{today_date}.log")

# Configure the logger for query_analysis.log
query_analysis_logger = logging.getLogger('query_analysis_logger_view')
query_analysis_logger.setLevel(logging.INFO)
query_analysis_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
query_analysis_handler.setFormatter(logging.Formatter('%(message)s'))
query_analysis_logger.addHandler(query_analysis_handler)

# Configure a separate logger for require_files_query_analysis.log
require_logger = logging.getLogger('require_files_logger_view')
require_logger.setLevel(logging.INFO)
require_handler = logging.FileHandler(require_log_file, mode='w', encoding='utf-8')
require_handler.setFormatter(logging.Formatter('%(message)s'))
require_logger.addHandler(require_handler)

config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')
rootViewDir = Path(config['DEFAULT']['rootViewDir'])


viewTableDir = current_root / Path(config['DEFAULT']['view_table_list_file_dir'])
view_table_folder_path = Path(config['DEFAULT']['view_table_list_file_dir']) 
error_log_dir = current_root / Path(config['DEFAULT']['errorlogDir']) 

# view_query_pattern = re.compile(r"GO\s+(CREATE VIEW.*?GO)", re.DOTALL | re.IGNORECASE)
view_query_pattern = re.compile(r"GO\s+(CREATE VIEW.*?)(?=\bGO\b|\Z)", re.DOTALL | re.IGNORECASE)
view_table_name_pattern = re.compile(r'CREATE\s+VIEW\s+(\[.*?\])\s+AS', re.DOTALL | re.IGNORECASE)
file_name = []
view_names = []

error_log_dir = current_root / Path(config['DEFAULT']['errorlogDir']) 
error_log_dir.mkdir(exist_ok=True)
read_file_error_log_path = error_log_dir / 'read_files_error.log' 
file_process_error_log_path = error_log_dir / 'file_process_error.log' 

def remove_all_comments(content):
    # Regular expression pattern to match nested HTML comments
    comment_pattern = re.compile(r'<!---(.*?)--->', re.DOTALL)
    nested_comment_pattern = re.compile(r'<!---(?:(?!<!---)(?:.|\\n))*?--->', re.DOTALL)
    
    # Remove nested comments
    cleaned_nested_content = nested_comment_pattern.sub('', content)
    cleaned_content = comment_pattern.sub('', cleaned_nested_content)

    return cleaned_content

def remove_sql_query_comment(content):
    # Remove all occurrences of C-style block comments (/* ... */)
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    # Remove all occurrences of C++-style line comments (// ...)
    content = re.sub(re.compile("//.*?\n"), "", content)
    # Remove lines that start with '--'
    content = re.sub(re.compile("--.*?$", re.MULTILINE), "", content)
    
    # Remove ColdFusion tags
    content = re.sub(re.compile(r'<\/?cf[^>]*>', re.IGNORECASE), '', content)
    # Remove ColdFusion comments
    content = re.sub(re.compile(r'<!---.*?--->', re.DOTALL), '', content)
    # Remove ColdFusion expressions
    content = re.sub(re.compile(r'#[^#]*#'), '', content)
    
    return content

def process_file(file_path):
    try:
        with file_path.open('r', encoding='utf-8') as file:
            file_contents = file.read()
            uncommented_contents = remove_all_comments(file_contents)  # Remove comments first
            matches = view_query_pattern.findall(uncommented_contents)  # Find <cfquery> tags
            square_brackets_regex = r"\[([^\]]+)\]"
            if matches:
                file_path_list = []
                file_name.append(file_path.name)

                query_analysis_logger.info(file_path)
                for match in matches:
                    view_table_names = view_table_name_pattern.findall(match)
                    for_remove = view_table_name_pattern.search(match)
                    sql_query = re.sub(re.escape(for_remove.group(0)), '', match)
                    view_table_name_with_db = re.sub(square_brackets_regex, r"\1", view_table_names[0])
                    view_table_name = view_table_name_with_db.split('.')[-1]
                    view_names.append(view_table_name.lower())
                    is_select = fnc.has_select_query(sql_query)
                    is_insert = fnc.has_insert_query(sql_query)
                    is_update = fnc.has_update_query(sql_query)
                    is_delete = fnc.has_delete_query(sql_query)
                    column_map = {}
                    
                    if is_select:
                        process_sql_query = remove_sql_query_comment(sql_query)
                        query_pattern = fnc.validate_sql_pattern(process_sql_query)
                        if query_pattern == 'simple select':
                            column_map = fnc.extract_table_column_names(process_sql_query)
                        elif query_pattern == 'simple join':
                            column_map = fnc.extract_table_column_names(process_sql_query)
                        elif query_pattern == 'subquery select':
                            column_map = fnc.extract_table_column_names_with_sub_pat(process_sql_query)

                        elif query_pattern == 'Not sql exists':
                            if len(file_path_list) == 0:
                                require_logger.info(file_path)
                                file_path_list.append(file_path)

                            if file_path not in file_path_list:
                                file_path_list = []
                            require_logger.info(process_sql_query + "\n")

                        for table, columns in column_map.items():
                            if 'join' in columns:
                                if columns['join']:
                                    join_tables = ','.join(columns['join'])
                                    table += ',' + join_tables

                            query_analysis_logger.info(f"View Table Name: {view_table_name}")                                
                            query_analysis_logger.info(f"Select Table Name: {table}")                                
                            query_analysis_logger.info(f"Select Columns: {columns['select']}")
                            query_analysis_logger.info(f"Where Columns: {columns['where']}")
                            query_analysis_logger.info(f"Order Columns: {columns['order']}")
                            query_analysis_logger.info(f"Group Columns: {columns['group']}")
                            query_analysis_logger.info("===================================")
                    if is_insert:
                        table_name, columns = fnc.insert_table_column_names(sql_query)
                        if table_name and columns:
                            # Log table name and column names
                            query_analysis_logger.info(f"View Table Name: {view_table_name}")
                            query_analysis_logger.info("Insert Table Name: %s", table_name)
                            query_analysis_logger.info("Insert Columns: %s ", columns)
                            query_analysis_logger.info("===================================")
                    if is_update:
                        # Extract table name, set columns, and where columns
                        process_sql_query = remove_sql_query_comment(sql_query)
                        table_name, set_columns, where_columns = fnc.update_table_column_names(process_sql_query)
                        if table_name and set_columns and where_columns:
                            # Log table name, set columns, and where columns
                            query_analysis_logger.info(f"View Table Name: {view_table_name}")
                            query_analysis_logger.info("Update Table Name: %s", table_name)
                            query_analysis_logger.info("Set Columns: %s", set_columns)
                            query_analysis_logger.info("Where Columns: %s", where_columns)
                            query_analysis_logger.info("===================================")
                    if is_delete:
                        # Extract table name and where columns
                        table_name, where_columns = fnc.extract_delete_info(sql_query)
                        if table_name and where_columns:
                            # Log table name and where columns
                            query_analysis_logger.info(f"View Table Name: {view_table_name}")
                            query_analysis_logger.info("Delete Table Name: %s", table_name)
                            query_analysis_logger.info("Where Columns: %s", where_columns)
                            query_analysis_logger.info("===================================")
    except UnicodeDecodeError:
        with read_file_error_log_path.open('a', encoding='utf-8') as error_log:
            error_log.write(f'{today_date} - Error in scan_files_for_view.py/process_file function: Unable to decode {file_path}\n')
    except Exception as e:
        with file_process_error_log_path.open('a', encoding='utf-8') as error_log:
            error_log.write(f'{today_date} - Error in scan_files_for_view.py/process_file function: {e}\n')

                     
def main():
    start_time = time.time()
    print(f"Script started at: {time.ctime(start_time)}")
    files = [file for file in rootViewDir.rglob('*') if file.is_file()]
    for file in files:
        process_file(file)
   
    fnc.start_run(log_file, viewTableDir, view_table_folder_path)
    file_names =  fnc.source_file_process(view_names)
    process_excel_file()
    end_time = time.time()
    
    print(f"Number of files containing query: {len(file_name) + len(file_names)}")
    print(f"Script ended at: {time.ctime(end_time)}")
    print(f"Total processing time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
    
