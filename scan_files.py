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
                        # Extract table and column names
                        column_map = fnc.extract_table_column_names(sql_query)
                        # Write results to logfile
                        for table, columns in column_map.items():
                            logging.info(f"Table Name: {table}")
                            logging.info(f"Select Columns: {columns['select']}")
                            logging.info(f"Where Columns: {columns['where']}")
                            logging.info(f"Order Columns: {columns['order']}\n")
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
