import re
import os
import time
from datetime import datetime
import logging
import configparser
from collections import defaultdict
from pathlib import Path

config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')
rootDir = Path(config['DEFAULT']['logFiledir'])
rootDir2 = Path(config['DEFAULT']['logFiledir2'])

folder_path = "logs2"

# Open the log file

column_map = defaultdict(list)
file_name = []

logDir = Path("logfile")
logDir.mkdir(exist_ok=True)
error_log_path = logDir / 'error_log.log'

def setup_logger(table_name):
    # Create the directory if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path, exist_ok=True)

    logger = logging.getLogger(table_name)  # Logger name based on table name
    logger.setLevel(logging.INFO)  # Set logging level to INFO

    # Create a file handler for the logger in the specified directory
    handler = logging.FileHandler(os.path.join(folder_path, f"{table_name}.txt"))
    handler.setLevel(logging.INFO)  # Set handler level to INFO

    # Create a formatter and set it for the handler
    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

def extract_table_names(section_content):
    section_content = section_content.replace("Insert Table Name:", "Select Table Name:")
    section_content = section_content.replace("Update Table Name:", "Select Table Name:")
    section_content = section_content.replace("Delete Table Name:", "Select Table Name:")
    section_content = section_content.replace("Insert Columns:", "Select Columns:")
    section_content = section_content.replace("Set Columns:", "Select Columns:")
    section_content = section_content.replace("Where Columns:", "Select Columns:")
    section_content = section_content.replace("Order Columns:", "Select Columns:")
    section_content = section_content.replace("Group Columns:", "Select Columns:")
    
    section_content = concat_column(section_content)
    select_table_name_pattern = re.compile(r'Select Table Name:\s*(.*)')
    select_column_name_pattern = re.compile(r'Select Columns:\s*(.*)')
    
    # pattern = re.compile(r'\b(\w+)\s+AS\s+(\w+)\b(?:,\s*|$)')
    match = select_table_name_pattern.search(section_content)
    match_column = select_column_name_pattern.search(section_content)
    split_table_name = []
    
    if match:
        table_name = match.group(1)
        split_table_name = re.split(r',\s*', table_name)
        for sp_name in split_table_name:
            table_name_split = re.compile(r'\bAS\s+(\w+)')
            output_table = table_name_split.sub('', sp_name)
            write_table_name = output_table.strip()
            logger = setup_logger(write_table_name)

    if match_column:
        split_name = ''
        column_name = match_column.group(1)
        if ',' in column_name:
            split_name = re.split(r',\s*', column_name)
            for sp_name in split_name: 
                extract_column_names(sp_name, split_table_name)     
        else:
            extract_column_names(split_name, split_table_name)
                             
    return column_map

def concat_column(input_string):
    table_name = ""
    columns = []
    pattern = re.compile(r"Select Columns:\s*\[(.*?)\]")
    # Split input_string into lines and process each line
    for line in input_string.strip().splitlines():
        line = line.strip()
        if line.startswith("Select Table Name:"):
            table_name = line.split("Select Table Name:")[1].strip()
        elif line.startswith("Select Columns:"):
            match = pattern.search(line)
            if match:
                column_str = match.group(1)
                if column_str:  # Check if column_str is not empty
                    columns.append(column_str)

    columns = list(dict.fromkeys(columns))

    final_result = f"Select Table Name: {table_name}\n"
    if columns:
        final_result += "Select Columns: ['" + "','".join(columns) + "']"

    return final_result

def extract_column_names(sp_name, split_table_name):
        column_name_split = re.compile(r'(\w+)\.(\w+)')
        col_matches = column_name_split.findall(sp_name)        
        if col_matches:
            col_alias = col_matches[0][0] 
            col_name = col_matches[0][1] 
            pattern = re.compile(r'\b(\w+)\s+AS\s+(\w+)\b')
            for table_name in split_table_name:    
                output_table = pattern.findall(table_name)
                for table_name, table_alias in output_table:
                    if col_alias == table_alias and col_name not in column_map[table_name]:
                        column_map[table_name].append(col_name)
        else:
            for table_name in split_table_name:
                if sp_name not in column_map[table_name]:
                    column_map[table_name].append(sp_name)  


def start_run(file_path):
    try:
        file_path = Path(file_path)
        with file_path.open('r', encoding='utf-8') as file:
            file_name.append(file_path.name)
            # Read each line in the file
            file_content = file.read()
            # Split the content by the delimiter
            sections = file_content.split('===================================')
            # Process each section
            column_map = {}
            for section in sections:
                section_content = section.strip()
                if section_content:  # Ensure the section is not empty
                    column_map = extract_table_names(section_content) 
            for key,value in column_map.items():
                target_log = f"{key}.txt"
                files = [file for file in rootDir2.rglob('*') if file.is_file()]
                for file_path in files:
                    if file_path.name == target_log:
                        with file_path.open('a', encoding='utf-8') as file:
                            for col in value:
                                exiting_data = read_file_to_array(file_path)
                                if col not in exiting_data:
                                    col = col.replace("'", "")
                                    col = col.replace("[", "")
                                    col = col.replace("]", "")
                                    file.write(col + '\n') 
    except UnicodeDecodeError:
        with error_log_path.open('a', encoding='utf-8') as error_log:
            error_log.write(f'File: {file_path} - Unable to decode with utf-8\n')

def read_file_to_array(filepath):
    lines = []
    with filepath.open('r', encoding='utf-8') as file:
        for line in file:
            stripped_line = line.strip()
            lines.append(stripped_line)

    return lines
