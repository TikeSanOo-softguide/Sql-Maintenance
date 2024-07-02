import re
import os
import logging
import configparser
from collections import defaultdict
from pathlib import Path

config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')
rootDir = Path(config['DEFAULT']['logFiledir'])

# Open the log file

column_map = defaultdict(list)
file_name = []

logDir = Path("logfile")
logDir.mkdir(exist_ok=True)
error_log_path = logDir / 'error_log.log'

def setup_logger(table_folder_path,table_name):
    # Create the directory if it doesn't exist
    if not os.path.exists(table_folder_path):
        os.makedirs(table_folder_path, exist_ok=True)

    logger = logging.getLogger(table_name)  # Logger name based on table name
    logger.setLevel(logging.INFO)  # Set logging level to INFO

    # Create a file handler for the logger in the specified directory
    handler = logging.FileHandler(os.path.join(table_folder_path, f"{table_name}.txt"))
    handler.setLevel(logging.INFO)  # Set handler level to INFO

    # Create a formatter and set it for the handler
    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

def extract_table_names(section_content, folder_path):
    section_content = section_content.replace("Insert Table Name:", "Select Table Name:")
    section_content = section_content.replace("Update Table Name:", "Select Table Name:")
    section_content = section_content.replace("Delete Table Name:", "Select Table Name:")
    section_content = section_content.replace("Insert Columns:", "Select Columns:")
    section_content = section_content.replace("Set Columns:", "Select Columns:")
    section_content = section_content.replace("Where Columns:", "Select Columns:")
    section_content = section_content.replace("Order Columns:", "Select Columns:")
    section_content = section_content.replace("Group Columns:", "Select Columns:")
    view_table_name = ''
    target_view_table_name = ''
    # check for exists view or not
    if "View Table Name: " in section_content:
        view_table_name_pattern = re.compile(r'View Table Name:\s*(.*)', re.IGNORECASE)
        view_table_name = view_table_name_pattern.search(section_content)
        target_view_table_name = view_table_name.group(1)

    section_content = concat_column(section_content)
    select_table_name_pattern = re.compile(r'Select Table Name:\s*(.*)', re.IGNORECASE)
    select_column_name_pattern = re.compile(r'Select Columns:\s*(.*)', re.IGNORECASE)
    
    # pattern = re.compile(r'\b(\w+)\s+AS\s+(\w+)\b(?:,\s*|$)')
    match = select_table_name_pattern.search(section_content)
    match_column = select_column_name_pattern.search(section_content)
    split_table_name = []
    write_table_name = ''   
    if match:
        table_name = match.group(1)
        split_table_name = re.split(r',\s*', table_name)
       
        if view_table_name != '':
            folder_path = os.path.join(folder_path, view_table_name.group(1))
        
        for sp_name in split_table_name:
            table_folder_path = folder_path
            table_name_split = re.compile(r'\bAS\s+(\w+)', re.IGNORECASE)
            output_table = table_name_split.sub('', sp_name)
            write_table_name = output_table.strip()
            split_strings = write_table_name.split('.')
            
            if len(split_strings) == 2:
                write_table_name = split_strings[-1]
                db_folder_path = os.path.join(folder_path, split_strings[0])
                table_folder_path = db_folder_path
            if len(split_strings) == 3:
                write_table_name = split_strings[-1]
                db_folder_path = os.path.join(folder_path, split_strings[0], split_strings[1])
                table_folder_path = db_folder_path
                if not os.path.exists(db_folder_path):
                    os.makedirs(db_folder_path, exist_ok=True)
            if write_table_name != '':                 
                logger = setup_logger(table_folder_path,write_table_name.lower())
    
    if match_column:
        split_name = ''
        column_name = match_column.group(1)
        if ',' in column_name:
            split_name = re.split(r',\s*', column_name)
            for sp_name in split_name:
                extract_column_names(sp_name, split_table_name, target_view_table_name)     
        else:
            extract_column_names(column_name, split_table_name, target_view_table_name)
                             
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

def extract_column_names(sp_name, split_table_name, target_view_table_name):
    column_name_split = re.compile(r'(.+?)\s*\.(\w+)', re.IGNORECASE)
    # column_name_split2 = re.compile(r'(\w+)\.(\w+)', re.IGNORECASE)
    col_matches = column_name_split.findall(sp_name)

    # Regular expression pattern to match 'table_name AS alias_name'
    table_pattern = re.compile(r'(.+?)\s+AS\s+(\w+)\b', re.IGNORECASE)      
  
    if col_matches:
        parts = sp_name.split('.')
        col_name = parts[-1].strip("'")  # Last part is the column name

        # Joining the other parts with dot to form the alias
        col_alias = '.'.join(parts[:-1]).strip("'")
        for table_name in split_table_name:
            target_table_name= ''
            if target_view_table_name != '':
                target_table_name = target_view_table_name + '.' + table_name
                
            output_table = table_pattern.findall(target_table_name)
            if output_table:
                # print(output_table)
                for table_name, table_alias in output_table:
                    if col_alias == table_alias and col_name not in column_map[table_name]:
                        column_map[table_name].append(col_name)

            if not output_table:
                    output_table = [tuple(table_name.strip().split(' '))]
                    if len(output_table) == 2 :
                        for table_name, table_alias in output_table:
                            if col_alias == table_alias and col_name not in column_map[table_name]:
                                column_map[table_name].append(col_name)
                    else:         
                        if col_alias == table_name and col_name not in column_map[table_name]:
                            column_map[target_table_name].append(col_name)
            
    else:    
        for table_name in split_table_name:
            if ' as' in table_name.lower():
                output_table = table_pattern.findall(table_name)
                if output_table:
                    table_name = output_table[0][0]
               
            if target_view_table_name != '':
                table_name = target_view_table_name + '.' + table_name
   
            if sp_name not in column_map[table_name]:
                column_map[table_name].append(sp_name)  


def start_run(file_path, rootDir2, folder_path):
    try:
        file_path = Path(file_path)
        sub_path = ''
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
                    column_map = extract_table_names(section_content, folder_path)

            for key,value in column_map.items():  
                table_name = key.lower().replace('.', '\\')
                target_log = f"{table_name}.txt"
                files = [file for file in rootDir2.rglob('*') if file.is_file()]
                for file_path in files:
                    parts = str(file_path).split('table_list', 1)
                    if len(parts) > 1:
                        sub_path = parts[1].lstrip('\\')
                         
                    if sub_path.lower() == target_log.lower():
                        exiting_data2 = []
                        with file_path.open('a', encoding='utf-8') as file:
                            for col in value:
                                exiting_data = read_file_to_array(file_path)
                                col = col.replace("'", "")
                                col = col.replace("[", "")
                                col = col.replace("]", "")
                                col = col.replace("{", "")
                                col = col.replace("}", "")
                                
                                if col not in exiting_data and col not in exiting_data2:
                                    exiting_data2.append(col)
                                    file.write(col + '\n') 
    except UnicodeDecodeError:
        with error_log_path.open('a', encoding='utf-8') as error_log:
            error_log.write(f'File: {file_path} - Unable to decode with utf-8\n')
            
def read_file_to_array(file_path):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found at: {file_path}")
    
    lines = []
    with open(file_path, 'r', encoding='utf-8') as file:  
        for line in file:
            stripped_line = line.strip()
            lines.append(stripped_line)
       
    return lines