import os
import mysql.connector
import db_setup_functions as dsf

server_config = {
    'user': 'DataEvalAdmin',
    'password': '############',
    'host': '127.0.0.1',
    'ssl_disabled': True
}

output_database_name = "PATHParticipantDB"
path_program_database_name = "PATHProgramMasterList"

raw_CSVs_and_cocDBs_folder_location = 'raw_csvs'
non_HMIS_folder_location = 'Non-HMIS'
parent_directory = os.path.dirname(os.getcwd())

raw_CSVs_and_cocDBs_folder_path = os.path.join(parent_directory, raw_CSVs_and_cocDBs_folder_location)
non_HMIS_folder_path = os.path.join(parent_directory, raw_CSVs_and_cocDBs_folder_location, non_HMIS_folder_location)

folder_names = ["LA", "LB", "OC", "SB", "SD", "SCC"]

conn = mysql.connector.connect(**server_config)
cursor = conn.cursor()

# Initialize the output database
dsf.download_raw_csv()
dsf.database_initialization(server_config, output_database_name)

dsf.load_path_master_list(server_config, output_database_name, path_program_database_name)

for folder_name in folder_names:
    database_name = f"{folder_name}"
    dsf.database_initialization(server_config, database_name)
    dsf.load_data_from_csv(server_config, database_name, os.path.join(raw_CSVs_and_cocDBs_folder_path, database_name))
    dsf.append_db_name_to_id_columns(server_config, database_name)
    
    dsf.update_move_in_dates(server_config, database_name, path_program_database_name) 
    dsf.update_engagement_dates(server_config, database_name, path_program_database_name) 
    dsf.ch_fill_in(server_config, database_name, path_program_database_name)
    dsf.apply_chronically_homeless_to_household(server_config, database_name)
    


    if database_name == "SD":
        dsf.update_SD_entry_dates(server_config, database_name, path_program_database_name) 
    
    dsf.attach_and_merge_data(server_config, output_database_name, database_name)


conn.close()
