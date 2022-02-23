'''
UNIVERSIDAD DEL VALLE DE GUATEMALA
PROYECTO 1
BASES DE DATOS 1
ROBERTO RIOS, 20979
MARIO DE LEON, 19019
ETAPA 1: SCRIPT PARA RECUPERAR LAS BASES DE DATOS A PARTIR DE ARCHIVOS CSV EN UN FOLDER
'''

import psycopg2
import glob

# function to retrieve data from every csv in a folder, create tables
# with the csv's names and fill the created tables
def fill_tables(path, hostname, database, username, passw, port_id):
    # path = path to the folder
    try:
        # connect to db with values
        with psycopg2.connect(
                host = hostname,
                dbname = database,
                user = username,
                password = passw,
                port = port_id) as conn:
            # call the cursor class to execute SQL commands
            with conn.cursor() as cur:
                # get csv's, then retrieve tables with its values
                # Loop through each CSV in the folder
                csv_path = f"{path}/"
                folder = glob.glob(csv_path+"*.csv")
                for filename in folder: 
                    # Create a table name
                    tablename = filename.replace(f"{path}\\", "").replace(".csv", "").lower()
                    print(f"Created: {tablename}")
                    # Open file
                    fileInput = open(filename, "r", encoding="utf8")
                    # Extract first line of file
                    firstLine = fileInput.readline().strip().lower()
                    # Split columns names into an array
                    columnsheader = firstLine.split(",")
                    # Build SQL code to drop table if exists and create table
                    sqlQueryCreate = f'DROP TABLE IF EXISTS {tablename};\n'
                    sqlQueryCreate += f'CREATE TABLE {tablename} ('
                    
                    if tablename != 'player_bios':
                        for column in columnsheader:
                            sqlQueryCreate += column + " VARCHAR(60),\n"
                        sqlQueryCreate = sqlQueryCreate[:-2]
                        sqlQueryCreate += ");"
                    else:
                        for column in columnsheader:
                            sqlQueryCreate += column + " VARCHAR(80),\n"
                        sqlQueryCreate = sqlQueryCreate[:-2]
                        sqlQueryCreate += ");"

                    sqlQueryCreate += f'''
                    COPY {tablename}
                    FROM '{filename}'
                    DELIMITER ','
                    CSV HEADER;
                    '''

                    cur.execute(sqlQueryCreate)
                    conn.commit()
    except Exception as e:
        print(e)
    finally:
        # close connection
        if conn is not None:
            conn.close()