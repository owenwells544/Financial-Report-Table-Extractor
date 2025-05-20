import pdfplumber as pp
import pandas as pd
import argparse
import mysql.connector
from mysql.connector import Error

#function for parsing command line arguments
def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    #name of pdf file to be parsed
    parser.add_argument(
        '--filename',
        type=str,
        default="",
        help='File name (path) of the pdf with tables to be extracted'
    )
    
    #intersection tolerance for parsing tables
    parser.add_argument(
        '--intersection_tolerance',
        type=int,
        default = 10,
        help='When combining edges into cells, orthogonal edges must be within intersection_tolerance points to be considered intersecting.'
    )

    #mysql connection params
    parser.add_argument(
        '--mysql_host',
        type=str,
        default="localhost",
        help='MySQL host address'
    )
    
    parser.add_argument(
        '--mysql_user',
        type=str,
        default="root",
        help='MySQL username'
    )
    
    parser.add_argument(
        '--mysql_password',
        type=str,
        default="",
        help='MySQL password'
    )
    
    parser.add_argument(
        '--mysql_database',
        type=str,
        default="pdf_tables",
        help='MySQL database name'
    )
   
    return parser.parse_args()

def create_mysql_connection(host, user, password, database):
    try:
        #create connection object
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )

        #verify connection was successful
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None
    
def create_table_mysql(connection, table_name, columns):
    try:
        #attach cursor object
        cursor = connection.cursor()

        #build create table query
        columns_def = ", ".join([f"`{col}` TEXT" for col in columns])
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {columns_def}
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """

        #execute create table query
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table '{table_name}' created or already exists")
    except Error as e:
        print(f"Error creating table: {e}")

def insert_data_to_mysql(connection, table_name, df):
    try:
        #Replace NaN values with None
        df = df.where(pd.notnull(df), None)
        
        #attach cursor object
        cursor = connection.cursor()

        #build table structure        
        columns = ", ".join([f"`{col}`" for col in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))
    
        for _, row in df.iterrows():
            row_data = list(row.values)
            
            insert_query = f"""
            INSERT INTO `{table_name}` ({columns})
            VALUES ({placeholders})
            """
            #insert data row by row into mysql table
            cursor.execute(insert_query, row_data)
        
        connection.commit()
        print(f"Inserted {len(df)} rows into '{table_name}'")
    except Error as e:
        print(f"Error inserting data: {e}")

def pull_tables(file, it, connection):
    all_tables = []

    settings_dict = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "intersection_tolerance": it
    }

    with pp.open(file) as pdf:
        for page in pdf.pages:
            cur_page_tables = page.extract_tables(settings_dict)

            for table in cur_page_tables:
                if not table or len(table) < 2:  #Skip empty tables or tables without headers
                    continue

                #Clean column names
                headers = table[0]
                cleaned_headers = []
                for i, header in enumerate(headers):
                    if not header or str(header).strip() == '':
                        header = f"column_{i+1}"
                    cleaned_headers.append(str(header).strip())
                
                df = pd.DataFrame(table[1:], columns=cleaned_headers)
                df = df.dropna(how='all') #clean up empty rows
                df = df.dropna(axis=1, how='all') #clean up empty columns
                
                # Remove any remaining empty column names
                df.columns = [col if str(col).strip() != '' else f"column_{i+1}" 
                            for i, col in enumerate(df.columns)]

                # normalization for numeric data
                for col in df.columns:
                    # Replace empty strings with NaN first
                    df[col] = df[col].replace('', None)
                    
                    #verify column type is text or mixed data
                    if pd.api.types.is_object_dtype(df[col]):
                        df[col] = df[col].astype(str).str.replace(r'[\$,]', '', regex=True) #get rid of $ and , for numeric conversion
                        df[col] = df[col].astype(str).str.encode('ascii', errors='ignore').str.decode('ascii')
                        df[col] = df[col].str.replace(r'[^\x00-\x7F]+', '', regex=True)
                    

                        #attempt conversion to numeric data type
                        try:
                            df[col] = pd.to_numeric(df[col])
                        except (ValueError, TypeError):
                            pass  #pass on case where data cannot be converted

                #add table to ret dict
                all_tables.append(table)

                #add table to mysql database
                if len(df.columns) > 0:  # Only proceed if we have valid columns
                    file_ext = file.replace(".pdf", "")
                    table_name = f"table_{len(all_tables)}_{file_ext}"
                    create_table_mysql(connection, table_name, df.columns)
                    insert_data_to_mysql(connection, table_name, df)

    return all_tables

def main():
    #get command line args
    args = parse_args()

    #create connection to mysql database
    connection = create_mysql_connection(
        args.mysql_host,
        args.mysql_user,
        args.mysql_password,
        args.mysql_database
    )

    #extract all tables from pdf
    tables_dict = pull_tables(args.filename, args.intersection_tolerance, connection)

    #close mysql connection
    if connection.is_connected():
        connection.close()


if __name__ == "__main__":
    main()

