import pymysql.cursors
import json
import time
import datetime

class MySQLDBQueries:

    # constructor. 
    def __init__(self):
        # self.__connection = pymysql.connect(host='localhost', user='root', password='Europe!2211',  db='dummy_application',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
        self.__without_db_connection = pymysql.connect(host='localhost', user='root', password='Europe!2211',  charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

    # Create a new DB
    def create_database(self):
        try:
            current_timestamp = time.time()
            db_substring = datetime.datetime.fromtimestamp(current_timestamp).strftime('%Y_%m_%d_%H_%M_%S')
            self.__current_db_name = "dummy_app_"+db_substring
            print("Db name {}".format(self.__current_db_name))
            with self.__without_db_connection.cursor() as cursor:
                # Create a new database
                create_db_sql_script = "CREATE DATABASE IF NOT EXISTS " + self.__current_db_name + " ;"
                cursor.execute(create_db_sql_script)

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            self.__without_db_connection.commit()
            print("Database created")
        except Exception as e:
            print(e)
            if self.__without_db_connection.open:
                self.__without_db_connection.close()

    # Create a table in newly created DB
    def create_table(self):
        try:
            with self.__without_db_connection.cursor() as cursor:
                # Create a new table for the records. 
                print("Creating table employee in the db.")
                create_table_sql_script = "CREATE TABLE IF NOT EXISTS {}.Employee ( id int AUTO_INCREMENT PRIMARY KEY, LastName varchar(100), FirstName varchar(100), DepartmentCode int, Address VARCHAR(1000), FatherName VARCHAR(100), MotherName VARCHAR(100), SKills VARCHAR(100) ); ".format(self.__current_db_name)
                cursor.execute(create_table_sql_script)

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            self.__without_db_connection.commit()
            print("Table created.")
        except Exception as e:
            print(e)
            if self.__without_db_connection.open:
                self.__without_db_connection.close()

    # Insert records. 
    def insert_records(self):
        # Create new records as default. 
        try:
            # get input data
            print("Reading the JSON from the file.")
            bulk_data=MySQLDBQueries.__get_input_data()
            print("got json fro data creation.")
            with self.__without_db_connection.cursor() as cursor:
                # Create a new table for the records. 
                print("going to insert the bulk records from JSON to DB.")
                bulk_insert_sql_script = "INSERT INTO {}.Employee(LastName, FirstName, DepartmentCode, Address, FatherName, MotherName, Skills)".format(self.__current_db_name) + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.executemany(bulk_insert_sql_script, bulk_data)

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            self.__without_db_connection.commit()
            print("records inserted")
        except Exception as e:
            print(e)
            if self.__without_db_connection.open:
                self.__without_db_connection.close()


    # Read TABLE
    def read_from_table(self):
        try:
            with self.__without_db_connection.cursor() as cursor:
                # Read TABLE
                sql = "SELECT * FROM {}.`Employee` ORDER BY id desc;".format(self.__current_db_name)
                cursor.execute(sql)
                result = cursor.fetchall()
                print(result)

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            self.__without_db_connection.commit()
        except Exception as e:
            print(e)
            if self.__without_db_connection.open:
                self.__without_db_connection.close()

    
    def drop_table(self):
        try:
            with self.__without_db_connection.cursor() as cursor:
                # Read record
                sql = "DROP TABLE IF EXISTS {}.`Employee`;".format(self.__current_db_name)
                cursor.execute(sql)

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            self.__without_db_connection.commit()
        except Exception as e:
            print(e)
            if self.__without_db_connection.open:
                self.__without_db_connection.close()

    def drop_database(self):
        try:
            with self.__without_db_connection.cursor() as cursor:
                # Read record
                sql = "DROP DATABASE IF EXISTS {};".format(self.__current_db_name)
                cursor.execute(sql)

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            self.__without_db_connection.commit()
        except Exception as e:
            print(e)
            if self.__without_db_connection.open:
                self.__without_db_connection.close()

    @staticmethod
    def __get_input_data():
        all_json = json.load(open('json_data.json'))
        data_values = []
        for iten in all_json:
            data_values.append(tuple((iten["LastName"], iten["FistName"], iten["DepartmentCode"], iten["Address"], iten["FatherName"], iten["MotherName"], iten["SKills"])))
        return data_values



print("1 Initialize the connection.")
new_db = MySQLDBQueries()

print("2 Create a brand new database.")
new_db.create_database()

print("3 Create a new table in the DB.")
new_db.create_table()

print("4 Insert new records to the table from JSON file.")
new_db.insert_records()

print("5 Read from the table.")
new_db.read_from_table()

print("6 Drop the table.")
new_db.drop_table()

print("7 Drop the database for cleanup purpose.")
new_db.drop_database()