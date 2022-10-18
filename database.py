from secret import SecretFile
import psycopg2.extras
import psycopg2

class Database:
    def __init__(self):
        self.hostname = '46.101.167.19'
        self.database = 'kafka_stream'
        self.username = 'postgres'
        self.pwd     = SecretFile.password
        self.port_id = 5432
        self.conn    = None
        self.cursor  = None

    def create_table_raw_data(self):
        try:
            with psycopg2.connect(
                host=self.hostname,
                dbname=self.database,
                user=self.username,
                password=self.pwd,
                port=self.port_id
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('DROP TABLE IF EXISTS raw_data')
                    create_table = """CREATE TABLE IF NOT EXISTS raw_data (
                                            year   varchar(4) NOT NULL,
                                            month  varchar(2) NOT NULL,
                                            day    varchar(2) NOT NULL,
                                            event  varchar NOT NULL
                                     )"""
                    cursor.execute(create_table)
        except Exception as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()

    def insert_into_raw_data(self, year, month, day, event):
        try:
            with psycopg2.connect(
                host=self.hostname,
                dbname=self.database,
                user=self.username,
                password=self.pwd,
                port=self.port_id
            ) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    insert = 'INSERT INTO raw_data (year, month, day, event) VALUES (%s, %s, %s, %s)'
                    record = (year, month, day, event)
                    cursor.execute(insert, record)
        except Exception as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()

    def create_table_processed_data(self, table_name):
        try:
            with psycopg2.connect(
                host=self.hostname,
                dbname=self.database,
                user=self.username,
                password=self.pwd,
                port=self.port_id
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('DROP TABLE IF EXISTS processed_data')
                    create_table = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                                            date   varchar NOT NULL,
                                            event  varchar NOT NULL
                                     )"""
                    cursor.execute(create_table)
        except Exception as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()

    def insert_into_processed_data(self, table_name, date, event):
        try:
            with psycopg2.connect(
                host=self.hostname,
                dbname=self.database,
                user=self.username,
                password=self.pwd,
                port=self.port_id
            ) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    insert = f'INSERT INTO {table_name} (date, event) VALUES (%s, %s)'
                    record = (date, event)
                    cursor.execute(insert, record)
        except Exception as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()