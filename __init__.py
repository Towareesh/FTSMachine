from datetime import datetime
import pandas as pd
import sqlite3


class Engine:
    __version__ = '0.1'
    
    def __init__(self, db_uri: str) -> None:
        self.db_uri = db_uri
    
    def _color_log(self, text: str, color: str) -> str:
        """Custom logs
        Args:
            text (str): data log
            color (str): flag
        Returns:
            str: colored text
        """
        colors_set = {'-e': '[31m',     # errors
                      '-c': '[32m',     # complete 
                      '-w': '[34m'}     # warning
        return f'[ftp_engine] \u001b{colors_set.get(color)}{text}\u001b[0m'
    
    def sqlite_query(self, sqlite_query: str) -> iter:
        """Connect to SQLite database
        Args:
            sqlite_query (str): query on SQL
        Returns:
            Generator: record in db
        """        
        try:
            sqlite_connection = sqlite3.connect(self.db_uri)
            cursor = sqlite_connection.cursor()
            cursor.execute(sqlite_query)
            records = cursor.fetchall()
            cursor.close()
            print(self._color_log('Connection to database is opened', '-c'))
            for record in records:
                yield record

        except sqlite3.Error as error:
            print(self._color_log(f'{error}: Failed connect to database', '-e'))
        finally:
            if sqlite_connection:
                sqlite_connection.close()
                print(self._color_log('Connection to database is closed', '-w'))
    
    @staticmethod
    def get_data_frame(*columns_name, db_query) -> pd.DataFrame:
        data_frame = pd.DataFrame(data=[[columns for columns in record] for record in db_query],
                                  columns=[columns for columns in columns_name])
        return data_frame
            

class FTSMachine(Engine):
    def __init__(self, db_uri: str) -> None:
        super().__init__(db_uri)
        self.db = sqlite3.connect(':memory:')
        self.cursor = self.db.cursor()
        
    
    def create_virtual_table(self, data_frame: pd.DataFrame, headers: list) -> None:
        columns     = ', '.join([i for i in headers])
        none_values = ', '.join(['?' for _ in range(len(headers))])
               
        self.cursor.execute(f"""CREATE VIRTUAL TABLE virtual_table
                                USING FTS5({columns}, tokenize="porter unicode61")""")
        
        self.cursor.executemany(f"""INSERT INTO virtual_table({columns})
                                    VALUES({none_values})""",
                                    data_frame.to_records(index=False))
        self.db.commit()

    def search_fetchall_query(self, value, column,
                              search_type='MATCH',
                              limit_answers='5') -> list:
        result = self.cursor.execute(f"""SELECT *, RANK
                                         FROM virtual_table
                                         WHERE {column} {search_type} "{value}"
                                         ORDER BY RANK
                                         LIMIT {limit_answers}
                                         """).fetchall()
        self.cursor.close()
        return result