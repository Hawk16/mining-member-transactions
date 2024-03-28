import json
import pandas as pd
import urllib.parse
from tqdm import tqdm
from sqlalchemy import text
from sqlalchemy import create_engine


class DatabaseMethods:

    """
    This class generates SQL connections and contains methods to read from and write to SQL database tables.
    """


    def __init__(self, db_cxn_params=None):
        self._get_db_params(db_cxn_params)
        self._get_db_uri()


    def _get_db_params(self, db_cxn_params):
        """"
        Open and read JSON file containing database connection parameters.
        """
        with open(f'database_connections/{db_cxn_params}') as f:
            self.db_params = json.load(f)


    def _get_db_uri(self):
        """
        Generate database connection parameters.
        NOTE: assumes using MS SQL
        """
        params = urllib.parse.quote_plus(
            f"DRIVER={{{self.db_params['sql_driver']}}};SERVER={self.db_params['server']};DATABASE={self.db_params['database']};Trusted_Connection={self.db_params['trusted_connection']}")
        self.db_uri = f"mssql+pyodbc:///?odbc_connect={params}"
    

    def _read_sql_file(self, filepath):
        """
        Read in SQL script.
        """
        with open(filepath, 'r') as file:
            return file.read()


    def sql_select(self, sql_query=None):
        """
        Input: SQL query (text)
        Output: result set as pd.DataFrame
        """
        with create_engine(self.db_uri).connect() as db_conn:
            return pd.concat([chunk for chunk in tqdm(pd.read_sql(text(sql_query), db_conn, chunksize=10000), desc='retrieving data from SQL')])
            

    def load_data(self, df, table_name):
        """
        Input: pd.DataFrame
        Output: None: saves dataframe to `table_name` 100K rows per iteration.
        """
        chunk_size = 100000 # rows to load per call to `pd.to_sql`
        with create_engine(self.db_uri).begin() as db_conn:
            for i in tqdm(range(0, len(df)+1, chunk_size), desc='loading transactions'):
                df_to_load = df.iloc[i:i+chunk_size]
                df_to_load.to_sql(table_name, con=db_conn, if_exists='append', index=False)
