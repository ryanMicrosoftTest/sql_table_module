"""
This module will write log data to sql table objects of expected structure
"""
import pyodbc
import datetime
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential


class ADFpipelinesSQL_Logger:
    def __init__(self, server_name, database_name, username, kv_url:str, secret_name):
        self.server_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = self._get_kv_secret(kv_url, secret_name)
        self.connection = self._connect_to_database()
        self.max_rerun_pipelines_list = []

    def _get_kv_secret(self, kv_url:str, secret_name:str)->str:

        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=kv_url, credential=credential)
        secret_value = client.get_secret(secret_name).value

        return secret_value

    def _connect_to_database(self):
        connection = pyodbc.connect(
            f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={self.server_name};DATABASE={self.database_name};UID={self.username};PWD={self.password}"
        )
        return connection
    
    def _check_run_id_exists(self, run_id:str)->bool:
        """
        """
        cursor = self.connection.cursor()
        # need to determine if record exists and if so increment instead of inserting
        sql_result = cursor.execute(f"SELECT run_id FROM dbo.tblFailedPipelines WHERE run_id = '{run_id}'").fetchall()

        if len(sql_result) > 0:
            return True
        else:
            return False
        
    def _update_total_runs(self, run_id:str):
            """
            """
            cursor = self.connection.cursor()

            # increment total_runs
            cursor.execute(
                f"UPDATE dbo.tblFailedPipelines SET total_runs = total_runs + 1 WHERE run_id = '{run_id}'"
            )
            cursor.commit()
            cursor.close()

    def _check_max_reruns(self, run_id:str)->str:
        """
        """
        cursor = self.connection.cursor()
        # need to determine if record exists and if so increment instead of inserting
        sql_result = cursor.execute(f"SELECT total_runs FROM dbo.tblFailedPipelines WHERE run_id = '{run_id}'").fetchall()

        if sql_result[0][0] >= 3:
            return 'email'
        else:
            return 'rerun'

    def insert_failed_pipeline(self, run_id:str, pipeline_name:str, data_factory:str, status:str, start_time:str, end_time:str, message:str)->str:
        """
        This will insert or update the run id of a failed pipeline into the SQL table
        If the run id already exists, the total_runs column will be incremented
        If the total_runs column is greater than or equal to the configurable max total_reruns, this will not increment and will instead return
        'email' as a result, indicating that the max reruns has been reached and an email should be sent to the operations team

        return:str ['insert', 'update', 'email']
        """

        # reduce error message to 128 characters
        message = self._clean_message(message)

        cursor = self.connection.cursor()
        # need to determine if record exists and if so increment instead of inserting
        does_exist = self._check_run_id_exists(run_id)

        if does_exist == True:
            # check if max total_reruns has been reached
            if self._check_max_reruns(run_id) == 'email':
                self.max_rerun_pipelines_list.append(run_id)
                return 'email'
            self._update_total_runs(run_id)
            cursor.close()
            return 'update'
        else:
            cursor.execute(
                f"""INSERT INTO dbo.tblFailedPipelines (run_id, pipeline_name, data_factory, status, start_time, end_time, message, total_runs) VALUES ('{run_id}', '{pipeline_name}', '{data_factory}', '{status}', '{start_time}', '{end_time}', '{message}', 1)"""
            )
            cursor.commit()
            cursor.close()
            return 'insert'

    def insert_rerun_activity(self, original_run_id:str, rerun_id:str, activity_name:str):
        cursor = self.connection.cursor()

        current_time = self.current_iso_time()
        cursor.execute(
            f"INSERT INTO dbo.tblRerunActivities (original_run_id, rerun_id, activity_name, rerun_start_time) VALUES ('{original_run_id}', '{rerun_id}', '{activity_name}', '{current_time}')"
        )
        cursor.commit()
        cursor.close()

    def current_iso_time(self):
        return datetime.datetime.now().isoformat()
    
    def _clean_message(self, message:str)->str:
        message = message[:120]

        # remove any ' or " characters from message
        message = message.replace("'", "")
        message = message.replace('"', '')
        
        return message

    def close_connection(self):
        self.connection.close()
