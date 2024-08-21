import logging
from typing import List
from airflow.providers.mysql.hooks.mysql import MySqlHook
from api_admin_dvo import ApiAdminDvo
class ApiAdminDao:
    def __init__(self, mysql_conn_id):
        self.mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        self.connection = self.mysql_hook.get_conn()
        self.cursor = self.connection.cursor()
    def selectBySrcNm(self, src_nm) -> List[ApiAdminDvo]:
        try:        
            self.cursor.execute(f"SELECT * FROM api_admin_tb WHERE src_nm = '{src_nm}'")
            result = self.cursor.fetchall()
            api_admin_dvos : List[ApiAdminDvo] = []
            for row in result:
                api_admin_dvo = ApiAdminDvo(
                src_nm=row[0],
                tb_nm=row[1],
                tb_code=row[2],
                version=row[3],
                uri=row[4],
                created_at=row[5],
                dir_path=row[6],
                column1=row[7],
                eng_tb_nm=row[8]
                )
                api_admin_dvos.append(api_admin_dvo)
            return api_admin_dvos
        except Exception as e:
            logging.error(f"Error in selectBySrcNm: {e}")
    def selectAll(self) -> List[ApiAdminDvo]:
        try:    
            self.cursor.execute("SELECT * FROM api_admin_tb")
            result = self.cursor.fetchall()
            api_admin_dvos : List[ApiAdminDvo] = []
            for row in result:
                api_admin_dvo = ApiAdminDvo(
                src_nm=row[0],
                tb_nm=row[1],
                tb_code=row[2],
                version=row[3],
                uri=row[4],
                created_at=row[5],
                dir_path=row[6],
                column1=row[7],
                eng_tb_nm=row[8]
                )
                api_admin_dvos.append(api_admin_dvo)
            return api_admin_dvos
        except Exception as e:
            logging.error(f"Error in selectAll: {e}")
    def selectbyTbNm(self, tb_nm) -> ApiAdminDvo:
        try:
            self.cursor.execute(f"SELECT * FROM api_admin_tb WHERE tb_nm = '{tb_nm}'")
            result = self.cursor.fetchone()
            api_admin_dvo = ApiAdminDvo(
                src_nm=result[0],
                tb_nm=result[1],
                tb_code=result[2],
                version=result[3],
                uri=result[4],
                created_at=result[5],
                dir_path=result[6],
                column1=result[7],
                eng_tb_nm=result[8]
            )
            return api_admin_dvo
        except Exception as e:
            logging.error(f"Error in selectbyTbNm: {e}")
            
    def close(self):
        self.cursor.close()
        self.connection.close()