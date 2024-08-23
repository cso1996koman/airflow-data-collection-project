from api_admin_dao import ApiAdminDao
from api_admin_dvo import ApiAdminDvo
from logisticsinfocenter_table_converter import LOGISTICSINFOCENTERTABLENAME
import logisticsinfocenter_table_converter
import datetime
import os
import re
from typing import List
from airflow import DAG
from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context
from airflow.models.dagrun import DagRun 
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
class LogisticsInfoCenter:
    @staticmethod
    def create_logisticsinfocenter_dag(dag_config_param : dict, dag_id : str, schedule_interval : str, start_date : datetime, default_args : dict, api_admin_dao : ApiAdminDao) -> DAG:
        @dag(dag_id=dag_id, schedule_interval=schedule_interval, start_date=start_date, default_args=default_args)
        def logisticsinfocenter_dag():
            @task.sensor(poke_interval=60, timeout=3600, mode='reschedule')
            def wait_for_file():
                cur_task_instance : TaskInstance = get_current_context()['task_instance']
                directory_path = dag_config_param['dir_path']
                files : List[str]= os.listdir(directory_path)
                if len(files) > 0:
                    cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=files[0])
                    return PokeReturnValue(True)
                return PokeReturnValue(False)
            @task
            def convert_file():
                cur_run_dag : DagRun = get_current_context()['dag_run']
                cur_run_dag_wait_for_file_task = cur_run_dag.get_task_instance('wait_for_file')
                cur_run_dag_wait_for_file_task_xcom_file_directory_path : str = cur_run_dag_wait_for_file_task.xcom_pull(key=f"{dag_id}_{cur_run_dag_wait_for_file_task.task_id}_{cur_run_dag_wait_for_file_task.run_id}")
                match = re.search(r'[가-힣\s]+(\([가-힣]+\))?', cur_run_dag_wait_for_file_task_xcom_file_directory_path)
                tb_nm = match.group()
                date = re.findall(r'20\d{2}', cur_run_dag_wait_for_file_task_xcom_file_directory_path)
                if tb_nm == LOGISTICSINFOCENTERTABLENAME.ODBYROADTRANSPORTPERFORMANCE:
                    api_admin_dvo : ApiAdminDvo = api_admin_dao.selectbyTbNm(tb_nm)
                    logisticsinfocenter_table_converter.BRZ_1C070201(api_admin_dvo)
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.ODBYRAILTRANSPORTPERFORMANCE:
                    pass
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.AIRPORTCARGOVOLUMESTATISTICS:
                    # Logic for AIRPORTCARGOVOLUMESTATISTICS
                    print("Processing 공항별 물동량 통계(항공)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.ANNUALDOMESTICCOURIERMARKETUNITPRICE:
                    # Logic for ANNUALDOMESTICCOURIERMARKETUNITPRICE
                    print("Processing 국내 택배시장 단가(연도)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.MONTHLYDOMESTICCOURIERMARKETUNITPRICE:
                    # Logic for MONTHLYDOMESTICCOURIERMARKETUNITPRICE
                    print("Processing 국내 택배시장 단가(월별)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.ANNUALDOMESTICCOURIERMARKETREVENUE:
                    # Logic for ANNUALDOMESTICCOURIERMARKETREVENUE
                    print("Processing 국내 택배시장 매출액(연도)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.MONTHLYDOMESTICCOURIERMARKETREVENUE:
                    # Logic for MONTHLYDOMESTICCOURIERMARKETREVENUE
                    print("Processing 국내 택배시장 매출액(월별)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.ANNUALDOMESTICCOURIERMARKETVOLUME:
                    # Logic for ANNUALDOMESTICCOURIERMARKETVOLUME
                    print("Processing 국내 택배시장 물동량(연도)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.MONTHLYDOMESTICCOURIERMARKETVOLUME:
                    # Logic for MONTHLYDOMESTICCOURIERMARKETVOLUME
                    print("Processing 국내 택배시장 물동량(월별)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.ANNUALDOMESTICCOURIERMARKETUSAGEFREQUENCY:
                    # Logic for ANNUALDOMESTICCOURIERMARKETUSAGEFREQUENCY
                    print("Processing 국내 택배시장 이용횟수(연도)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.ROUTECARGOVOLUMESTATISTICS:
                    # Logic for ROUTECARGOVOLUMESTATISTICS
                    print("Processing 노선별 물동량 통계(항공)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.AREABASEDLOGISTICSWAREHOUSEREGISTRATION:
                    # Logic for AREABASEDLOGISTICSWAREHOUSEREGISTRATION
                    print("Processing 면적별 물류창고업 등록현황")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.VESSELARRIVALDEPARTURESTATISTICS:
                    # Logic for VESSELARRIVALDEPARTURESTATISTICS
                    print("Processing 선박 입출항 통계(해운)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.EXPORTIMPORTCARGOVOLUMESTATISTICS:
                    # Logic for EXPORTIMPORTCARGOVOLUMESTATISTICS
                    print("Processing 수출입화물 물동량 통계(항공)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.ANNUALLOGISTICSWAREHOUSEREGISTRATION:
                    # Logic for ANNUALLOGISTICSWAREHOUSEREGISTRATION
                    print("Processing 연도별 물류창고업 등록현황")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.REGIONCOUNTRYCARGOVOLUMESTATISTICS:
                    # Logic for REGIONCOUNTRYCARGOVOLUMESTATISTICS
                    print("Processing 지역·국가별 물동량 통계(항공)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.REGIONBASEDLOGISTICSWAREHOUSEREGISTRATION:
                    # Logic for REGIONBASEDLOGISTICSWAREHOUSEREGISTRATION
                    print("Processing 지역별 물류창고업 등록현황")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.VEHICLETYPEFREIGHTTRUCKREGISTRATION:
                    # Logic for VEHICLETYPEFREIGHTTRUCKREGISTRATION
                    print("Processing 차종별 화물차 등록현황(도로)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.RAILFREIGHTDISPATCHANDARRIVALPERFORMANCE:
                    # Logic for RAILFREIGHTDISPATCHANDARRIVALPERFORMANCE
                    print("Processing 철도 화물 발송/도착실적(철도)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.ITEMCARGOVOLUMESTATISTICS:
                    # Logic for ITEMCARGOVOLUMESTATISTICS
                    print("Processing 품목별 물동량 통계(해운)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.ROADITEMFREIGHTTRANSPORTPERFORMANCE:
                    # Logic for ROADITEMFREIGHTTRANSPORTPERFORMANCE
                    print("Processing 품목별 화물 수송 실적(도로)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.RAILITEMFREIGHTTRANSPORTPERFORMANCE:
                    # Logic for RAILITEMFREIGHTTRANSPORTPERFORMANCE
                    print("Processing 품목별 화물 수송 실적(철도)")
                elif tb_nm == LOGISTICSINFOCENTERTABLENAME.PORTCARGOVOLUMESTATISTICS:
                    # Logic for PORTCARGOVOLUMESTATISTICS
                    print("Processing 항만별 물동량 통계(해운)")
                else:
                    assert False, f"Invalid table name: {tb_nm}"
                
            @task
            def upload_to_hdfs():
                cur_dag_run : DagRun = get_current_context()['dag_run']
                cur_dag_run_convert_file_task = cur_dag_run.get_task_instance('convert_file')
                cur_dag_run_convert_file_task_xcom_file_directory_path : str = cur_dag_run_convert_file_task.xcom_pull(key=f"{dag_id}_{cur_dag_run_convert_file_task.task_id}_{cur_dag_run_convert_file_task.run_id}")
                hdfs_file_path = cur_dag_run_convert_file_task_xcom_file_directory_path
                try:
                    hdfs_hook = WebHDFSHook(webhdfs_conn_id='local_hdfs')
                    hdfs_client = hdfs_hook.get_conn()
                    hdfs_client.upload(hdfs_file_path, cur_dag_run_convert_file_task_xcom_file_directory_path)
                except:
                    assert False, f"Failed to upload file to HDFS: {hdfs_file_path}"
            wait_for_file_task = wait_for_file()
            convert_file_task = convert_file()
            upload_to_hdfs_task = upload_to_hdfs()
            wait_for_file_task >> convert_file_task >> upload_to_hdfs_task
        return logisticsinfocenter_dag()