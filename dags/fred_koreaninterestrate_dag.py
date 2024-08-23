from datetime import datetime, timedelta
import json
import logging
from dateutil.relativedelta import relativedelta
from airflow.decorators import dag, task
from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from pandas import DataFrame
from csv_manager import CsvManager
from fred_request_param_dvo import FredRequestParamDvo
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context
import ast
import pandas_datareader.data
from open_api_xcom_dto import OpenApiXcomDto
class FredKoreanInterestRateDag:
    def create_fred_koreaninterestrate_dag(dag_config_param : dict, dag_id : str, schedule_interval : timedelta, start_date : datetime, default_args : dict) -> DAG:
        @dag(dag_id=dag_id,
                    schedule_interval=schedule_interval,
                    params=dag_config_param,
                    start_date=start_date,
                    default_args=default_args)
        def fred_koreaninterestrate_dag() -> DAG:
            @task
            def open_api_request():
                context = get_current_context()
                cur_task_instance : TaskInstance = context['task_instance']
                prev_task_instance : TaskInstance = cur_task_instance.get_previous_ti()
                fred_request_param_dic : dict = {}
                fred_request_param_dvo : FredRequestParamDvo = None
                if(prev_task_instance is None):                    
                    fred_request_param_str : str = dag_config_param['uri']
                    fred_request_param_dic : dict = ast.literal_eval(fred_request_param_str)
                    logging.info(f"fred_request_param_dic : {fred_request_param_dic.__str__()}")
                    fred_request_param_dvo = FredRequestParamDvo.from_dict(fred_request_param_dic)
                    fred_request_param_dvo.end = datetime.strftime(datetime.strptime(fred_request_param_dvo.start, "%Y-%m-%d") + relativedelta(months=1), "%Y-%m-%d")
                else:
                    fred_request_param_dic : dict = prev_task_instance.xcom_pull(key=f"{dag_id}_{prev_task_instance.task_id}_{prev_task_instance.run_id}")
                    fred_request_param_dic = fred_request_param_dic.get('next_request_url',{})
                    fred_request_param_dvo = FredRequestParamDvo.from_dict(fred_request_param_dic)
                koreaninterestrate_dataframe : DataFrame = pandas_datareader.get_data_fred(fred_request_param_dvo.series, start=fred_request_param_dvo.start, end=fred_request_param_dvo.end)
                koreaninterestrate_dataframe.index = koreaninterestrate_dataframe.index.strftime("%Y-%m-%d")
                koreaninterestrate_json : dict = json.loads(koreaninterestrate_dataframe.to_json()).get('IR3TIB01KRM156N',{})
                open_api_xcom_dvo : OpenApiXcomDto = OpenApiXcomDto(response_json = koreaninterestrate_json)
                start : datetime = datetime.strptime(fred_request_param_dvo.start, "%Y-%m-%d")
                end : datetime = datetime.strptime(fred_request_param_dvo.end, "%Y-%m-%d")
                start = start + relativedelta(months=1)
                end = end + relativedelta(months=1)
                fred_request_param_dvo.start = start.strftime("%Y-%m-%d")
                fred_request_param_dvo.end = end.strftime("%Y-%m-%d")
                open_api_xcom_dvo.next_request_url = fred_request_param_dvo.to_dict()
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=open_api_xcom_dvo.to_dict())                
            @task
            def open_api_csv_save():
                context = get_current_context()
                cur_dag_run : DagRun = context['dag_run']
                cur_task_instance : TaskInstance = context['task_instance']
                cur_dag_run_open_api_request_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='open_api_request')                
                open_api_xcom_dvo : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_request_task_instance.xcom_pull(key=f"{dag_id}_{cur_dag_run_open_api_request_task_instance.task_id}_{cur_dag_run_open_api_request_task_instance.run_id}"))
                koreaninterestrate_json : dict = open_api_xcom_dvo.response_json
                csv_manager = CsvManager()
                csv_dir_path : str = dag_config_param['dir_path']
                csv_dir_path = csv_dir_path[1:csv_dir_path.__len__()]
                cur_dag_run_execution_date : datetime = cur_dag_run.execution_date
                csv_dir_path = csv_dir_path.replace("TIMESTAMP", cur_dag_run_execution_date.strftime("%Y-%m-%d"))
                csv_manager.save_csv(koreaninterestrate_json, csv_dir_path)
                open_api_xcom_dvo.csv_file_path = csv_dir_path
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=open_api_xcom_dvo.to_dict())
            @task
            def open_api_hdfs_save():
                context = get_current_context()
                cur_dag_run : DagRun = context['dag_run']
                cur_dag_run_open_api_csv_save_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='open_api_csv_save')                
                open_api_xcom_dvo : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_csv_save_task_instance.xcom_pull(key=f"{dag_id}_{cur_dag_run_open_api_csv_save_task_instance.task_id}_{cur_dag_run_open_api_csv_save_task_instance.run_id}"))
                csv_dir_path : str = open_api_xcom_dvo.csv_file_path
                try:
                    hdfs_hook = WebHDFSHook(webhdfs_conn_id='local_hdfs')
                    hdfs_client = hdfs_hook.get_conn()
                    hdfs_csv_path = csv_dir_path
                    hdfs_client.upload(hdfs_csv_path, csv_dir_path)
                    logging.info("File uploaded to HDFS successfully")
                    # os.remove(file_path)
                except Exception as e:
                    logging.error(f"Failed to upload file to HDFS: {e}")
                    raise                    
            open_api_request_task = open_api_request()
            open_api_csv_save_task = open_api_csv_save()
            open_api_hdfs_save_task = open_api_hdfs_save()
            open_api_request_task >> open_api_csv_save_task >> open_api_hdfs_save_task
        return fred_koreaninterestrate_dag()