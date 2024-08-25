from airflow.decorators import dag, task
from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context
from airflow.utils.context import Context
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pandas import DataFrame
import pandas_datareader.data
import json
import logging
import ast
from csv_manager import CsvManager
from open_api_xcom_dto import OpenApiXcomDto
from fred_request_param_dvo import FredRequestParamDvo
class FredOilPriceDag:
    def create_fred_oilprice_dag(dag_config_param : dict, dag_id : str, schedule_interval : timedelta, start_date : datetime, default_args : dict) -> DAG:
        @dag(dag_id=dag_id,
                    schedule_interval=schedule_interval,
                    params=dag_config_param,
                    start_date=start_date,
                    default_args=default_args)
        def fred_oilprice_dag() -> DAG:
            @task
            def open_api_request():
                cur_context : Context = get_current_context()
                cur_task_instance : TaskInstance = cur_context['task_instance']
                prev_task_instance_or_none : TaskInstance = cur_task_instance.get_previous_ti()
                prev_or_first_task_instance_request_param_dvo : FredRequestParamDvo = None
                
                if(prev_task_instance_or_none is None):                    
                    config_request_param_str : str = dag_config_param['uri']
                    config_request_param_dict : dict = ast.literal_eval(config_request_param_str)
                    prev_or_first_task_instance_request_param_dvo = FredRequestParamDvo.from_dict(config_request_param_dict)
                    prev_or_first_task_instance_request_param_dvo.end = datetime.strftime(datetime.strptime(prev_or_first_task_instance_request_param_dvo.start , "%Y-%m-%d")
                                                                                          + relativedelta(months=1) + relativedelta(days=-1),
                                                                                          "%Y-%m-%d")
                else:
                    prev_task_instance_xcom_dict : dict = prev_task_instance_or_none.xcom_pull(key=f"{dag_id}_{prev_task_instance_or_none.task_id}_{prev_task_instance_or_none.run_id}")
                    prev_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(prev_task_instance_xcom_dict)
                    prev_request_param_str : str = prev_task_instance_xcom_dto.next_request_url
                    prev_request_param_dic : dict = ast.literal_eval(prev_request_param_str)
                    prev_or_first_task_instance_request_param_str : str = prev_task_instance_xcom_dict.get('next_request_url',{})
                    prev_or_first_task_instance_request_param_dict : dict = ast.literal_eval(prev_or_first_task_instance_request_param_str)
                    prev_or_first_task_instance_request_param_dvo = FredRequestParamDvo.from_dict(prev_or_first_task_instance_request_param_dict)
                                        
                oilprice_dataframe : DataFrame = pandas_datareader.get_data_fred(prev_or_first_task_instance_request_param_dvo.series,
                                                                                 start=prev_or_first_task_instance_request_param_dvo.start, 
                                                                                 end=prev_or_first_task_instance_request_param_dvo.end)
                oilprice_dataframe.index = oilprice_dataframe.index.strftime("%Y-%m-%d")
                oilprice_json : dict = json.loads(oilprice_dataframe.to_json()).get('DCOILWTICO',{})
                start : datetime = datetime.strptime(prev_or_first_task_instance_request_param_dvo.start, "%Y-%m-%d")
                end : datetime = datetime.strptime(prev_or_first_task_instance_request_param_dvo.end, "%Y-%m-%d")
                start = start + relativedelta(months=1)
                end = end + relativedelta(months=1) + relativedelta(days=-1)
                prev_or_first_task_instance_request_param_dvo.start = start.strftime("%Y-%m-%d")
                prev_or_first_task_instance_request_param_dvo.end = end.strftime("%Y-%m-%d")
                cur_task_instance_xcom_dto = OpenApiXcomDto(response_json = oilprice_json, next_request_url = prev_or_first_task_instance_request_param_dvo.to_dict())
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=cur_task_instance_xcom_dto.to_dict())
            @task
            def open_api_csv_save():
                cur_context : Context = get_current_context()
                cur_dag_run : DagRun = cur_context['dag_run']
                cur_task_instance : TaskInstance = cur_context['task_instance']
                cur_dag_run_open_api_request_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='open_api_request')
                xcom_key_str = f"{dag_id}_{cur_dag_run_open_api_request_task_instance.task_id}_{cur_dag_run_open_api_request_task_instance.run_id}"
                cur_dag_run_open_api_request_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_request_task_instance.xcom_pull(key=xcom_key_str))
                oilprice_json : dict = ast.literal_eval(cur_dag_run_open_api_request_task_instance_xcom_dto.response_json)
                csv_manager = CsvManager()
                csv_dir_path : str = dag_config_param['dir_path']
                csv_dir_path = csv_dir_path[1:csv_dir_path.__len__()]
                csv_dir_path = csv_dir_path.replace("TIMESTAMP", cur_dag_run.execution_date.strftime("%Y-%m-%d"))
                csv_manager.save_csv(oilprice_json, csv_dir_path)
                cur_dag_run_open_api_csv_save_task_instance_xcom_dto = cur_dag_run_open_api_request_task_instance_xcom_dto
                cur_dag_run_open_api_csv_save_task_instance_xcom_dto.csv_file_path = csv_dir_path
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=cur_dag_run_open_api_csv_save_task_instance_xcom_dto.to_dict())
            @task
            def open_api_hdfs_save():
                cur_context : Context = get_current_context()
                cur_dag_run : DagRun = cur_context['dag_run']
                cur_dag_run_open_api_csv_save_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='open_api_csv_save')
                cur_dag_run_open_api_csv_save_xcom_key_str : str = f"{dag_id}_{cur_dag_run_open_api_csv_save_task_instance.task_id}_{cur_dag_run_open_api_csv_save_task_instance.run_id}"
                xcom_dict : dict = cur_dag_run_open_api_csv_save_task_instance.xcom_pull(key=cur_dag_run_open_api_csv_save_xcom_key_str)
                cur_dag_run_open_api_csv_save_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(xcom_dict)
                csv_dir_path : str = cur_dag_run_open_api_csv_save_task_instance_xcom_dto.csv_file_path
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
        return fred_oilprice_dag()