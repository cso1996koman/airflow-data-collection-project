from airflow.decorators import dag, task
from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context
from airflow.utils.context import Context
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import logging
import ast
import pandas_datareader.data
from pandas import DataFrame
from csv_manager import CsvManager
from fred_request_param_dvo import FredRequestParamDvo
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
                cur_context : Context = get_current_context()
                cur_task_instance : TaskInstance = cur_context['task_instance']
                prev_task_instance_or_none : TaskInstance = cur_task_instance.get_previous_ti()
                prev_task_instance_xcom_request_param_dvo : FredRequestParamDvo = None
                if(prev_task_instance_or_none is None):                    
                    request_param_str : str = dag_config_param['uri']
                    request_param_dic : dict = ast.literal_eval(request_param_str)
                    logging.info(f"fred_request_param_dic : {request_param_dic.__str__()}")
                    prev_task_instance_xcom_request_param_dvo = FredRequestParamDvo.from_dict(request_param_dic)
                    prev_task_instance_xcom_request_param_dvo.end = datetime.strftime(datetime.strptime(prev_task_instance_xcom_request_param_dvo.start, "%Y-%m-%d")
                                                                                      + relativedelta(months=1) + relativedelta(days=-1), "%Y-%m-%d") 
                else:
                    pre_task_instance_xcom_key_str : str = f"{dag_id}_{prev_task_instance_or_none.task_id}_{prev_task_instance_or_none.run_id}"
                    pre_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(prev_task_instance_or_none.xcom_pull(key=pre_task_instance_xcom_key_str))
                    prev_task_instance_xcom_request_param_dic : dict = ast.literal_eval(pre_task_instance_xcom_dto.next_request_url)
                    prev_task_instance_xcom_request_param_dic = prev_task_instance_xcom_request_param_dic.get('next_request_url',{})
                    prev_task_instance_xcom_request_param_dvo = FredRequestParamDvo.from_dict(prev_task_instance_xcom_request_param_dic)
                    
                koreaninterestrate_dataframe : DataFrame = pandas_datareader.data.get_data_fred(prev_task_instance_xcom_request_param_dvo.series,
                                                                                           start = prev_task_instance_xcom_request_param_dvo.start,
                                                                                           end = prev_task_instance_xcom_request_param_dvo.end)
                data_frame_index_datetime : datetime = koreaninterestrate_dataframe.index
                koreaninterestrate_dataframe.index = data_frame_index_datetime.strftime("%Y-%m-%d")
                koreaninterestrate_dataframe_body_json : dict = json.loads(koreaninterestrate_dataframe.to_json()).get('IR3TIB01KRM156N',{})
                prev_request_param_dvo_start_datetime_obj : datetime = datetime.strptime(prev_task_instance_xcom_request_param_dvo.start, "%Y-%m-%d")
                prev_request_param_dvo_end_datetime_obj : datetime = datetime.strptime(prev_task_instance_xcom_request_param_dvo.end, "%Y-%m-%d")
                next_request_param_dvo_start = prev_request_param_dvo_start_datetime_obj + relativedelta(months=1)
                next_request_param_dvo_end = prev_request_param_dvo_end_datetime_obj + relativedelta(months=1)
                prev_task_instance_xcom_request_param_dvo.start = next_request_param_dvo_start.strftime("%Y-%m-%d")
                prev_task_instance_xcom_request_param_dvo.end = next_request_param_dvo_end.strftime("%Y-%m-%d")
                next_task_instance_xcom_request_param_dic = prev_task_instance_xcom_request_param_dvo.to_dict()
                cur_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto(next_request_url =  next_task_instance_xcom_request_param_dic, response_json = koreaninterestrate_dataframe_body_json)
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=cur_task_instance_xcom_dto.to_dict())
            @task
            def open_api_csv_save():
                cur_context : Context = get_current_context()
                cur_dag_run : DagRun = cur_context['dag_run']
                cur_task_instance : TaskInstance = cur_context['task_instance']
                cur_dag_run_open_api_request_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='open_api_request')
                cur_dag_run_open_api_request_xcom_key_str : str = f"{dag_id}_{cur_dag_run_open_api_request_task_instance.task_id}_{cur_dag_run_open_api_request_task_instance.run_id}"
                cur_dag_run_open_api_request_task_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_request_task_instance.xcom_pull(key=cur_dag_run_open_api_request_xcom_key_str))
                koreaninterestrate_json : dict = cur_dag_run_open_api_request_task_xcom_dto.response_json
                csv_manager = CsvManager()
                csv_dir_path : str = dag_config_param['dir_path']
                csv_dir_path = csv_dir_path[1:csv_dir_path.__len__()]
                cur_dag_run_execution_date : datetime = cur_dag_run.execution_date
                csv_dir_path = csv_dir_path.replace("TIMESTAMP", cur_dag_run_execution_date.strftime("%Y-%m-%d"))
                csv_manager.save_csv(koreaninterestrate_json, csv_dir_path)
                cur_dag_run_open_api_csv_save_task_instance_xcom_dto : OpenApiXcomDto = cur_dag_run_open_api_request_task_xcom_dto
                cur_dag_run_open_api_csv_save_task_instance_xcom_dto.csv_file_path = csv_dir_path
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=cur_dag_run_open_api_csv_save_task_instance_xcom_dto.to_dict())
            @task
            def open_api_hdfs_save():
                cur_context : Context = get_current_context()
                cur_dag_run : DagRun = cur_context['dag_run']
                cur_dag_run_open_api_csv_save_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='open_api_csv_save')
                cur_dag_run_open_api_csv_save_xcom_key_str : str = f"{dag_id}_{cur_dag_run_open_api_csv_save_task_instance.task_id}_{cur_dag_run_open_api_csv_save_task_instance.run_id}"
                cur_dag_run_open_api_csv_save_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_csv_save_task_instance.xcom_pull(key=cur_dag_run_open_api_csv_save_xcom_key_str))
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
        return fred_koreaninterestrate_dag()