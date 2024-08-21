from datetime import datetime, timedelta
import json
import logging
from dateutil.relativedelta import relativedelta
from airflow.decorators import dag, task
from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from pandas import DataFrame
import yfinance
from csv_manager import CsvManager
from open_api_xcom_dvo import OpenApiXcomDvo
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context
import ast
from yfinance_usdtokrwexchangerate_request_param_dto import YFinanceUsdToKrwExchangeRateRequestParamDvo
class YFinanceUsdToKrwExchangeRateDag:
    def create_yfinance_usdtokrwexchangerate_dag(dag_config_param : dict, dag_id : str, schedule_interval : timedelta, start_date : datetime, default_args : dict) -> DAG:
        @dag(dag_id=dag_id,
                    schedule_interval=schedule_interval,
                    params=dag_config_param,
                    start_date=start_date,
                    default_args=default_args)
        def yfinance_usdtokrwexchangerate_dag() -> DAG:
            @task
            def open_api_request():
                context = get_current_context()
                cur_task_instance : TaskInstance = context['task_instance']
                prev_task_instance : TaskInstance = cur_task_instance.get_previous_ti()
                yfinanceusdtokrwexchangerate_request_param_dic : dict = {}
                yfinanceusdtokrwexchangerate_request_param_dvo : YFinanceUsdToKrwExchangeRateRequestParamDvo = None
                if(prev_task_instance is None):                    
                    yfinanceusdtokrwexchangerate_request_param_str : str = dag_config_param['uri']
                    yfinanceusdtokrwexchangerate_request_param_dic : dict = ast.literal_eval(yfinanceusdtokrwexchangerate_request_param_str)
                    logging.info(f"yfinanceusdtokrwexchangerate_request_param_dic : {yfinanceusdtokrwexchangerate_request_param_dic.__str__()}")
                    yfinanceusdtokrwexchangerate_request_param_dvo = YFinanceUsdToKrwExchangeRateRequestParamDvo.from_dict(yfinanceusdtokrwexchangerate_request_param_dic)
                else:
                    yfinanceusdtokrwexchangerate_request_param_dic : dict = prev_task_instance.xcom_pull(key=f"{dag_id}_{prev_task_instance.task_id}_{prev_task_instance.run_id}")
                    logging.info(f"yfinanceusdtokrwexchangerate_request_param_dic : {yfinanceusdtokrwexchangerate_request_param_dic.__str__()}")
                    yfinanceusdtokrwexchangerate_request_param_dvo = YFinanceUsdToKrwExchangeRateRequestParamDvo.from_dict(yfinanceusdtokrwexchangerate_request_param_dic)
                usdtokrwexchangerate_dataframe : DataFrame = yfinance.download(yfinanceusdtokrwexchangerate_request_param_dvo.ticker, start=yfinanceusdtokrwexchangerate_request_param_dvo.start, end=yfinanceusdtokrwexchangerate_request_param_dvo.end, interval=yfinanceusdtokrwexchangerate_request_param_dvo.interval)
                usdtokrwexchangerate_json : dict = json.loads(usdtokrwexchangerate_dataframe.to_json())
                open_api_xcom_dvo : OpenApiXcomDvo = OpenApiXcomDvo(response_json = usdtokrwexchangerate_json)
                start : datetime = datetime.strptime(yfinanceusdtokrwexchangerate_request_param_dvo.start, "%Y-%m-%d")
                end : datetime = datetime.strptime(yfinanceusdtokrwexchangerate_request_param_dvo.end, "%Y-%m-%d")
                start = start + relativedelta(months=1)
                end = end + relativedelta(months=1)
                yfinanceusdtokrwexchangerate_request_param_dvo.start = start.strftime("%Y-%m-%d")
                yfinanceusdtokrwexchangerate_request_param_dvo.end = end.strftime("%Y-%m-%d")
                open_api_xcom_dvo.next_request_url = yfinanceusdtokrwexchangerate_request_param_dvo.to_dict()
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=open_api_xcom_dvo.to_dict())                
            @task
            def open_api_csv_save():
                context = get_current_context()
                cur_dag_run : DagRun = context['dag_run']
                cur_task_instance : TaskInstance = context['task_instance']
                cur_dag_run_open_api_request_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='open_api_request')                
                open_api_xcom_dvo : OpenApiXcomDvo = OpenApiXcomDvo.from_dict(cur_dag_run_open_api_request_task_instance.xcom_pull(key=f"{dag_id}_{cur_dag_run_open_api_request_task_instance.task_id}_{cur_dag_run_open_api_request_task_instance.run_id}"))
                usdtokrwexchangerate_json : dict = open_api_xcom_dvo.response_json
                csv_manager = CsvManager()
                csv_dir_path : str = dag_config_param['dir_path']
                csv_dir_path = csv_dir_path[1:csv_dir_path.__len__()]
                open_api_xcom_dvo.csv_file_path = csv_dir_path
                cur_dag_run_execution_date : datetime = cur_dag_run.execution_date
                csv_manager.save_csv(usdtokrwexchangerate_json, csv_dir_path.replace("TIMESTAMP", cur_dag_run_execution_date.strftime("%Y-%m-%d")))
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=open_api_xcom_dvo.to_dict())
            @task
            def open_api_hdfs_save():
                context = get_current_context()
                cur_dag_run : DagRun = context['dag_run']
                cur_dag_run_open_api_csv_save_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='open_api_csv_save')                
                open_api_xcom_dvo : OpenApiXcomDvo = OpenApiXcomDvo.from_dict(cur_dag_run_open_api_csv_save_task_instance.xcom_pull(key=f"{dag_id}_{cur_dag_run_open_api_csv_save_task_instance.task_id}_{cur_dag_run_open_api_csv_save_task_instance.run_id}"))
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
        return yfinance_usdtokrwexchangerate_dag()
                