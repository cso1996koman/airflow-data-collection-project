from airflow.decorators import dag, task
from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.utils.context import Context
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import logging
import ast
from pandas import DataFrame
import yfinance
from csv_manager import CsvManager
from open_api_xcom_dto import OpenApiXcomDto
from yfinance_usdtokrwexchangerate_request_param import YFinanceUsdToKrwExchangeRateRequestParam
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
                yfinanceusdtokrwexchangerate_request_param : dict = {}
                prev_or_first_task_instance_yfinanceusdtokrwexchangerate_request_param : YFinanceUsdToKrwExchangeRateRequestParam = None
                #fisrt task instance xcom init
                if(prev_task_instance is None):                    
                    yfinanceusdtokrwexchangerate_request_param : str = dag_config_param['uri']
                    yfinanceusdtokrwexchangerate_request_param : dict = ast.literal_eval(yfinanceusdtokrwexchangerate_request_param)
                    logging.info(f"yfinanceusdtokrwexchangerate_request_param : {yfinanceusdtokrwexchangerate_request_param.__str__()}")
                    prev_or_first_task_instance_yfinanceusdtokrwexchangerate_request_param = YFinanceUsdToKrwExchangeRateRequestParam.from_dict(yfinanceusdtokrwexchangerate_request_param)
                else:
                    prev_task_instance_xcom_key_str : str = f"{dag_id}_{prev_task_instance.task_id}_{prev_task_instance.run_id}"
                    prev_or_first_task_instance_xcom_dvo = OpenApiXcomDto.from_dict(prev_task_instance.xcom_pull(key=prev_task_instance_xcom_key_str))
                    logging.info(f"yfinanceusdtokrwexchangerate_request_param : {yfinanceusdtokrwexchangerate_request_param.__str__()}")
                    yfinanceusdtokrwexchangerate_request_param_str : str = prev_or_first_task_instance_xcom_dvo.next_request_url
                    yfinanceusdtokrwexchangerate_request_param_dict : dict = ast.literal_eval(yfinanceusdtokrwexchangerate_request_param_str) 
                    prev_or_first_task_instance_yfinanceusdtokrwexchangerate_request_param = YFinanceUsdToKrwExchangeRateRequestParam.from_dict(yfinanceusdtokrwexchangerate_request_param_dict)
                usdtokrwexchangerate_dataframe : DataFrame = yfinance.download(tickers = prev_or_first_task_instance_yfinanceusdtokrwexchangerate_request_param.ticker,
                                                                               start=prev_or_first_task_instance_yfinanceusdtokrwexchangerate_request_param.start,
                                                                               end=prev_or_first_task_instance_yfinanceusdtokrwexchangerate_request_param.end,
                                                                               interval=prev_or_first_task_instance_yfinanceusdtokrwexchangerate_request_param.interval)
                usdtokrwexchangerate_json : dict = json.loads(usdtokrwexchangerate_dataframe.to_json())
                cur_task_instance_xcom_yfinanceusdtokrwexchangerate_request_param = YFinanceUsdToKrwExchangeRateRequestParam(ticker = prev_or_first_task_instance_yfinanceusdtokrwexchangerate_request_param.ticker,
                                                                                                                            start = prev_or_first_task_instance_yfinanceusdtokrwexchangerate_request_param.start,
                                                                                                                            end = prev_or_first_task_instance_yfinanceusdtokrwexchangerate_request_param.end,
                                                                                                                            interval = prev_or_first_task_instance_yfinanceusdtokrwexchangerate_request_param.interval)
                start : datetime = datetime.strptime(cur_task_instance_xcom_yfinanceusdtokrwexchangerate_request_param.start, "%Y-%m-%d")
                end : datetime = datetime.strptime(cur_task_instance_xcom_yfinanceusdtokrwexchangerate_request_param.end, "%Y-%m-%d")
                start = start + relativedelta(months=1)
                end = end + relativedelta(months=1)
                cur_task_instance_xcom_yfinanceusdtokrwexchangerate_request_param.start = start.strftime("%Y-%m-%d")
                cur_task_instance_xcom_yfinanceusdtokrwexchangerate_request_param.end = end.strftime("%Y-%m-%d")
                cur_task_instance_xcom : OpenApiXcomDto = OpenApiXcomDto(next_request_url=cur_task_instance_xcom_yfinanceusdtokrwexchangerate_request_param.to_dict().__str__(), response_json=usdtokrwexchangerate_json)
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value = cur_task_instance_xcom.to_dict())
            @task
            def open_api_csv_save():
                context = get_current_context()
                cur_dag_run : DagRun = context['dag_run']
                cur_task_instance : TaskInstance = context['task_instance']
                cur_dag_run_open_api_request_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='open_api_request')
                cur_dag_run_open_api_request_xcom_key_str : str = f"{dag_id}_{cur_dag_run_open_api_request_task_instance.task_id}_{cur_dag_run_open_api_request_task_instance.run_id}"
                cur_dag_run_open_api_request_task_instance_xcom = OpenApiXcomDto.from_dict(cur_dag_run_open_api_request_task_instance.xcom_pull(key=cur_dag_run_open_api_request_xcom_key_str))
                usdtokrwexchangerate_json : dict = cur_dag_run_open_api_request_task_instance_xcom.response_json
                csv_manager = CsvManager()
                csv_dir_path : str = dag_config_param['dir_path']
                csv_dir_path = csv_dir_path[1:csv_dir_path.__len__()]
                cur_dag_run_execution_date : datetime = cur_dag_run.execution_date
                csv_dir_path = csv_dir_path.replace("TIMESTAMP", cur_dag_run_execution_date.strftime("%Y-%m-%d"))
                cur_dag_run_open_api_request_task_instance_xcom.csv_file_path = csv_dir_path
                csv_manager.save_csv(usdtokrwexchangerate_json,csv_dir_path)
                cur_dag_run_open_api_csv_save_task_instance_xcom : OpenApiXcomDto = cur_dag_run_open_api_request_task_instance_xcom
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=cur_dag_run_open_api_csv_save_task_instance_xcom.to_dict())
            @task
            def open_api_hdfs_save():
                context = get_current_context()
                cur_dag_run : DagRun = context['dag_run']
                cur_dag_run_open_api_csv_save_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='open_api_csv_save')
                cur_dag_run_open_api_csv_save_xcom_key_str : str = f"{dag_id}_{cur_dag_run_open_api_csv_save_task_instance.task_id}_{cur_dag_run_open_api_csv_save_task_instance.run_id}"
                cur_dag_run_open_api_csv_save_task_instance_xcom = OpenApiXcomDto.from_dict(cur_dag_run_open_api_csv_save_task_instance.xcom_pull(key=cur_dag_run_open_api_csv_save_xcom_key_str))
                csv_file_path : str = cur_dag_run_open_api_csv_save_task_instance_xcom.csv_file_path
                try:
                    hdfs_hook = WebHDFSHook(webhdfs_conn_id='local_hdfs')
                    hdfs_client = hdfs_hook.get_conn()
                    hdfs_csv_path = csv_file_path
                    hdfs_client.upload(hdfs_csv_path, csv_file_path)
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