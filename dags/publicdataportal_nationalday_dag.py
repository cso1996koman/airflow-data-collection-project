from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.models import TaskInstance
from airflow.models import DagRun
from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.utils.context import Context
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
from csv_manager import CsvManager
from publicdataportal_nationalday_url import PublicDataPortalNationalDayUrl
from open_api_helper import OpenApiHelper
from open_api_xcom_dto import OpenApiXcomDto
from url_object_factory import UrlObjectFactory
class PublicDataPortalNationalDayDag:
    @staticmethod
    def create_publicdataportal_nationalday_dag(dag_config_param : dict, dag_id : str, schedule_interval : timedelta, start_date : datetime, default_args : dict) -> DAG:
        @dag(dag_id=dag_id,
                    schedule_interval=schedule_interval,
                    params=dag_config_param,
                    start_date=start_date,
                    default_args=default_args)
        def publicdataportal_nationalday_dag() -> DAG:
            @task
            def open_api_request():
                cur_context : Context = get_current_context()
                cur_task_instance : TaskInstance = cur_context['task_instance']
                prev_task_instance_or_None : TaskInstance = cur_task_instance.get_previous_ti()
                cur_request_url_obj : PublicDataPortalNationalDayUrl = None
                cur_request_url = None
                
                if prev_task_instance_or_None is None:
                    cur_request_url = dag_config_param['uri']
                    assert cur_request_url is not None
                    cur_request_url_obj : PublicDataPortalNationalDayUrl = UrlObjectFactory.createPublicDataPortalNationalDayUrl(cur_request_url)
                    cur_request_url_obj.serviceKey = dag_config_param['api_keys']
                    cur_request_url_obj.solYear = datetime(2015,1,1).strftime('%Y')
                    cur_request_url_obj.solMonth = datetime(2015,1,1).strftime('%m')
                    cur_request_url = cur_request_url_obj.get_full_url()
                else:
                    prev_task_instance_xcom_key_str : str = f"{dag_id}_{prev_task_instance_or_None.task_id}_{prev_task_instance_or_None.run_id}"
                    prev_task_instance_xcom_dict : dict = prev_task_instance_or_None.xcom_pull(key=prev_task_instance_xcom_key_str)
                    assert prev_task_instance_xcom_dict is not None
                    prev_task_instance_xcom_dto = OpenApiXcomDto.from_dict(prev_task_instance_xcom_dict)
                    cur_request_url = prev_task_instance_xcom_dto.next_request_url
                    cur_request_url_obj = UrlObjectFactory.createPublicDataPortalNationalDayUrl(cur_request_url)
                    
                open_api_helper = OpenApiHelper()
                response_json = open_api_helper.get_response(cur_request_url, dag_config_param['src_nm'], dag_config_param['tb_nm'])
                cur_request_url_datetime_obj : datetime = datetime.strptime(f"{cur_request_url_obj.solYear}-{cur_request_url_obj.solMonth}-01","%Y-%m-%d") + relativedelta(months=1)
                cur_request_url_obj.solYear = cur_request_url_datetime_obj.strftime('%Y')
                cur_request_url_obj.solMonth = cur_request_url_datetime_obj.strftime('%m')
                next_request_url = cur_request_url_obj.get_full_url()
                cur_task_isntance_xcom_dto = OpenApiXcomDto(next_request_url = next_request_url, response_json = response_json)
                cur_task_instance_xcom_dict = cur_task_isntance_xcom_dto.to_dict()
                cur_task_instance.xcom_push(f"{dag_id}_open_api_request_{cur_task_instance.run_id}",value = cur_task_instance_xcom_dict)
            @task
            def open_api_csv_save():
                cur_context : Context = get_current_context()
                cur_task_instance = cur_context['task_instance']
                cur_dag_run : DagRun = cur_context['dag_run']
                cur_dag_run_open_api_reqeust_task_instance : TaskInstance = cur_dag_run.get_task_instance('open_api_request')
                cur_dag_run_open_api_request_xcom_key_str : str = f"{dag_id}_{cur_dag_run_open_api_reqeust_task_instance.task_id}_{cur_dag_run_open_api_reqeust_task_instance.run_id}"
                cur_dag_run_open_api_request_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_reqeust_task_instance.xcom_pull(key=cur_dag_run_open_api_request_xcom_key_str))
                assert cur_dag_run_open_api_request_xcom_dto is not None
                csv_file_path : str = f"{dag_config_param['dir_path']}".replace("TIMESTAMP", cur_dag_run.execution_date.strftime('%Y%m'))
                csv_file_path = csv_file_path[1:csv_file_path.__len__()]
                csv_manager = CsvManager()
                csv_manager.save_csv(json_data = cur_dag_run_open_api_request_xcom_dto.response_json, csv_path = csv_file_path)
                cur_dag_run_open_api_request_xcom_dto.csv_file_path = csv_file_path
                cur_dag_run_open_api_request_xcom_dict : dict = cur_dag_run_open_api_request_xcom_dto.to_dict()
                cur_task_instance.xcom_push(f"{dag_id}_open_api_csv_save_{cur_task_instance.run_id}", value = cur_dag_run_open_api_request_xcom_dict)
            @task
            def open_api_hdfs_save():
                cur_context : Context = get_current_context()
                cur_dag_run : DagRun = cur_context['dag_run']
                cur_dag_run_open_api_csv_save_task_instance : TaskInstance = cur_dag_run.get_task_instance('open_api_csv_save')
                cur_dag_run_open_api_csv_save_task_instance_xcom_key_str : str = f"{dag_id}_{cur_dag_run_open_api_csv_save_task_instance.task_id}_{cur_dag_run_open_api_csv_save_task_instance.run_id}"
                cur_dag_run_open_api_csv_save_xcom_dict : dict = cur_dag_run_open_api_csv_save_task_instance.xcom_pull(key=cur_dag_run_open_api_csv_save_task_instance_xcom_key_str)
                cur_dag_run_open_api_csv_save_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_csv_save_xcom_dict)
                assert cur_dag_run_open_api_csv_save_xcom_dto is not None
                csv_file_path : str = cur_dag_run_open_api_csv_save_xcom_dto.csv_file_path
                hdfs_file_path : str = csv_file_path
                try:
                    hdfs_hook = WebHDFSHook(webhdfs_conn_id='local_hdfs')
                    hdfs_client : WebHDFSHook = hdfs_hook.get_conn()
                    #if hdfs_file_path is already existed, then overwrite it    
                    hdfs_client.upload(hdfs_file_path, csv_file_path, overwrite=True)
                except Exception as e:
                    logging.error(f"Error while uploading file to HDFS: {e}")                    
            open_api_request_task = open_api_request()
            open_api_csv_save_task = open_api_csv_save()
            open_api_hdfs_save_task = open_api_hdfs_save()
            open_api_request_task >> open_api_csv_save_task >> open_api_hdfs_save_task
        return publicdataportal_nationalday_dag()