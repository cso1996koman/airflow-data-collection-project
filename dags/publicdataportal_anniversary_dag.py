from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.models import TaskInstance
from airflow.models import DagRun
from airflow.decorators import dag, task
from airflow.utils.context import Context
from airflow import DAG
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import logging
from dateutil.relativedelta import relativedelta
from csv_manager import CsvManager
from publicdataportal_anniversary_url import PublicDataPortalAnniversaryUrl
from open_api_helper import OpenApiHelper
from open_api_xcom_dto import OpenApiXcomDto
from url_object_factory import UrlObjectFactory
class PublicDataPortalAnniversaryDag:
    @staticmethod
    def create_publicdataportal_anniversary_dag(dag_config_param : dict, dag_id : str, schedule_interval : timedelta, start_date : datetime, default_args : dict) -> DAG:
        @dag(dag_id=dag_id,
                    schedule_interval=schedule_interval,
                    params=dag_config_param,
                    start_date=start_date,
                    default_args=default_args)
        def publicdataportal_anniversary_dag() -> DAG:
            @task
            def open_api_request():
                cur_context : Context = get_current_context()
                cur_task_instance : TaskInstance = cur_context['task_instance']
                prev_task_instance_or_none : TaskInstance = cur_task_instance.get_previous_ti()
                cur_request_url_obj : PublicDataPortalAnniversaryUrl = None
                
                if prev_task_instance_or_none is None:
                    request_url : str = dag_config_param['uri']
                    cur_request_url_obj = UrlObjectFactory.createPublicDataPortalAnniversaryUrl(request_url)
                    assert request_url is not None                    
                    cur_request_url_obj.serviceKey = dag_config_param['api_keys']
                    cur_request_url_obj.solYear = datetime(2015,1,1).strftime('%Y')
                    cur_request_url_obj.solMonth = datetime(2015,1,1).strftime('%m')
                else:
                    prev_task_instance_xcom_dict : dict = prev_task_instance_or_none.xcom_pull(key=f"{dag_id}_{prev_task_instance_or_none.task_id}_{prev_task_instance_or_none.run_id}")
                    assert prev_task_instance_xcom_dict is not None
                    prev_task_instance_xcom_dto = OpenApiXcomDto.from_dict(prev_task_instance_xcom_dict)
                    cur_request_url_obj = UrlObjectFactory.createPublicDataPortalAnniversaryUrl(prev_task_instance_xcom_dto.next_request_url)
                    
                open_api_helper = OpenApiHelper()
                response_json = open_api_helper.get_response(cur_request_url_obj.get_full_url(), dag_config_param['src_nm'], dag_config_param['tb_nm'])
                next_request_url_datetime_obj : datetime = datetime.strptime(f"{cur_request_url_obj.solYear}-{cur_request_url_obj.solMonth}-01","%Y-%m-%d") + relativedelta(months=1)
                cur_request_url_obj.solYear = next_request_url_datetime_obj.strftime('%Y')
                cur_request_url_obj.solMonth = next_request_url_datetime_obj.strftime('%m')                
                next_request_url = cur_request_url_obj.get_full_url()
                cur_task_instance_xcom_dto = OpenApiXcomDto(next_request_url = next_request_url, response_json = response_json)
                cur_task_instance_xcom_dict = cur_task_instance_xcom_dto.to_dict()
                cur_task_instance.xcom_push(f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}",value = cur_task_instance_xcom_dict)
            @task
            def open_api_csv_save():
                cur_context : Context = get_current_context()
                cur_task_instance : TaskInstance = cur_context['task_instance']
                cur_dag_run : DagRun = cur_context['dag_run']
                cur_dag_run_open_api_request_task_instance : TaskInstance = cur_dag_run.get_task_instance('open_api_request')
                cur_dag_run_open_api_request_xcom_key_str : str = f"{dag_id}_{cur_dag_run_open_api_request_task_instance.task_id}_{cur_dag_run_open_api_request_task_instance.run_id}"
                cur_dag_run_open_api_request_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_request_task_instance.xcom_pull(key=cur_dag_run_open_api_request_xcom_key_str))
                assert cur_dag_run_open_api_request_task_instance_xcom_dto is not None
                csv_file_path : str = f"{dag_config_param['dir_path']}".replace("TIMESTAMP", cur_dag_run.execution_date.strftime('%Y%m'))
                csv_file_path = csv_file_path[1:csv_file_path.__len__()]
                csv_manager = CsvManager()
                csv_manager.save_csv(json_data = cur_dag_run_open_api_request_task_instance_xcom_dto.response_json, csv_path = csv_file_path)
                cur_dag_run_open_api_request_task_instance_xcom_dto.csv_file_path = csv_file_path
                cur_dag_run_open_api_request_task_instance_xcom_dict : dict = cur_dag_run_open_api_request_task_instance_xcom_dto.to_dict()
                cur_dag_run_open_api_csv_save_task_instance_xcom_dict = cur_dag_run_open_api_request_task_instance_xcom_dict
                cur_task_instance.xcom_push(f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value = cur_dag_run_open_api_csv_save_task_instance_xcom_dict)
            @task
            def open_api_hdfs_save():
                cur_context : Context = get_current_context()
                cur_dag_run : DagRun = cur_context['dag_run']
                cur_dag_run_open_api_csv_save_task_instance : TaskInstance = cur_dag_run.get_task_instance('open_api_csv_save')
                cur_dag_run_open_api_csv_save_xcom_key_str : str = f"{dag_id}_{cur_dag_run_open_api_csv_save_task_instance.task_id}_{cur_dag_run_open_api_csv_save_task_instance.run_id}"
                cur_dag_run_open_api_csv_save_xcom_dict = cur_dag_run_open_api_csv_save_task_instance.xcom_pull(key=cur_dag_run_open_api_csv_save_xcom_key_str)
                cur_dag_run_open_api_csv_save_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_csv_save_xcom_dict)
                assert cur_dag_run_open_api_csv_save_xcom_dto is not None
                csv_file_path : str = cur_dag_run_open_api_csv_save_xcom_dto.csv_file_path
                hdfs_file_path : str = csv_file_path
                try:
                    hdfs_hook = WebHDFSHook(webhdfs_conn_id='local_hdfs')
                    hdfs_client = hdfs_hook.get_conn()
                    hdfs_client.upload(hdfs_file_path, csv_file_path)
                except Exception as e:
                    logging.error(f"Error: {e}")
            open_api_request_task = open_api_request()
            open_api_csv_save_task = open_api_csv_save()
            open_api_hdfs_save_task = open_api_hdfs_save()
            open_api_request_task >> open_api_csv_save_task >> open_api_hdfs_save_task
        return publicdataportal_anniversary_dag()