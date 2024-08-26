from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.models import TaskInstance
from airflow.models import DagRun
from airflow.utils.context import Context
from airflow.providers.apache.hdfs.hooks.webhdfs import InsecureClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from csv_manager import CsvManager
from weatheradministration_url import WeatherAdministrationUrl
from open_api_helper import OpenApiHelper
from url_object_factory import UrlObjectFactory
from open_api_xcom_dto import OpenApiXcomDto
class PublicDataPortalWeatherStaticsDag:
    @staticmethod
    def create_publicdataportal_weatherstatics_dag(dag_config_param : dict, dag_id : str, schedule_interval : timedelta, start_date : datetime, default_args : dict) -> DAG:
        @dag(dag_id=dag_id,
                    schedule_interval=schedule_interval,
                    params=dag_config_param,
                    start_date=start_date,
                    default_args=default_args)
        def weatherstatics_open_api_dag() -> DAG:
            @task
            def open_api_request():
                cur_context : Context = get_current_context()
                cur_task_instance : TaskInstance = cur_context['task_instance']
                prev_task_instance : TaskInstance = cur_task_instance.get_previous_ti()
                cur_request_url = None
                cur_reqeust_url_obj : WeatherAdministrationUrl = None
                if prev_task_instance is None:
                    cur_request_url = dag_config_param['uri']
                    assert cur_request_url is not None
                    cur_reqeust_url_obj = UrlObjectFactory.createWeatherAdministrationUrl(cur_request_url)
                    cur_reqeust_url_obj.serviceKey = dag_config_param['api_keys']
                    assert cur_reqeust_url_obj.serviceKey is not None
                    cur_reqeust_url_obj.startDt = datetime(2015, 1, 1).strftime('%Y%m%d')
                    cur_request_url_start_datetime_obj : datetime = datetime.strptime(cur_reqeust_url_obj.startDt,"%Y%m%d")
                    end_datetime_obj : datetime = cur_request_url_start_datetime_obj + relativedelta(years=1) + relativedelta(days=-1)
                    cur_reqeust_url_obj.endDt = end_datetime_obj.strftime('%Y%m%d')
                    cur_request_url_end_datetime_obj = datetime.strptime(cur_reqeust_url_obj.endDt, "%Y%m%d")
                    assert cur_request_url_end_datetime_obj < datetime.now() - relativedelta(days=1), "endDt should be less than yesterday"
                    cur_request_url = cur_reqeust_url_obj.getFullUrl()
                else:
                    prev_task_instance_xcom_dto_key_str : str = f"{dag_id}_{prev_task_instance.task_id}_{prev_task_instance.run_id}"
                    prev_task_instance_xcom_dict : dict = prev_task_instance.xcom_pull(key = prev_task_instance_xcom_dto_key_str)
                    assert prev_task_instance_xcom_dict is not None
                    prev_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(prev_task_instance_xcom_dict)
                    cur_request_url = prev_task_instance_xcom_dto.next_request_url
                cur_reqeust_url_obj = UrlObjectFactory.createWeatherAdministrationUrl(cur_request_url)
                open_api_helper = OpenApiHelper()
                assert dag_config_param['src_nm'] is not None
                assert dag_config_param['tb_nm'] is not None
                response_json = open_api_helper.get_response(cur_request_url, dag_config_param['src_nm'], dag_config_param['tb_nm'])
                cur_request_url_end_datetime_obj = datetime.strptime(cur_reqeust_url_obj.endDt, "%Y%m%d")
                if(cur_request_url_end_datetime_obj + relativedelta(years=1)) <= (datetime.now() - relativedelta(days=1)):
                    next_request_url_start_datetime_obj : datetime = datetime.strptime(cur_reqeust_url_obj.endDt, "%Y%m%d") + relativedelta(days=1)
                    cur_reqeust_url_obj.startDt = next_request_url_start_datetime_obj.strftime('%Y%m%d')
                    next_request_url_end_datetime_obj : datetime = cur_request_url_end_datetime_obj + relativedelta(years=1)
                    cur_reqeust_url_obj.endDt = next_request_url_end_datetime_obj.strftime('%Y%m%d')
                else:
                    cur_request_url_end_datetime_obj : datetime = datetime.strptime(cur_reqeust_url_obj.endDt, "%Y%m%d")
                    reamining_days_datetime_obj : datetime = (datetime.now() - relativedelta(days=1)) - cur_request_url_end_datetime_obj
                    next_start_datetime_obj : datetime = datetime.strptime(cur_reqeust_url_obj.endDt, "%Y%m%d") + relativedelta(days=1)
                    cur_reqeust_url_obj.startDt = next_start_datetime_obj.strftime('%Y%m%d')
                    next_end_datetime_obj : datetime = next_start_datetime_obj + reamining_days_datetime_obj
                    cur_reqeust_url_obj.endDt = next_end_datetime_obj.strftime('%Y%m%d')
                next_cur_request_url = cur_reqeust_url_obj.getFullUrl()
                cur_task_instance_xcom_dto = OpenApiXcomDto(next_request_url = next_cur_request_url, response_json = response_json)
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=cur_task_instance_xcom_dto.to_dict())
            @task
            def open_api_csv_save():
                cur_context : Context = get_current_context()
                cur_task_instance : TaskInstance = cur_context['task_instance']
                cur_dag_run : DagRun = cur_task_instance.dag_run
                cur_dag_run_open_api_request_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id = 'open_api_request')
                assert cur_dag_run_open_api_request_task_instance is not None
                cur_dag_run_oepn_api_request_xcom_key_str : str = f"{dag_id}_{cur_dag_run_open_api_request_task_instance.task_id}_{cur_dag_run.run_id}"
                open_api_request_task_instance_xcom_dict : dict = cur_dag_run_open_api_request_task_instance.xcom_pull(key=cur_dag_run_oepn_api_request_xcom_key_str)
                assert open_api_request_task_instance_xcom_dict is not None
                cur_dag_run_open_api_request_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(open_api_request_task_instance_xcom_dict)
                response_json = cur_dag_run_open_api_request_task_instance_xcom_dto.response_json
                csv_file_path : str = dag_config_param['dir_path']
                csv_file_path = csv_file_path[1:csv_file_path.__len__()]
                csv_file_path = csv_file_path.replace('TIMESTAMP', cur_dag_run.execution_date.strftime('%Y%m%d'))
                csv_manager = CsvManager()
                csv_manager.save_csv(response_json, csv_file_path)
                cur_dag_run_open_api_csv_save_xcom_dto : OpenApiXcomDto = cur_dag_run_open_api_request_task_instance_xcom_dto
                cur_dag_run_open_api_csv_save_xcom_dto.csv_file_path = csv_file_path
                cur_dag_run_open_api_csv_save_task_instance_xcom_key_str : str = f"{dag_id}_{cur_task_instance.task_id}_{cur_dag_run.run_id}"
                cur_task_instance.xcom_push(key=cur_dag_run_open_api_csv_save_task_instance_xcom_key_str, value=cur_dag_run_open_api_csv_save_xcom_dto.to_dict())
            @task
            def open_api_hdfs_save():
                cur_context : Context = get_current_context()
                cur_dag_run = cur_context['dag_run']
                assert cur_dag_run is not None
                cur_dag_run_open_api_csv_save_task_instance = cur_dag_run.get_task_instance(task_id = 'open_api_csv_save')
                assert cur_dag_run_open_api_csv_save_task_instance is not None
                cur_dag_run_open_api_csv_save_xcom_key_str = f"{dag_id}_{cur_dag_run_open_api_csv_save_task_instance.task_id}_{cur_dag_run.run_id}"
                cur_dag_run_open_api_csv_save_xcom_dict : dict = cur_dag_run_open_api_csv_save_task_instance.xcom_pull(key=cur_dag_run_open_api_csv_save_xcom_key_str)
                cur_dag_run_open_api_csv_save_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_csv_save_xcom_dict)
                csv_file_path = cur_dag_run_open_api_csv_save_xcom_dto.csv_file_path
                hdfs_file_path = csv_file_path
                webhdfs_hook = WebHDFSHook(webhdfs_conn_id='local_hdfs')
                hdfs_client : InsecureClient = webhdfs_hook.get_conn()
                # if csv_file_path is already existed, overwrite it
                hdfs_client.upload(hdfs_file_path, csv_file_path, overwrite=True)
            open_api_request_task = open_api_request()
            open_api_csv_save_task = open_api_csv_save()
            open_api_hdfs_save_task = open_api_hdfs_save()
            open_api_request_task>>open_api_csv_save_task>>open_api_hdfs_save_task
        return weatherstatics_open_api_dag()