from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow import DAG
from csv_manager import CsvManager
from weatheradministration_url import WeatherAdministrationUrl
from open_api_helper import OpenApiHelper
from url_object_factory import UrlObjectFactory
from airflow.operators.python import get_current_context
from airflow.models import TaskInstance
from airflow.models import DagRun
from open_api_xcom_dvo import OpenApiXcomDvo
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
                context = get_current_context()
                cur_task_instance : TaskInstance = context['task_instance']
                prev_task_instance : TaskInstance = cur_task_instance.get_previous_ti()
                request_url = None
                weatheradministration_url_obj : WeatherAdministrationUrl = None
                if prev_task_instance is None:
                    request_url = dag_config_param['uri']
                    assert request_url is not None
                    weatheradministration_url_obj = UrlObjectFactory.createWeatherAdministrationUrl(request_url)
                    weatheradministration_url_obj.serviceKey = dag_config_param['api_keys']
                    weatheradministration_url_obj.startDt = datetime(2015, 1, 1).strftime('%Y%m%d')
                    endDt_datetime_obj : datetime = datetime.strptime(weatheradministration_url_obj.startDt,"%Y%m%d") + timedelta(days=365)
                    weatheradministration_url_obj.endDt = endDt_datetime_obj.strftime('%Y%m%d')
                    assert datetime.strptime(weatheradministration_url_obj.endDt, "%Y%m%d") < datetime.now() - timedelta(days=1), "endDt should be less than yesterday"
                    request_url = weatheradministration_url_obj.getFullUrl()
                else:
                    prev_task_instance_xcom_dict : dict = prev_task_instance.xcom_pull(key=f"{dag_id}_open_api_request_{prev_task_instance.run_id}")
                    assert prev_task_instance_xcom_dict is not None
                    prev_task_instance_xcom_dto = OpenApiXcomDvo.from_dict(prev_task_instance_xcom_dict)  
                    request_url = prev_task_instance_xcom_dto.next_request_url
                    weatheradministration_url_obj = UrlObjectFactory.createWeatherAdministrationUrl(request_url)
                open_api_helper = OpenApiHelper()
                response_json = open_api_helper.get_response(request_url, dag_config_param['src_nm'], dag_config_param['tb_nm'])
                if((datetime.strptime(weatheradministration_url_obj.endDt, "%Y%m%d") + timedelta(days=365)) < (datetime.now() - timedelta(days=1))):
                    startDt_datetime_obj : datetime = datetime.strptime(weatheradministration_url_obj.endDt, "%Y%m%d") + timedelta(days=1)
                    weatheradministration_url_obj.startDt = startDt_datetime_obj.strftime('%Y%m%d')
                    endDt_datetime_obj : datetime = startDt_datetime_obj + timedelta(days=365)
                    weatheradministration_url_obj.endDt = endDt_datetime_obj.strftime('%Y%m%d')
                else:
                    reamining_days_datetime_obj : datetime = (datetime.now() - timedelta(days=1)) - datetime.strptime(weatheradministration_url_obj.endDt, "%Y%m%d")
                    startDt_datetime_obj : datetime = datetime.strptime(weatheradministration_url_obj.endDt, "%Y%m%d") + timedelta(days=1)
                    weatheradministration_url_obj.startDt = startDt_datetime_obj.strftime('%Y%m%d')
                    endDt_datetime_obj : datetime = startDt_datetime_obj + reamining_days_datetime_obj
                    weatheradministration_url_obj.endDt = endDt_datetime_obj.strftime('%Y%m%d')
                next_request_url = weatheradministration_url_obj.getFullUrl()
                open_api_xcom_dto = OpenApiXcomDvo(next_request_url = next_request_url, response_json = response_json)
                cur_task_instance.xcom_push(key=f"{dag_id}_open_api_request_{cur_task_instance.run_id}", value=open_api_xcom_dto.to_dict())
            @task
            def open_api_csv_save():
                context = get_current_context()
                cur_task_instance : TaskInstance = context['task_instance']
                cur_dag_run : DagRun = cur_task_instance.dag_run
                open_api_request_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id = 'open_api_request')
                open_api_request_task_instance_xcom_dict : dict = open_api_request_task_instance.xcom_pull(key=f"{dag_id}_open_api_request_{open_api_request_task_instance.run_id}")
                assert open_api_request_task_instance_xcom_dict is not None
                open_api_request_task_instance_xcom_dto : OpenApiXcomDvo = OpenApiXcomDvo.from_dict(open_api_request_task_instance_xcom_dict)
                response_json = open_api_request_task_instance_xcom_dto.response_json
                next_request_url_obj = UrlObjectFactory.createWeatherAdministrationUrl(open_api_request_task_instance_xcom_dto.next_request_url)
                csv_file_path : str = dag_config_param['dir_path']
                csv_file_path = csv_file_path[1:csv_file_path.__len__()]
                csv_file_path = csv_file_path.replace('TIMESTAMP', next_request_url_obj.startDt)
                csv_manager = CsvManager()
                csv_manager.save_csv(response_json, csv_file_path)
                open_api_request_task_instance_xcom_dto.csv_file_path = csv_file_path
                cur_task_instance.xcom_push(key=f"{dag_id}_open_api_csv_save_{cur_task_instance.run_id}", value=open_api_request_task_instance_xcom_dto.to_dict())
            @task
            def open_api_hdfs_save():
                context = get_current_context()
                cur_dag_run = context['dag_run']
                open_api_csv_save_task_instance = cur_dag_run.get_task_instance(task_id = 'open_api_csv_save')
                open_api_csv_save_task_instance_xcom_dict : dict = open_api_csv_save_task_instance.xcom_pull(key=f"{dag_id}_open_api_csv_save_{cur_dag_run.run_id}")
                open_api_csv_save_task_instance_xcom_dto : OpenApiXcomDvo = OpenApiXcomDvo.from_dict(open_api_csv_save_task_instance_xcom_dict)                
                csv_file_path = open_api_csv_save_task_instance_xcom_dto.csv_file_path
                hdfs_file_path = csv_file_path
                webhdfs_hook = WebHDFSHook(webhdfs_conn_id='local_hdfs')
                webhdfs_hook.load_file(csv_file_path, hdfs_file_path)                
            open_api_request_task = open_api_request()
            open_api_csv_save_task = open_api_csv_save()
            open_api_hdfs_save_task = open_api_hdfs_save()
            open_api_request_task>>open_api_csv_save_task>>open_api_hdfs_save_task
        return weatherstatics_open_api_dag()