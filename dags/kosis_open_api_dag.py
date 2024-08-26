from airflow.models import TaskInstance
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.operators.python import get_current_context
from airflow.decorators import dag, task
from airflow.models.dagrun import DagRun
from airflow import DAG
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
from typing import List
import pytz
from url_object_factory import UrlObjectFactory
from csv_manager import CsvManager
from kosis_url import OBJTYPE, KosisUrl, PRDSEENUM
from open_api_xcom_dto import OpenApiXcomDto
from open_api_helper import OpenApiHelper
class KosisOpenApiDag:
    @staticmethod
    def create_kosis_open_api_dag(dag_config_param : dict, dag_id : str, schedule_interval : timedelta, start_date : datetime, default_args : dict) -> DAG:
        @dag(dag_id=dag_id,
                    schedule_interval=schedule_interval,
                    params=dag_config_param,
                    start_date=start_date,
                    default_args=default_args)
        def kosis_open_api_dag() -> DAG:
            @task
            def open_api_request():
                context = get_current_context()
                prev_or_first_task_instance : TaskInstance = None
                cur_task_instance : TaskInstance = None
                prev_or_first_task_instance_xcom_request_url : str = None
                cur_request_url : str = None

                #현재 태스크 인스턴스가 첫번째 태스크 인스턴스인 경우
                if context['task_instance'].get_previous_ti() is None:
                    prev_or_first_task_instance = context['task_instance']
                    cur_task_instance = context['task_instance']
                    prev_or_first_task_instance_xcom_request_url = dag_config_param['uri']
                    cur_request_url_obj : KosisUrl = UrlObjectFactory.createKosisUrl(prev_or_first_task_instance_xcom_request_url)
                    if cur_request_url_obj.prdSe == PRDSEENUM.YEAR.value:
                        cur_request_url_obj.startPrdDe = start_date.strftime('%Y')
                        cur_request_url_obj.endPrdDe = cur_request_url_obj.startPrdDe
                    elif cur_request_url_obj.prdSe == PRDSEENUM.MONTH.value:
                        cur_request_url_obj.startPrdDe = start_date.strftime('%Y%m')
                        cur_request_url_obj.endPrdDe = cur_request_url_obj.startPrdDe
                    elif cur_request_url_obj.prdSe == PRDSEENUM.QUARTER.value:
                        cur_request_url_obj.startPrdDe = start_date.strftime('%Y%m')
                        cur_request_url_obj.endPrdDe = cur_request_url_obj.startPrdDe
                    else:
                        assert False, "prdSe is not valid"
                    cur_request_url = cur_request_url_obj.get_full_url()
                #현재 태스크 인스턴스가 첫번째 태스크 인스턴스가 아닌 경우
                else:
                    prev_or_first_task_instance = context['task_instance'].get_previous_ti()
                    cur_task_instance = context['task_instance']                    
                    assert prev_or_first_task_instance is not None, "prev_or_first_task_instance is None"
                    prev_or_first_task_instance_xcom_dic : OpenApiXcomDto = prev_or_first_task_instance.xcom_pull(key = f"{dag_id}_{prev_or_first_task_instance.task_id}_{prev_or_first_task_instance.run_id}")
                    assert prev_or_first_task_instance_xcom_dic is not None, "prev_or_first_task_instance_xcom_dto is None"
                    prev_or_first_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(prev_or_first_task_instance_xcom_dic)
                    cur_request_url = prev_or_first_task_instance_xcom_dto.next_request_url
                
                # 현재 task에서 url에 대한 요청 수행 
                # url은 다중파라미터 형태 혹은 단일 파라미터 형태로 요청
                cur_request_url_obj : KosisUrl = UrlObjectFactory.createKosisUrl(cur_request_url)
                cur_request_url_obj.apiKey = dag_config_param['api_keys']
                open_api_helper_obj = OpenApiHelper()
                # 다중 파라미터 형태 요청
                if(not cur_request_url_obj.objL1.__contains__("ALL") and cur_request_url_obj.objL1 != ""):
                    # objL1 : 1+2+3+4+5+...n+
                    # objL1 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(cur_request_url_obj.objL1)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(cur_request_url_obj, obj_unit_params, OBJTYPE.OBJL1.value)
                elif(not cur_request_url_obj.objL2.__contains__("ALL") and cur_request_url_obj.objL2 != ""):
                    # objL2 = 1+2+3+4+5+...n+
                    # objL2 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(cur_request_url_obj.objL2)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(cur_request_url_obj, obj_unit_params, OBJTYPE.OBJL2.value)
                elif(not cur_request_url_obj.objL3.__contains__("ALL") and cur_request_url_obj.objL3 != ""):
                    # objL3 = 1+2+3+4+5+...n+
                    # objL3 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(cur_request_url_obj.objL3)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(cur_request_url_obj, obj_unit_params, OBJTYPE.OBJL3.value)
                elif(not cur_request_url_obj.objL4.__contains__("ALL") and cur_request_url_obj.objL4 != ""):
                    # objL4 = 1+2+3+4+5+...n+
                    # objL4 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(cur_request_url_obj.objL4)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(cur_request_url_obj, obj_unit_params, OBJTYPE.OBJL4.value)
                elif(not cur_request_url_obj.objL5.__contains__("ALL") and cur_request_url_obj.objL5 != ""):
                    # objL5 = 1+2+3+4+5+...n+
                    # objL5 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(cur_request_url_obj.objL5)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(cur_request_url_obj, obj_unit_params, OBJTYPE.OBJL5.value)
                elif(not cur_request_url_obj.objL6.__contains__("ALL") and cur_request_url_obj.objL6 != ""):
                    # objL6 = 1+2+3+4+5+...n+
                    # objL6 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(cur_request_url_obj.objL6)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(cur_request_url_obj, obj_unit_params, OBJTYPE.OBJL6.value)
                elif(not cur_request_url_obj.objL7.__contains__("ALL") and cur_request_url_obj.objL7 != ""):
                    # objL7 = 1+2+3+4+5+...n+
                    # objL7 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(cur_request_url_obj.objL7)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(cur_request_url_obj, obj_unit_params, OBJTYPE.OBJL7.value)
                elif(not cur_request_url_obj.objL8.__contains__("ALL") and cur_request_url_obj.objL8 != ""):
                    # objL8 = 1+2+3+4+5+...n+
                    # objL8 unit param : 1+, 2+, 3+, 4+, 5+ ,..., n+
                    obj_unit_params : List[str] = open_api_helper_obj.get_multi_unit_param(cur_request_url_obj.objL8)
                    response : dict = open_api_helper_obj.get_appeneded_response_bymulti_unit_param(cur_request_url_obj, obj_unit_params, OBJTYPE.OBJL8.value)
                # 단일 파라미터 형태 요청
                else:
                    response = open_api_helper_obj.get_response(cur_request_url_obj.get_full_url(), dag_config_param['src_nm'], dag_config_param['tb_nm'])
                
                # 현재 task에서 response를 xcom에 저장 및 다음 task 요청 url 생성 및 전달
                cur_task_instance_xcom_dto = OpenApiXcomDto(response_json=response)
                prdSe = cur_request_url_obj.prdSe
                if prdSe == PRDSEENUM.YEAR.value:
                    start_prd_de = datetime.strptime(cur_request_url_obj.startPrdDe, '%Y').replace(tzinfo=pytz.UTC)
                    end_prd_de = datetime.strptime(cur_request_url_obj.endPrdDe, '%Y').replace(tzinfo=pytz.UTC)
                    start_prd_de += relativedelta(years=1)
                    end_prd_de += relativedelta(years=1)
                    cur_request_url_obj.startPrdDe = start_prd_de.strftime('%Y')
                    cur_request_url_obj.endPrdDe = end_prd_de.strftime('%Y')                
                elif prdSe == PRDSEENUM.MONTH.value:
                    start_prd_de = datetime.strptime(cur_request_url_obj.startPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    end_prd_de = datetime.strptime(cur_request_url_obj.endPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    start_prd_de += relativedelta(months=1)
                    end_prd_de += relativedelta(months=1)
                    cur_request_url_obj.startPrdDe = start_prd_de.strftime('%Y%m')
                    cur_request_url_obj.endPrdDe = end_prd_de.strftime('%Y%m')                
                elif prdSe == PRDSEENUM.QUARTER.value:
                    cur_request_url_start_datetime_obj = datetime.strptime(cur_request_url_obj.startPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    cur_request_url_end_datetime_obj = datetime.strptime(cur_request_url_obj.endPrdDe, '%Y%m').replace(tzinfo=pytz.UTC)
                    cur_request_url_start_datetime_obj += relativedelta(months=1)
                    cur_request_url_end_datetime_obj += relativedelta(months=1)
                    cur_request_url_end_month_str = cur_request_url_end_datetime_obj.strftime('%m')
                    cur_request_url_start_month_str = cur_request_url_start_datetime_obj.strftime('%m')
                    
                    if cur_request_url_start_month_str == '05':
                        cur_request_url_start_datetime_obj += relativedelta(years=1)
                        cur_request_url_start_datetime_obj = cur_request_url_start_datetime_obj.replace(month=1)
                        cur_request_url_start_month_str = cur_request_url_start_datetime_obj.strftime('%m')
                    if cur_request_url_end_month_str == '05':
                        cur_request_url_end_datetime_obj += relativedelta(years=1)
                        cur_request_url_end_datetime_obj = cur_request_url_end_datetime_obj.replace(month=1)
                        cur_request_url_end_month_str = cur_request_url_end_datetime_obj.strftime('%m')

                    cur_request_url_obj.startPrdDe = cur_request_url_start_datetime_obj.strftime('%Y%m')
                    cur_request_url_obj.endPrdDe = cur_request_url_end_datetime_obj.strftime('%Y%m')
                    
                else:
                    assert False, "prdSe is not valid"
                cur_task_instance_xcom_dto.next_request_url = cur_request_url_obj.get_full_url()
                cur_task_instance.xcom_push(key=f"{dag_id}_{cur_task_instance.task_id}_{cur_task_instance.run_id}", value=cur_task_instance_xcom_dto.to_dict())
            # DagRun은 현재 실행중인 DAG에 대한 실행 단위, Task는 DAG내에 노드, TaskInstance는 Task에 대한 실행 단위
            @task
            def openapi_csv_save():
                context = get_current_context()
                cur_dag_run : DagRun = context['dag_run']
                cur_dag_run_open_api_request_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='open_api_request')
                assert cur_dag_run_open_api_request_task_instance is not None, "open_api_request_task_instance is None"
                cur_dag_run_open_api_request_task_instance_xcom_dict : dict = cur_dag_run_open_api_request_task_instance.xcom_pull(key=f"{dag_id}_open_api_request_{cur_dag_run_open_api_request_task_instance.run_id}")
                assert cur_dag_run_open_api_request_task_instance_xcom_dict is not None
                cur_dag_run_openapi_csv_save_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_request_task_instance_xcom_dict)
                csv_manager : CsvManager = CsvManager()
                assert dag_config_param['dir_path'] is not None, "dir_path is None"
                csv_dir_path : str = dag_config_param['dir_path']
                csv_dir_path = csv_dir_path[1:csv_dir_path.__len__()]
                csv_dir_path = csv_dir_path.replace("TIMESTAMP", cur_dag_run.execution_date.strftime('%Y%m%d'))
                csv_manager.save_csv(json_data = cur_dag_run_openapi_csv_save_task_instance_xcom_dto.response_json, csv_path = csv_dir_path)
                cur_dag_run_openapi_csv_save_task_instance_xcom_dto.csv_file_path = csv_dir_path
                cur_task_instance : TaskInstance = context['task_instance']
                cur_task_instance.xcom_push(key=f"{dag_id}_open_api_csv_save_{context['run_id']}", value=cur_dag_run_openapi_csv_save_task_instance_xcom_dto.to_dict())
            @task
            def openapi_upload_to_hdfs():
                context = get_current_context()
                cur_dag_run : DagRun = context['dag_run']
                cur_dag_run_open_api_csv_save_task_instance : TaskInstance = cur_dag_run.get_task_instance(task_id='openapi_csv_save')
                assert cur_dag_run_open_api_csv_save_task_instance is not None, "open_api_csv_save_task_instance is None"
                cur_dag_run_open_api_csv_save_task_instance_xcom_dict : dict = cur_dag_run_open_api_csv_save_task_instance.xcom_pull(key=f"{dag_id}_open_api_csv_save_{cur_dag_run_open_api_csv_save_task_instance.run_id}")
                assert cur_dag_run_open_api_csv_save_task_instance_xcom_dict is not None
                cur_dag_run_openapi_csv_save_task_instance_xcom_dto : OpenApiXcomDto = OpenApiXcomDto.from_dict(cur_dag_run_open_api_csv_save_task_instance_xcom_dict)
                csv_file_path = cur_dag_run_openapi_csv_save_task_instance_xcom_dto.csv_file_path
                try:
                    hdfs_hook = WebHDFSHook(webhdfs_conn_id='local_hdfs')
                    hdfs_client : WebHDFSHook = hdfs_hook.get_conn()
                    hdfs_file_path = csv_file_path
                    hdfs_client.upload(hdfs_file_path, csv_file_path, overwrite=True)
                    logging.info("File uploaded to HDFS successfully")
                    # os.remove(file_path)
                except Exception as e:
                    logging.error(f"Failed to upload file to HDFS: {e}")
                    raise
            open_api_request_task = open_api_request()
            openapi_csv_save_task = openapi_csv_save()
            openapi_upload_to_hdfs_task = openapi_upload_to_hdfs()                    
            open_api_request_task >> openapi_csv_save_task >> openapi_upload_to_hdfs_task
        return kosis_open_api_dag()