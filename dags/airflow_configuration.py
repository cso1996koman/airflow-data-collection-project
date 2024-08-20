from datetime import datetime, timedelta
import logging
from typing import List
from api_admin_dao import ApiAdminDao
from api_admin_dvo import ApiAdminDvo
from dag_factory import DagFactory
from airflow import DAG
from airflow.models import Variable
from airflow.executors.sequential_executor import SequentialExecutor
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 1, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=5), 
    'depends_on_past': True,
    'max_active_runs': 120
}
api_admin_dao : ApiAdminDao = ApiAdminDao('load_admin_db_mariadb')
api_admin_dvos_bykosis : List[ApiAdminDvo] = api_admin_dao.selectBySrcNm('KOSIS')
api_admin_dvos_bypublicdataportal : List[ApiAdminDvo] = api_admin_dao.selectBySrcNm('공공데이터포털')
api_admin_dvos_bypandas : List[ApiAdminDvo] = api_admin_dao.selectBySrcNm('FRED')
api_admin_dvos_byyfinance : List[ApiAdminDvo] = api_admin_dao.selectBySrcNm('Yfinance')                                                                            
Variable.set("kosis_api_key", "OTYwYjBlMGMyZmM2MmRlZDk0MjdjYWFhZWZmYTMwM2E=")
Variable.set("fred_api_key", "gAsNsUhyrbEmGPOt/eP8GO1Bf5ALh/akqttu0dJIpnR/q1LS2o+Ym0v8SDoMMTvAxNR8G1wNmB/xEWlf9CrSyg==")
api_admin_dvos = api_admin_dvos_bykosis + api_admin_dvos_bypublicdataportal + api_admin_dvos_bypandas + api_admin_dvos_byyfinance
dags : List[DAG] = DagFactory.dag_factory(default_args, api_admin_dvos)

for dag in dags:
    logging.info(f"Type of dag: {type(dag)}")   
for dag in dags:
    if isinstance(dag, DAG):
        globals()[dag.dag_id] = dag
    else :
        assert False, "dag is not instance of DAG"