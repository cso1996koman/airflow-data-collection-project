from airflow import DAG
from datetime import datetime
from api_admin_dao import ApiAdminDao
from logisticsinfocenter_dag import LogisticsInfoCenter
from publicdataportal_anniversary_dag import PublicDataPortalAnniversaryDag
from publicdataportal_holiday_dag import PublicDataPortalHolidayDag
from publicdataportal_nationalday_dag import PublicDataPortalNationalDayDag
from publicdataportal_solarterm_dag import PublicDataPortalSolarTermDag
from publicdataportal_table_name_enum import PublicDataPortalTableName
from publicdataportal_traditionalday_dag import PublicDataPortalTraditionalDayDag
from publicdataportal_weatherstatics_dag import PublicDataPortalWeatherStaticsDag
from kosis_open_api_dag import KosisOpenApiDag
from data_collection_source_name_enum import DATACOLLECTIONSOURCENAME
from kosis_url import PRDSEENUM, KosisUrl
from dag_param_dvo import DagParamDvo
from url_object_factory import UrlObjectFactory
from api_admin_dvo import ApiAdminDvo
from typing import List
from fred_table_name_enum import FredTableName
from fred_koreaninterestrate_dag import FredKoreanInterestRateDag
from fred_usinterestrate_dag import FredUsInterestRateDag
from fred_oilprice_dag import FredOilPriceDag
from yfinance_usdtokrwexchangerate_dag import YFinanceUsdToKrwExchangeRateDag
from dateutil.relativedelta import relativedelta
class DagFactory:
    @staticmethod
    def dag_factory(_default_args : dict, _api_admin_dvos : List[ApiAdminDvo], api_admin_dao : ApiAdminDao) -> List[DAG]:
        dag_list : List[DAG] = []
        for api_admin_dvo in _api_admin_dvos:
            dvo : ApiAdminDvo = api_admin_dvo
            if(dvo.src_nm == DATACOLLECTIONSOURCENAME.KOSIS.value):
                schedule_interval : relativedelta = None
                start_date : datetime = None                
                dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                             tb_code = dvo.tb_code,
                             tb_nm = dvo.tb_nm,
                             eng_tb_nm = dvo.eng_tb_nm,
                             uri = dvo.uri,
                             dir_path = dvo.dir_path,
                             api_keys = ["OTYwYjBlMGMyZmM2MmRlZDk0MjdjYWFhZWZmYTMwM2E="])
                kosis_url_obj : KosisUrl = UrlObjectFactory.createKosisUrl(dag_param_dvo.uri)
                if kosis_url_obj.prdSe == PRDSEENUM.YEAR.value:
                    schedule_interval = relativedelta(years=1)
                    assert(len(kosis_url_obj.startPrdDe) == 4), "InvalidPrdSe"
                    start_date_str : str = f"{kosis_url_obj.startPrdDe}-01-01"
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")                    
                elif kosis_url_obj.prdSe == PRDSEENUM.MONTH.value:
                    schedule_interval = relativedelta(months=1)
                    assert(len(kosis_url_obj.startPrdDe) == 6), "InvalidPrdSe"
                    start_date_str : str = f"{kosis_url_obj.startPrdDe[0:4]}-{kosis_url_obj.startPrdDe[4:6]}-01"
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                elif kosis_url_obj.prdSe == PRDSEENUM.QUARTER.value:
                    schedule_interval = relativedelta(months=3)
                    # 1분기 kosis_url_obj.startPrdDe : 20XX-01, 2분기 kosis_url_obj.startPrdDe : 20XX-02, 3분기 kosis_url_obj.startPrdDe : 20XX-03, 4분기 kosis_url_obj.startPrdDe : 20XX-04
                    assert(len(kosis_url_obj.startPrdDe) == 6), "InvalidPrdSe"
                    month : int = int(kosis_url_obj.startPrdDe[5:6])
                    if(month > 1):
                        month = (month - 1) * 3 + 1
                    start_date_str : str = f"{kosis_url_obj.startPrdDe[0:4]}-{month:02d}-01"
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                else:
                    assert False, "Invalid prdSe"
                kosis_url_obj.endPrdDe = kosis_url_obj.startPrdDe
                dag_param_dvo.uri = kosis_url_obj.get_full_url()
                kosis_open_api_dag = KosisOpenApiDag.create_kosis_open_api_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                               dag_id=dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dvo.tb_code}_{dag_param_dvo.src_nm}_{dag_param_dvo.eng_tb_nm}'),
                                                                               schedule_interval=schedule_interval,
                                                                               start_date=start_date,
                                                                               default_args=_default_args)
                dag_list.append(kosis_open_api_dag)
            elif(dvo.src_nm == DATACOLLECTIONSOURCENAME.PUBLICDATAPORTAL.value):
                if(dvo.tb_nm == PublicDataPortalTableName.WEATHERSTATICS.value):
                    dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                             tb_code = dvo.tb_code,
                             tb_nm = dvo.tb_nm,
                             eng_tb_nm = dvo.eng_tb_nm,
                             uri = dvo.uri,
                             dir_path = dvo.dir_path,
                             api_keys = ["%2BODpMm%2FIQ2XvsE4H4adLL5A5Oc7bExWMxoT1AlGn8Up%2BAzzvEQ4zxh7WhZK6Z278Of4pxFE%2Bp4Zh7XqZHTFctA%3D%3D"])
                    weatheradministration_open_api_dag : PublicDataPortalWeatherStaticsDag = PublicDataPortalWeatherStaticsDag.create_publicdataportal_weatherstatics_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                                                                                                dag_id = dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dvo.tb_code}_PublicDatPortal_{dag_param_dvo.eng_tb_nm}'),
                                                                                                                                                schedule_interval=relativedelta(days=1),
                                                                                                                                                start_date=datetime(2015, 1, 1),
                                                                                                                                                default_args=_default_args)
                    dag_list.append(weatheradministration_open_api_dag)            
                elif(dvo.tb_nm == PublicDataPortalTableName.SOLARTERM.value):
                    dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                                    tb_code = dvo.tb_code,
                                    tb_nm = dvo.tb_nm,
                                    eng_tb_nm = dvo.eng_tb_nm,
                                    uri = dvo.uri,
                                    dir_path = dvo.dir_path,
                                    api_keys = ["%2BODpMm%2FIQ2XvsE4H4adLL5A5Oc7bExWMxoT1AlGn8Up%2BAzzvEQ4zxh7WhZK6Z278Of4pxFE%2Bp4Zh7XqZHTFctA%3D%3D"])
                    publicdataportal_solarterm_dag = PublicDataPortalSolarTermDag.create_publicdataportal_solarterm_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                                                                     dag_id=dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dvo.tb_code}_PublicDataPortal_{dag_param_dvo.eng_tb_nm}'),
                                                                                                                     schedule_interval=relativedelta(months=1),
                                                                                                                     start_date=datetime(2015, 1, 1),
                                                                                                                     default_args=_default_args)
                    dag_list.append(publicdataportal_solarterm_dag)
                elif(dvo.tb_nm == PublicDataPortalTableName.HOLIDAY.value):
                    dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                                    tb_code = dvo.tb_code,
                                    tb_nm = dvo.tb_nm,
                                    eng_tb_nm = dvo.eng_tb_nm,
                                    uri = dvo.uri,
                                    dir_path = dvo.dir_path,
                                    api_keys = ["%2BODpMm%2FIQ2XvsE4H4adLL5A5Oc7bExWMxoT1AlGn8Up%2BAzzvEQ4zxh7WhZK6Z278Of4pxFE%2Bp4Zh7XqZHTFctA%3D%3D"])
                    publicdataportal_holiday_dag = PublicDataPortalHolidayDag.create_publicdataportal_holiday_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                                                                 dag_id=dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dvo.tb_code}_PublicDataPortal_{dag_param_dvo.eng_tb_nm}'),
                                                                                                                 schedule_interval=relativedelta(months=1),
                                                                                                                 start_date=datetime(2015, 1, 1),
                                                                                                                 default_args=_default_args)
                    dag_list.append(publicdataportal_holiday_dag)
                elif(dvo.tb_nm == PublicDataPortalTableName.NATIONALDAY.value):
                    dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                                    tb_code = dvo.tb_code,
                                    tb_nm = dvo.tb_nm,
                                    eng_tb_nm = dvo.eng_tb_nm,
                                    uri = dvo.uri,
                                    dir_path = dvo.dir_path,
                                    api_keys = ["%2BODpMm%2FIQ2XvsE4H4adLL5A5Oc7bExWMxoT1AlGn8Up%2BAzzvEQ4zxh7WhZK6Z278Of4pxFE%2Bp4Zh7XqZHTFctA%3D%3D"])
                    publicdataportal_nationalday_dag = PublicDataPortalNationalDayDag.create_publicdataportal_nationalday_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                                                                     dag_id=dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dvo.tb_code}_PublicDataPortal_{dag_param_dvo.eng_tb_nm}'),
                                                                                                                     schedule_interval=relativedelta(months=1),
                                                                                                                     start_date=datetime(2015, 1, 1),
                                                                                                                     default_args=_default_args)
                    dag_list.append(publicdataportal_nationalday_dag)
                elif(dvo.tb_nm == PublicDataPortalTableName.Anniversary.value):
                    dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                                    tb_code = dvo.tb_code,
                                    tb_nm = dvo.tb_nm,
                                    eng_tb_nm = dvo.eng_tb_nm,
                                    uri = dvo.uri,
                                    dir_path = dvo.dir_path,
                                    api_keys = ["%2BODpMm%2FIQ2XvsE4H4adLL5A5Oc7bExWMxoT1AlGn8Up%2BAzzvEQ4zxh7WhZK6Z278Of4pxFE%2Bp4Zh7XqZHTFctA%3D%3D"])
                    publicdataportal_anniversary_dag = PublicDataPortalAnniversaryDag.create_publicdataportal_anniversary_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                                                                     dag_id=dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dvo.tb_code}_PublicDataPortal_{dag_param_dvo.eng_tb_nm}'),
                                                                                                                     schedule_interval=relativedelta(months=1),
                                                                                                                     start_date=datetime(2015, 1, 1),
                                                                                                                     default_args=_default_args)
                    dag_list.append(publicdataportal_anniversary_dag)
                elif(dvo.tb_nm == PublicDataPortalTableName.TRADITIONALDAY.value):
                    dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                                    tb_code = dvo.tb_code,
                                    tb_nm = dvo.tb_nm,
                                    eng_tb_nm = dvo.eng_tb_nm,
                                    uri = dvo.uri,
                                    dir_path = dvo.dir_path,
                                    api_keys = ["%2BODpMm%2FIQ2XvsE4H4adLL5A5Oc7bExWMxoT1AlGn8Up%2BAzzvEQ4zxh7WhZK6Z278Of4pxFE%2Bp4Zh7XqZHTFctA%3D%3D"])
                    publicdataportal_traditionalday_dag = PublicDataPortalTraditionalDayDag.create_publicdataportal_traditionalday_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                                                                     dag_id=dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dvo.tb_code}_PublicDataPortal_{dag_param_dvo.eng_tb_nm}'),
                                                                                                                     schedule_interval=relativedelta(months=1),
                                                                                                                     start_date=datetime(2015, 1, 1),
                                                                                                                     default_args=_default_args)
                    dag_list.append(publicdataportal_traditionalday_dag)
                else:
                    assert False, "Invalid tb_nm"
            elif(dvo.src_nm == DATACOLLECTIONSOURCENAME.LOGISTICSINFOCENTER.value):
                pass
            elif(dvo.src_nm == DATACOLLECTIONSOURCENAME.FRED.value):
                if(dvo.tb_nm == FredTableName.KOREANINTERESTRATE.value):
                    dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                                    tb_code = dvo.tb_code,
                                    tb_nm = dvo.tb_nm,
                                    eng_tb_nm = dvo.eng_tb_nm,
                                    uri = dvo.uri,
                                    dir_path = dvo.dir_path,
                                    api_keys = ["fb85ea45398d2cb8aed6c2842f81c936"])
                    fred_koreaninterestrate_dag = FredKoreanInterestRateDag.create_fred_koreaninterestrate_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                                                                dag_id=dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dvo.tb_code}_Pandas_{dag_param_dvo.eng_tb_nm}'),
                                                                                                                schedule_interval=relativedelta(months=1),
                                                                                                                start_date=datetime(2015, 1, 1),
                                                                                                                default_args=_default_args)
                    dag_list.append(fred_koreaninterestrate_dag)
                elif(dvo.tb_nm == FredTableName.USINTERESTRATE.value):
                    dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                                    tb_code = dvo.tb_code,
                                    tb_nm = dvo.tb_nm,
                                    eng_tb_nm = dvo.eng_tb_nm,
                                    uri = dvo.uri,
                                    dir_path = dvo.dir_path,
                                    api_keys = ["fb85ea45398d2cb8aed6c2842f81c936"])
                    fred_usinterestrate_dag = FredUsInterestRateDag.create_fred_usinterestrate_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                                                                dag_id=dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dvo.tb_code}_Pandas_{dag_param_dvo.eng_tb_nm}'),
                                                                                                                schedule_interval=relativedelta(months=1),
                                                                                                                start_date=datetime(2015, 1, 1),
                                                                                                                default_args=_default_args)
                    dag_list.append(fred_usinterestrate_dag)
                elif(dvo.tb_nm == FredTableName.OILPRICE.value):
                    dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                                    tb_code = dvo.tb_code,
                                    tb_nm = dvo.tb_nm,
                                    eng_tb_nm = dvo.eng_tb_nm,
                                    uri = dvo.uri,
                                    dir_path = dvo.dir_path,
                                    api_keys = ["fb85ea45398d2cb8aed6c2842f81c936"])
                    fred_oilprice_dag = FredOilPriceDag.create_fred_oilprice_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                                                                dag_id=dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dvo.tb_code}_Pandas_{dag_param_dvo.eng_tb_nm}'),
                                                                                                                schedule_interval=relativedelta(months=1),
                                                                                                                start_date=datetime(2015, 1, 1),
                                                                                                                default_args=_default_args)
                    dag_list.append(fred_oilprice_dag)
                else:
                    assert False, "Invalid tb_nm"
            elif(dvo.src_nm == DATACOLLECTIONSOURCENAME.YFINANCE.value):
                dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                                    tb_code = dvo.tb_code,
                                    tb_nm = dvo.tb_nm,
                                    eng_tb_nm = dvo.eng_tb_nm,
                                    uri = dvo.uri,
                                    dir_path = dvo.dir_path,
                                    api_keys = ["None"])
                yfinance_usdvokrwexchangerate_dag = YFinanceUsdToKrwExchangeRateDag.create_yfinance_usdtokrwexchangerate_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                                                                dag_id=dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'{dag_param_dvo.tb_code}_Yfinance_{dag_param_dvo.eng_tb_nm}'),
                                                                                                                schedule_interval=relativedelta(months=1),
                                                                                                                start_date=datetime(2015, 1, 1),
                                                                                                                default_args=_default_args)
                dag_list.append(yfinance_usdvokrwexchangerate_dag)
            else:
                assert False, "Invalid src_nm"                            
        dag_param_dvo = DagParamDvo(src_nm = dvo.src_nm,
                                    tb_code = dvo.tb_code,
                                    tb_nm = dvo.tb_nm,
                                    eng_tb_nm = dvo.eng_tb_nm,
                                    uri = dvo.uri,
                                    dir_path = dvo.dir_path,
                                    api_keys = ["None"])
        logisticsinfocenter_dag = LogisticsInfoCenter.create_logisticsinfocenter_dag(dag_config_param=dag_param_dvo.to_dict(),
                                                                                             dag_id=dag_param_dvo.remove_except_alphanumericcharacter_dashe_dot_underscore(f'LogisticsInfoCenter'),
                                                                                             schedule_interval=relativedelta(days=1),
                                                                                             start_date=datetime(2015, 1, 1),
                                                                                             default_args=_default_args,
                                                                                             api_admin_dao=api_admin_dao)
        dag_list.append(logisticsinfocenter_dag)
                
        return dag_list        
