import json
import logging
import xmltodict
import requests
from typing import List, Dict
from kosis_url import OBJTYPE, KosisUrl
from publicdataportal_table_name_enum import PublicDataPortalTableName
from data_collection_source_name_enum import DATACOLLECTIONSOURCENAME
class OpenApiHelper:
    def __init__(self):    
        pass
    def get_multi_unit_param(self, unit_param: str) -> List[str]:    
        param_list : List[str] = []
        unit_param = unit_param.replace(' ', '')
        for part in unit_param.split('+'):
            part = part.strip()
            if part == '+' or part == '':
                break
            param_list.append(f"{part}+")
        return param_list
    def get_appeneded_response_bymulti_unit_param(self, url_obj : KosisUrl, unit_params: List[str], obj_type : str) -> Dict:
        merged_json_responses : dict = None
        for param in unit_params:
            logging.info(f"param : {param}, paramType : {type(param)}")
            if obj_type == OBJTYPE.OBJL1.value:
                url_obj.objL1 = param
            elif obj_type == OBJTYPE.OBJL2.value:
                url_obj.objL2 = param
            elif obj_type == OBJTYPE.OBJL3.value:
                url_obj.objL3 = param
            elif obj_type == OBJTYPE.OBJL4.value:
                url_obj.objL4 = param
            elif obj_type == OBJTYPE.OBJL5.value:
                url_obj.objL5 = param
            elif obj_type == OBJTYPE.OBJL6.value:
                url_obj.objL6 = param
            elif obj_type == OBJTYPE.OBJL7.value:
                url_obj.objL7 = param
            elif obj_type == OBJTYPE.OBJL8.value:
                url_obj.objL8 = param
            else:
                assert False, f"Invalid obj_type: {obj_type}"
            logging.info(f"appended_response_url_obj.get_full_url() : {url_obj.get_full_url()}")
            response : requests.Response = requests.get(url_obj.get_full_url())
            logging.info(f"response content : {response.content}")
            logging.info(f"response content : {response.json()}")
            if response.status_code == 200:
                # KosisErrorResponseMessageDict : {"err": "errNo", "errMsg": "errMessage"}
                response_json : dict = response.json()
                response_json_key_list : list = list(response_json.keys())
                if response_json_key_list[0] == 'err':
                    logging.info(f"Error fetching data for url : {url_obj.get_full_url()} , errorMessage : {response_json.get('errMsg')}")
                    continue
                if merged_json_responses is None:
                    merged_json_responses = response.json()
                else:
                    merged_json_responses.update(response.json())
            else:
                print(f"Error fetching data for param {param}: {response.status_code}")
        return merged_json_responses
    def get_response(self, url_str : str, src_nm : str, tb_nm : str) -> Dict:
        logging.info(f"response_url_str : {url_str}")
        if src_nm == DATACOLLECTIONSOURCENAME.KOSIS.value:
            response : requests.Response = requests.get(url_str)
            if response.status_code == 200:
                return response.json()
            else:
                assert False, f"Error fetching data: {response.status_code}"
        elif src_nm == DATACOLLECTIONSOURCENAME.PUBLICDATAPORTAL.value:
            response : requests.Response = requests.get(url_str)
            response_json : json = None
            if response.status_code == 200:
                logging.info(f"get_response_content :{response.content}")
                if tb_nm == PublicDataPortalTableName.WEATHERSTATICS.value:                    
                    response_json = response.json().get('response', {}).get('body', {}).get('items', {}).get('item', {})
                else:
                    xml_data = response.content
                    json_data = json.loads(json.dumps(xmltodict.parse(xml_data)))                    
                    response_json = json_data.get('response', {})
                    body_json = response_json.get('body', {})
                    items_json = body_json.get('items', {})
                    if items_json is not None:
                        response_json = items_json.get('item', {})
            else:
                assert False, f"Error fetching data: {response.status_code}"
            return response_json
    def assert_valid_unit_param(self, unit_param: str):
        parts = unit_param.split('+')
        for part in parts:
            if part and not part.isdigit():
                raise AssertionError(f"Invalid unit param: {unit_param}")