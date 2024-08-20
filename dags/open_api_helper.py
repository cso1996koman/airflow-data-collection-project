import json
import logging
import xml.etree.ElementTree as ET
import requests
from typing import List, Dict
from publicdataportal_table_name_enum import PublicDataPortalTableName
from data_collection_source_name_enum import DATACOLLECTIONSOURCENAME
class OpenApiHelper:
    def __init__(self):    
        pass
    def get_multi_unit_param(self, unit_param: str) -> List[str]:    
        return [param + '+' for param in unit_param.split('+')]
    def get_appeneded_response_bymulti_unit_param(self, url_obj , unit_params: List[str]) -> Dict:    
        merged_json_responses : dict = None
        for param in unit_params:
            url_obj.unit_param = param
            response : requests.Response = requests.get(url_obj.get_full_url())
            if response.status_code == 200:
                if merged_json_responses is None:
                    merged_json_responses = response.json()
                else:
                    merged_json_responses.update(response.json())
            else:
                print(f"Error fetching data for param {param}: {response.status_code}")
        return merged_json_responses
    def get_response(self, url_str : str, src_nm : str, tb_nm : str) -> Dict:
        if src_nm == DATACOLLECTIONSOURCENAME.KOSIS.value:
            response : requests.Response = requests.get(url_str)
            if response.status_code == 200:
                return response.json()
            else:
                assert False, f"Error fetching data: {response.status_code}"
        elif src_nm == DATACOLLECTIONSOURCENAME.PUBLICDATAPORTAL.value:
            response : requests.Response = requests.get(url_str)
            if response.status_code == 200:
                if tb_nm == PublicDataPortalTableName.WEATHERSTATICS.value:
                    logging.info(url_str)
                    response : requests.Response = requests.get(url_str)
                    if response.status_code == 200:
                        response_json = response.json().get('response', {}).get('body', {}).get('items', {}).get('item', {})
                        return response_json
                    else:
                        assert False, f"Error fetching data: {response.status_code}"                
                else:                        
                    xml_data = response.content
                    root = ET.fromstring(xml_data)
                    dict_data = {root.tag: self.xml_to_dict(xml_data)}
                    json_data = json.dumps(dict_data, indent=4)
                    return json_data
            else:
                assert False, f"Error fetching data: {response.status_code}"
    def assert_valid_unit_param(self, unit_param: str):
        parts = unit_param.split('+')
        for part in parts:
            if part and not part.isdigit():
                raise AssertionError(f"Invalid unit param: {unit_param}")
    def xml_to_dict(self, xml_data) -> Dict:
        result = {}
        for child in xml_data:
            if child.tag not in result:
                result[child.tag] = self.xml_to_dict(child)
            else:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(self.xml_to_dict(child))
        return result