from dataclasses import dataclass
import re
from typing import List
@dataclass
class DagParamDvo:
    src_nm : str
    tb_code : str
    tb_nm : str
    eng_tb_nm : str
    uri : str
    dir_path : str
    api_keys : List[str]    
    def to_dict(self):
        return {
            "src_nm": self.src_nm,
            "tb_code": self.tb_code,
            "tb_nm": self.tb_nm,
            "uri": self.uri,            
            "dir_path": self.dir_path,
            "api_keys": self.api_keys[0]
        }
    def remove_except_alphanumericcharacter_dashe_dot_underscore(self, param_str : str) -> str:
        return re.sub(r'[^a-zA-Z0-9-_\.]', '', param_str)