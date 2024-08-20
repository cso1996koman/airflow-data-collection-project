from dataclasses import dataclass
from datetime import datetime
@dataclass
class ApiAdminDvo:
    src_nm: str
    tb_nm: str
    tb_code: str
    version: str
    uri: str
    created_at: datetime
    dir_path: str
    column1: str
    eng_tb_nm : str
    