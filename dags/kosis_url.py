from dataclasses import dataclass
from enum import Enum
@dataclass
class KosisUrl:
    baseUrl : str
    apiKey : str
    itmId : str
    objL1 : str
    objL2 : str
    objL3 : str
    objL4 : str
    objL5 : str
    objL6 : str
    objL7 : str
    objL8 : str
    format : str
    jsonVD : str
    prdSe : str
    startPrdDe : str
    endPrdDe : str
    orgId : str
    tblId : str
    def get_full_url(self) -> str:
        url = (
                f"{self.baseUrl}&apiKey={self.apiKey}&itmId={self.itmId}&objL1={self.objL1}"
                f"&objL2={self.objL2}&objL3={self.objL3}&objL4={self.objL4}&objL5={self.objL5}"
                f"&objL6={self.objL6}&objL7={self.objL7}&objL8={self.objL8}&format={self.format}"
                f"&jsonVD={self.jsonVD}&prdSe={self.prdSe}&startPrdDe={self.startPrdDe}"
                f"&endPrdDe={self.endPrdDe}&orgId={self.orgId}&tblId={self.tblId}"
            )
        return url    
class PRDSEENUM(Enum):
    YEAR = 'Y'
    MONTH = 'M'
    QUARTER = 'Q'
    
    