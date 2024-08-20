from dataclasses import dataclass
@dataclass
class WeatherAdministrationUrl:
    baseUrl : str
    serviceKey : str
    pageNo : str
    numOfRows : str
    dataType : str
    dataCd : str
    dateCd : str
    startDt : str
    endDt : str
    stnIds : str    
    def getFullUrl(self) -> str:
        url = (
                f"{self.baseUrl}?ServiceKey={self.serviceKey}&pageNo={self.pageNo}&numOfRows={self.numOfRows}"
                f"&dataType={self.dataType}&dataCd={self.dataCd}&dateCd={self.dateCd}&startDt={self.startDt}"
                f"&endDt={self.endDt}&stnIds={self.stnIds}"
            )
        return url