from dataclasses import dataclass


@dataclass
class PublicDataPortalAnniversaryUrl:
    baseUrl : str
    serviceKey : str
    pageNo : str
    numOfRows : str
    solYear : str
    solMonth : str
    def __init__(self, baseUrl, serviceKey, pageNo, numOfRows, solYear, solMonth):
        self.baseUrl = baseUrl
        self.serviceKey = serviceKey
        self.pageNo = pageNo
        self.numOfRows = numOfRows
        self.solYear = solYear
        self.solMonth = solMonth
    def get_full_url(self):
        return f"{self.baseUrl}?serviceKey={self.serviceKey}&pageNo={self.pageNo}&numOfRows={self.numOfRows}&solYear={self.solYear}&solMonth={self.solMonth}"