from dataclasses import dataclass
@dataclass
class PublicDataPortalSolarTermUrl:
    baseUrl : str
    serviceKey : str
    solYear : str
    solMonth : str
    kst : str
    sunLongitude : str
    numOfRows : str
    pageNo : str
    totalCount : str
    def __init__(self, baseUrl, serviceKey, solYear, solMonth, kst, sunLongitude, numOfRows, pageNo, totalCount):
        self.baseUrl = baseUrl
        self.serviceKey = serviceKey
        self.solYear = solYear
        self.solMonth = solMonth
        self.kst = kst
        self.sunLongitude = sunLongitude
        self.numOfRows = numOfRows
        self.pageNo = pageNo
        self.totalCount = totalCount
    def get_full_url(self):
        return f"{self.baseUrl}?serviceKey={self.serviceKey}&solYear={self.solYear}&solMonth={self.solMonth}&kst={self.kst}&sunLongitude={self.sunLongitude}&numOfRows={self.numOfRows}&pageNo={self.pageNo}&totalCount={self.totalCount}"