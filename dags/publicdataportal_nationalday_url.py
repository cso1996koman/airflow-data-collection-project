from dataclasses import dataclass
@dataclass
class PublicDataPortalNationalDayUrl:
    baseUrl : str
    serviceKey : str
    solYear : str
    solMonth : str
    def __init__(self, baseUrl, serviceKey, solYear, solMonth):
        self.baseUrl = baseUrl
        self.serviceKey = serviceKey
        self.solYear = solYear
        self.solMonth = solMonth
    def get_full_url(self):
        return f"{self.baseUrl}?serviceKey={self.serviceKey}&solYear={self.solYear}&solMonth={self.solMonth}"
    