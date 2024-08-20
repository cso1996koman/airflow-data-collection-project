import logging
import re
from urllib.parse import urlparse, urlunparse
from kosis_url import KosisUrl
from weatheradministration_url import WeatherAdministrationUrl
from publicdataportal_anniversary_url import PublicDataPortalAnniversaryUrl
from publicdataportal_holiday_url import PublicDataPortalHolidayUrl
from publicdataportal_nationalday_url import PublicDataPortalNationalDayUrl
from publicdataportal_solarterm_url import PublicDataPortalSolarTermUrl
from publicdataportal_traditionalday_url import PublicDataPortalTraditionalDayUrl
class UrlObjectFactory:    
    @staticmethod
    def createKosisUrl(fullUrl : str) -> KosisUrl:
        logging.info(f"fullUrl: {fullUrl}")
        baseUrl = UrlObjectFactory.extractKosisBaseUrl(fullUrl)
        apikey = UrlObjectFactory.extractParameter(fullUrl, r'apikey=[^&]*')
        orgId = UrlObjectFactory.extractParameter(fullUrl, r'orgId=[^&]*')
        tblId = UrlObjectFactory.extractParameter(fullUrl, r'tblId=[^&]*')
        itmId = UrlObjectFactory.extractParameter(fullUrl, r'itmId=[^&]*')
        objL1 = UrlObjectFactory.extractParameter(fullUrl, r'objL1=[^&]*')
        objL2 = UrlObjectFactory.extractParameter(fullUrl, r'objL2=[^&]*')
        objL3 = UrlObjectFactory.extractParameter(fullUrl, r'objL3=[^&]*')
        objL4 = UrlObjectFactory.extractParameter(fullUrl, r'objL4=[^&]*')
        objL5 = UrlObjectFactory.extractParameter(fullUrl, r'objL5=[^&]*')
        objL6 = UrlObjectFactory.extractParameter(fullUrl, r'objL6=[^&]*')
        objL7 = UrlObjectFactory.extractParameter(fullUrl, r'objL7=[^&]*')
        objL8 = UrlObjectFactory.extractParameter(fullUrl, r'objL8=[^&]*')
        format = UrlObjectFactory.extractParameter(fullUrl, r'format=[^&]*')
        jsonVD = UrlObjectFactory.extractParameter(fullUrl, r'jsonVD=[^&]*')
        prdSe = UrlObjectFactory.extractParameter(fullUrl, r'prdSe=[^&]*')
        startPrdDe = UrlObjectFactory.extractParameter(fullUrl, r'startPrdDe=[^&]*')
        endPrdDe = UrlObjectFactory.extractParameter(fullUrl, r'endPrdDe=[^&]*')
        logging.info(f"baseUrl: {baseUrl}, apikey: {apikey}, orgId: {orgId}, tblId: {tblId}, itmId: {itmId}, objL1: {objL1}, objL2: {objL2}, objL3: {objL3}, objL4: {objL4}, objL5: {objL5}, objL6: {objL6}, objL7: {objL7}, objL8: {objL8}, format: {format}, jsonVD: {jsonVD}, prdSe: {prdSe}, startPrdDe: {startPrdDe}, endPrdDe: {endPrdDe}")
        return KosisUrl(baseUrl, apikey, itmId, objL1, objL2, objL3, objL4, objL5, objL6, objL7, objL8, format, jsonVD, prdSe, startPrdDe, endPrdDe, orgId, tblId)
    @staticmethod  
    def createWeatherAdministrationUrl(fullUrl : str) -> WeatherAdministrationUrl:
        logging.info(f"fullUrl: {fullUrl}")
        baseUrl = UrlObjectFactory.extractWeatherAdministrationBaseUrl(fullUrl)
        serviceKey = UrlObjectFactory.extractParameter(fullUrl, r'ServiceKey=[^&]*')
        pageNo = UrlObjectFactory.extractParameter(fullUrl, r'pageNo=[^&]*')
        numOfRows = UrlObjectFactory.extractParameter(fullUrl, r'numOfRows=[^&]*')
        dataType = UrlObjectFactory.extractParameter(fullUrl, r'dataType=[^&]*')
        dataCd = UrlObjectFactory.extractParameter(fullUrl, r'dataCd=[^&]*')
        dateCd = UrlObjectFactory.extractParameter(fullUrl, r'dateCd=[^&]*')
        startDt = UrlObjectFactory.extractParameter(fullUrl, r'startDt=[^&]*')
        endDt = UrlObjectFactory.extractParameter(fullUrl, r'endDt=[^&]*')
        stnIds = UrlObjectFactory.extractParameter(fullUrl, r'stnIds=[^&]*')
        logging.info(f"baseUrl: {baseUrl}, serviceKey: {serviceKey}, pageNo: {pageNo}, numOfRows: {numOfRows}, dataType: {dataType}, dataCd: {dataCd}, dateCd: {dateCd}, startDt: {startDt}, endDt: {endDt}, stnIds: {stnIds}")
        return WeatherAdministrationUrl(baseUrl, serviceKey, pageNo, numOfRows, dataType, dataCd, dateCd, startDt, endDt, stnIds)
    @staticmethod
    def createPublicDataPortalAnniversaryUrl(fullUrl : str) -> PublicDataPortalAnniversaryUrl:
        logging.info(f"fullUrl: {fullUrl}")
        baseUrl = UrlObjectFactory.extractPublicDataPortalAnniversaryBaseUrl(fullUrl)
        serviceKey = UrlObjectFactory.extractParameter(fullUrl, r'serviceKey=[^&]*')
        pageNo = UrlObjectFactory.extractParameter(fullUrl, r'pageNo=[^&]*')
        numOfRows = UrlObjectFactory.extractParameter(fullUrl, r'numOfRows=[^&]*')
        startDt = UrlObjectFactory.extractParameter(fullUrl, r'startDt=[^&]*')
        endDt = UrlObjectFactory.extractParameter(fullUrl, r'endDt=[^&]*')
        logging.info(f"baseUrl: {baseUrl}, serviceKey: {serviceKey}, pageNo: {pageNo}, numOfRows: {numOfRows}, startDt: {startDt}, endDt: {endDt}")
        return PublicDataPortalAnniversaryUrl(baseUrl, serviceKey, pageNo, numOfRows, startDt, endDt)
    @staticmethod
    def createPublicDataPortalHolidayUrl(fullUrl : str) -> PublicDataPortalHolidayUrl:
        logging.info(f"fullUrl: {fullUrl}")
        baseUrl = UrlObjectFactory.extractPublicDataPortalHolidayBaseUrl(fullUrl)
        serviceKey = UrlObjectFactory.extractParameter(fullUrl, r'serviceKey=[^&]*')
        pageNo = UrlObjectFactory.extractParameter(fullUrl, r'pageNo=[^&]*')
        numOfRows = UrlObjectFactory.extractParameter(fullUrl, r'numOfRows=[^&]*')
        startDt = UrlObjectFactory.extractParameter(fullUrl, r'startDt=[^&]*')
        endDt = UrlObjectFactory.extractParameter(fullUrl, r'endDt=[^&]*')
        logging.info(f"baseUrl: {baseUrl}, serviceKey: {serviceKey}, pageNo: {pageNo}, numOfRows: {numOfRows}, startDt: {startDt}, endDt: {endDt}")
        return PublicDataPortalHolidayUrl(baseUrl, serviceKey, pageNo, numOfRows, startDt, endDt)
    @staticmethod
    def createPublicDataPortalNationalDayUrl(fullUrl : str) -> PublicDataPortalNationalDayUrl:
        logging.info(f"fullUrl: {fullUrl}")
        baseUrl = UrlObjectFactory.extractPublicDataPortalNationalDayBaseUrl(fullUrl)
        serviceKey = UrlObjectFactory.extractParameter(fullUrl, r'serviceKey=[^&]*')
        pageNo = UrlObjectFactory.extractParameter(fullUrl, r'pageNo=[^&]*')
        numOfRows = UrlObjectFactory.extractParameter(fullUrl, r'numOfRows=[^&]*')
        startDt = UrlObjectFactory.extractParameter(fullUrl, r'startDt=[^&]*')
        endDt = UrlObjectFactory.extractParameter(fullUrl, r'endDt=[^&]*')
        logging.info(f"baseUrl: {baseUrl}, serviceKey: {serviceKey}, pageNo: {pageNo}, numOfRows: {numOfRows}, startDt: {startDt}, endDt: {endDt}")
        return PublicDataPortalNationalDayUrl(baseUrl, serviceKey, pageNo, numOfRows, startDt, endDt)
    @staticmethod
    def createPublicDataPortalSolarTermUrl(fullUrl : str) -> PublicDataPortalSolarTermUrl:
        logging.info(f"fullUrl: {fullUrl}")
        baseUrl = UrlObjectFactory.extractPublicDataPortalSolarTermBaseUrl(fullUrl)
        serviceKey = UrlObjectFactory.extractParameter(fullUrl, r'serviceKey=[^&]*')
        pageNo = UrlObjectFactory.extractParameter(fullUrl, r'pageNo=[^&]*')
        numOfRows = UrlObjectFactory.extractParameter(fullUrl, r'numOfRows=[^&]*')
        startDt = UrlObjectFactory.extractParameter(fullUrl, r'startDt=[^&]*')
        endDt = UrlObjectFactory.extractParameter(fullUrl, r'endDt=[^&]*')
        logging.info(f"baseUrl: {baseUrl}, serviceKey: {serviceKey}, pageNo: {pageNo}, numOfRows: {numOfRows}, startDt: {startDt}, endDt: {endDt}")
        return PublicDataPortalSolarTermUrl(baseUrl, serviceKey, pageNo, numOfRows, startDt, endDt)
    @staticmethod
    def createPublicDataPortalTraditionalDayUrl(fullUrl : str) -> PublicDataPortalTraditionalDayUrl:
        logging.info(f"fullUrl: {fullUrl}")
        baseUrl = UrlObjectFactory.extractPublicDataPortalTraditionalDayBaseUrl(fullUrl)
        serviceKey = UrlObjectFactory.extractParameter(fullUrl, r'serviceKey=[^&]*')
        pageNo = UrlObjectFactory.extractParameter(fullUrl, r'pageNo=[^&]*')
        numOfRows = UrlObjectFactory.extractParameter(fullUrl, r'numOfRows=[^&]*')
        startDt = UrlObjectFactory.extractParameter(fullUrl, r'startDt=[^&]*')
        endDt = UrlObjectFactory.extractParameter(fullUrl, r'endDt=[^&]*')
        logging.info(f"baseUrl: {baseUrl}, serviceKey: {serviceKey}, pageNo: {pageNo}, numOfRows: {numOfRows}, startDt: {startDt}, endDt: {endDt}")
        return PublicDataPortalTraditionalDayUrl(baseUrl, serviceKey, pageNo, numOfRows, startDt, endDt)
    @staticmethod
    def extractKosisBaseUrl(fullUrl : str) -> str:
        parsed_url = urlparse(fullUrl)
        base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', 'method=getList', ''))
        return base_url
    @staticmethod
    def extractWeatherAdministrationBaseUrl(fullUrl : str) -> str:
        parsed_url = urlparse(fullUrl)
        base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', ''))
        return base_url
    @staticmethod
    def extractPublicDataPortalAnniversaryBaseUrl(fullUrl : str) -> str:
        parsed_url = urlparse(fullUrl)
        base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', ''))
        return base_url
    @staticmethod
    def extractPublicDataPortalHolidayBaseUrl(fullUrl : str) -> str:
        parsed_url = urlparse(fullUrl)
        base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', ''))
        return base_url
    @staticmethod
    def extractPublicDataPortalNationalDayBaseUrl(fullUrl : str) -> str:
        parsed_url = urlparse(fullUrl)
        base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', ''))
        return base_url
    @staticmethod
    def extractPublicDataPortalSolarTermBaseUrl(fullUrl : str) -> str:
        parsed_url = urlparse(fullUrl)
        base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', ''))
        return base_url
    @staticmethod
    def extractPublicDataPortalTraditionalDayBaseUrl(fullUrl : str) -> str:
        parsed_url = urlparse(fullUrl)
        base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', ''))
        return base_url
    @staticmethod
    def extractParameter(fullUrl, pattern: str) -> str:
        logging.info(f"fullUrl: {fullUrl}, pattern: {pattern}")
        match = re.search(pattern, fullUrl)
        return match.group(0).split('=')[1] if match else ''