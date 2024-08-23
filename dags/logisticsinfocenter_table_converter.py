import datetime
from enum import Enum
import pandas as pd
from api_admin_dvo import ApiAdminDvo
def BRZ_1C070201(api_admin_dvo :  ApiAdminDvo): #OD별수송실적(도로)
        st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
        df = pd.read_excel(api_admin_dvo.file_path, engine='xlrd')
        df.columns = df.iloc[0]
        df = df[1:]
        df['시점']=api_admin_dvo.date[0]
        df.rename(columns={df.columns[0]:'출발지/목적지'}, inplace=True)
        df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", encoding="euc-kr", index=False)
        df_len = len(df)        
        api_admin_dvo.sql(st_dt, df_len)        
def BRZ_1C070202(api_admin_dvo :  ApiAdminDvo): #OD별수송실적(철도)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine='xlrd')
    df['시점']=api_admin_dvo.date[0]
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", encoding="euc-kr", index=False)
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)
def BRZ_1C070203(api_admin_dvo :  ApiAdminDvo): #품목별 화물 수송 실적(도로)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine='xlrd')
    df.drop(df.columns[-1], axis=1, inplace=True)
    new_columns=[df.columns[0]]    
    for i in range(1,len(df.columns),3):
        category = df.columns[i]
        new_columns.append(category)
        new_columns.append(category)
        new_columns.append(category)
    df.columns = new_columns    
    df['시점']=api_admin_dvo.date[0]
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", encoding="euc-kr", index=False)
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)
def BRZ_1C070204(api_admin_dvo :  ApiAdminDvo): #품목별 화물 수송 실적(철도)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine='xlrd')
    df.drop(df.index[-1], inplace=True)
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", encoding="euc-kr", index=False)
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)
def BRZ_1C070205(api_admin_dvo :  ApiAdminDvo): #철도 화물 발송,도착 실적(철도) 
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine='xlrd')
    new_columns=['역', '적재컨테이터(출발)', '적재컨테이터(출발)', '적재컨테이터(출발)', '적재컨테이터(출발)',
                    '공컨테이터(출발)', '공컨테이터(출발)', '공컨테이터(출발)', '공컨테이터(출발)', '소계(출발)',
                    '적재컨테이너(도착)', '적재컨테이너(도착)', '적재컨테이너(도착)', '적재컨테이너(도착)',
                    '공컨테이너(도착)', '공컨테이너(도착)', '공컨테이너(도착)', '공컨테이너(도착)', '소계(도착)', '합계'] 
    df.columns = new_columns # 첫번째 행을 컬럼으로 설정
    df = df[1:] # 불필요한 첫번째 행 삭제
    df.iloc[0,0]='컨테이너크기'
    df.iloc[0,9]='소계'
    df.iloc[0,18]='소계'
    df.iloc[0,19]='합계' 
    df['날짜']=api_admin_dvo.date # 새로운 열 추가
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", encoding="euc-kr", index=False)
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)
def BRZ_1C070206(api_admin_dvo :  ApiAdminDvo): #항만별 물동량 통계(해운) 
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine='xlrd')
    year = int(api_admin_dvo.date[:4])
    month = int(api_admin_dvo.date[4:])
    last_year = year-1
    last_date=f"{last_year:04d}{month:02d}"
    file_month=f"{month:02d}"
    df.drop(df.columns[-1], axis=1, inplace=True) 
    new_columns=['항만명', '항만명', '항만명', '항만명', api_admin_dvo.date,'누계(1월~'+file_month+'월)',
                    last_date,'누계(1월~'+file_month+'월)','전년대비 증감율(월간누계)','전년대비 증감율(년간누계)']    
    df.columns = new_columns
    df = df[1:] 
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", encoding="euc-kr", index=False)
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)
def BRZ_1C070207(api_admin_dvo :  ApiAdminDvo): #선박 입항 통계(해운)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine='xlrd')
    new_columns=['항만명', '입항소계(척수)', '입항소계(톤수)', '외항 국전선(척수)', '외항 국적선(톤수)',
                    '외항 외국선(척수)', '외항 외국선(톤수)', '외항소계(척수)', '외항소계(톤수)',
                    '내항 연안선(척수)', '내항 연안선(톤수)', '입출항 합계(척수)', '입출항 합계(톤수)']
    df.columns = new_columns
    df['시점']=api_admin_dvo.date[0]
    df = df[3:]
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", encoding="euc-kr", index=False)
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070208(api_admin_dvo :  ApiAdminDvo): #공항별 물동량 통계    
        st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
        df=pd.read_excel(api_admin_dvo.file_path, engine="xlrd")
        df=df.drop(df.columns[10],axis=1)
        new_columns=[df.columns[0],df.columns[1]]
        for j in range(2,10,4):
            category=df.columns[j] 
            new_columns.append(f"누계({category})")
            new_columns.append(f"화물({category})")
            new_columns.append(f"수화물({category})")
            new_columns.append(f"우편물({category})")
        df.columns=new_columns    
        df=df[1:]
        df['시점']=api_admin_dvo.date[0]
        df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
        df_len = len(df)        
        api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070209(api_admin_dvo :  ApiAdminDvo): #노선별 물동량 통계
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df=pd.read_excel(api_admin_dvo.file_path, engine="xlrd")
    df=df.drop(df.columns[10],axis=1)
    new_columns=[df.columns[0],df.columns[1]]
    for j in range(2,10,4):
        category=df.columns[j] 
        new_columns.append(f"합계({category})")
        new_columns.append(f"화물({category})")
        new_columns.append(f"수화물({category})")
        new_columns.append(f"우편물({category})")
    df.columns=new_columns    
    df=df[1:]
    df['시점']=api_admin_dvo.date[0]
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070210(api_admin_dvo :  ApiAdminDvo): #지역·국가별 물동량 통계
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df=pd.read_excel(api_admin_dvo.file_path, engine="xlrd")
    df=df.drop(df.columns[10],axis=1)
    new_columns=[df.columns[0],df.columns[1]]
    for j in range(2,10,4):
        category=df.columns[j] 
        new_columns.append(f"합계({category})")
        new_columns.append(f"화물({category})")
        new_columns.append(f"수화물({category})")
        new_columns.append(f"우편물({category})")
    df.columns=new_columns
    df=df[1:]
    df['시점']=api_admin_dvo.date[0]
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070211(api_admin_dvo :  ApiAdminDvo): #품목별 물동량 통계
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df=pd.read_excel(api_admin_dvo.file_path, engine="xlrd")
    df.rename(columns={'육 류':'육류','양 곡':'양곡','당 류':'당류','모 래':'모래','비 료':'비료','원 목':'원목','고 철':'고철','기 타':'기타','방직용섬유 및 그제품':'방직용섬유 및 그 제품','철강 및 그제품':'철강 및 그 제품','비철금속 및 그제품':'비철금속 및 그 제품','기계류 및 그부품':'기계류 및 그 부품','전기기기 및 그부품':'전기기기 및 그 부품','차량 및 그부품':'차량 및 그 부품'}, inplace=True)
    df=df.drop(df.columns[34], axis=1)
    df['시점']=api_admin_dvo.date[0]
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070212(api_admin_dvo :  ApiAdminDvo): #수출입화물 물동량 통계
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df=pd.read_excel(api_admin_dvo.file_path, engine="xlrd")
    new_columns=[df.columns[0]]
    for j in range(1, len(df.columns), 2):
        category = df.columns[j]   
        new_columns.append(f"BL건수({category})")
        new_columns.append(f"중량({category})")
    df.columns=new_columns
    df['시점']=api_admin_dvo.date[0]
    df=df[1:]
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070213(api_admin_dvo :  ApiAdminDvo): #차종별 화물차 등록현황(도로)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df=pd.read_excel(api_admin_dvo.file_path, engine="xlrd")
    new_columns=[df.columns[0]]
    for j in range(1, 13):
        new_columns.append(df.iloc[0,j])
    df.columns=new_columns
    df['시점']=api_admin_dvo.date[0]
    df=df[1:]
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070207(api_admin_dvo :  ApiAdminDvo): #선박 출항 통계(해운)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine='xlrd')
    new_columns=['항만명', '출항소계(척수)', '출항소계(톤수)', '외항 국전선(척수)', '외항 국적선(톤수)',
                '외항 외국선(척수)', '외항 외국선(톤수)', '외항소계(척수)', '외항소계(톤수)',
                '내항 연안선(척수)', '내항 연안선(톤수)', '입출항 합계(척수)', '입출항 합계(톤수)']
    df.columns = new_columns
    df['시점']=api_admin_dvo.date[0]
    df = df[3:]
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", encoding="euc-kr", index=False)
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070301(api_admin_dvo :  ApiAdminDvo): #국내 택배시장 물동량(연도)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path)
    df = df.iloc[3:14,6:]
    new_columns = ['시점']
    for j in range(1,len(df.columns)):
        new_columns.append(df.iloc[0,j])
    df.columns = new_columns
    df = df[1:]    
    year = int(api_admin_dvo.end_year) - int(api_admin_dvo.st_year)
    for i in range(year):
        df1 = df.iloc[i]
        df1=pd.DataFrame(df1)
        df1=df1.transpose()
        df1.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
        df_len = len(df1)        
        api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070302(api_admin_dvo :  ApiAdminDvo): #국내 택배시장 매출액(연도)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path)
    df = df.iloc[3:14,6:]
    new_columns = ['시점']
    for j in range(1,len(df.columns)):
        new_columns.append(df.iloc[0,j])
    df.columns = new_columns
    df = df[1:]  
    year = int(api_admin_dvo.end_year) - int(api_admin_dvo.st_year)
    for i in range(year):
        df1 = df.iloc[i]
        df1=pd.DataFrame(df1)
        df1=df1.transpose()
        df1.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
        df_len = len(df1)        
        api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070303(api_admin_dvo :  ApiAdminDvo): #국내 택배시장 단가(연도)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path)
    df = df.iloc[3:13,6:]
    new_columns = ['시점']
    for j in range(1,len(df.columns)):
        new_columns.append(df.iloc[0,j])
    df.columns = new_columns
    df = df[1:]            
    st_year = int(api_admin_dvo.st_year)
    end_year = int(api_admin_dvo.end_year)
    year = end_year - st_year
    for i in range(year):
        df1 = df.iloc[i]
        df1=pd.DataFrame(df1)
        df1=df1.transpose()
        df1.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
        df_len = len(df1)        
        api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070304(api_admin_dvo :  ApiAdminDvo): #국내 택배시장 이용횟수(연도)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path)
    df = df.iloc[3:13,5:]
    new_columns = ['시점']
    for j in range(1,3):
        new_columns.append(df.iloc[0,j])
    df.columns = new_columns
    df = df[1:]    
    year = int(api_admin_dvo.end_year) - int(api_admin_dvo.st_year)
    for i in range(year):
        df1 = df.iloc[i]
        df1=pd.DataFrame(df1)
        df1=df1.transpose()
        df1.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
        df_len = len(df1)        
        api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070305(api_admin_dvo :  ApiAdminDvo): #국내 택배시장 물동량(월별)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine="xlrd")
    df.drop(df.columns[14],axis=1, inplace=True)
    year = int(api_admin_dvo.end_year) - int(api_admin_dvo.st_year)
    for i in range(year):
        df1 = df.iloc[i]
        df1=pd.DataFrame(df1)
        df1=df1.transpose()
        df1.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
        df_len = len(df1)        
        api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070306(api_admin_dvo :  ApiAdminDvo): #국내 택배시장 매출액(월별)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine="xlrd")
    df.drop(df.columns[14],axis=1, inplace=True)
    year = int(api_admin_dvo.end_year) - int(api_admin_dvo.st_year)
    for i in range(year):
        df1 = df.iloc[i]
        df1=pd.DataFrame(df1)
        df1=df1.transpose()
        df1.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
        df_len = len(df1)        
        api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070307(api_admin_dvo :  ApiAdminDvo): #국내 택배시장 단가(월별)
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine="xlrd")
    df.drop(df.columns[13],axis=1, inplace=True)
    year = int(api_admin_dvo.end_year) - int(api_admin_dvo.st_year)
    for i in range(year):
        df1 = df.iloc[i]
        df1=pd.DataFrame(df1)
        df1=df1.transpose()
        df1.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
        df_len = len(df1)        
        api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070401(api_admin_dvo :  ApiAdminDvo): #지역별 물류창고업 등록현황
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine="xlrd")
    new_columns=[df.columns[0], df.iloc[0,1], "물시법창고(물시법)", "항만창고(물시법)", "보세창고(관세법)", "보관저장업(화확물질관리법)", "냉동냉장(식품위생법)", "축산물보관(축산물위생법)","냉동냉장(수산식품산업법)"]
    df.columns = new_columns
    df['시점']=api_admin_dvo.date[0]
    df = df[2:]
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", index=False, encoding="euc-kr")
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070402(api_admin_dvo :  ApiAdminDvo): #면적별 물류창고업 등록현황
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine='xlrd')
    new_columns=[df.columns[0]]

    for i in range(1, len(df.columns), 2):
        category = df.columns[i]   
        new_columns.append(f"업체수({category})")
        new_columns.append(f"면적합계(㎡)({category})")
    df.columns = new_columns
    df['시점']=api_admin_dvo.date[0]
    df = df[1:]
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", encoding="euc-kr", index=False)
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)

def BRZ_1C070403(api_admin_dvo :  ApiAdminDvo): #연도별 물류창고업 등록현황    
    st_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    df = pd.read_excel(api_admin_dvo.file_path, engine='xlrd')
    df = df[1:]        
    df.to_csv(f"{api_admin_dvo.table_code}-{api_admin_dvo.timestamp}.csv", encoding="euc-kr", index=False)
    df_len = len(df)        
    api_admin_dvo.sql(st_dt, df_len)
        
class LOGISTICSINFOCENTERTABLENAME(Enum):
    ODBYROADTRANSPORTPERFORMANCE = "OD별 수송실적(도로)"
    ODBYRAILTRANSPORTPERFORMANCE = "OD별 수송실적(철도)"
    AIRPORTCARGOVOLUMESTATISTICS = "공항별 물동량 통계(항공)"
    ANNUALDOMESTICCOURIERMARKETUNITPRICE = "국내 택배시장 단가(연도)"
    MONTHLYDOMESTICCOURIERMARKETUNITPRICE = "국내 택배시장 단가(월별)"
    ANNUALDOMESTICCOURIERMARKETREVENUE = "국내 택배시장 매출액(연도)"
    MONTHLYDOMESTICCOURIERMARKETREVENUE = "국내 택배시장 매출액(월별)"
    ANNUALDOMESTICCOURIERMARKETVOLUME = "국내 택배시장 물동량(연도)"
    MONTHLYDOMESTICCOURIERMARKETVOLUME = "국내 택배시장 물동량(월별)"
    ANNUALDOMESTICCOURIERMARKETUSAGEFREQUENCY = "국내 택배시장 이용횟수(연도)"
    ROUTECARGOVOLUMESTATISTICS = "노선별 물동량 통계(항공)"
    AREABASEDLOGISTICSWAREHOUSEREGISTRATION = "면적별 물류창고업 등록현황"
    VESSELARRIVALDEPARTURESTATISTICS = "선박 입출항 통계(해운)"
    EXPORTIMPORTCARGOVOLUMESTATISTICS = "수출입화물 물동량 통계(항공)"
    ANNUALLOGISTICSWAREHOUSEREGISTRATION = "연도별 물류창고업 등록현황"
    REGIONCOUNTRYCARGOVOLUMESTATISTICS = "지역·국가별 물동량 통계(항공)"
    REGIONBASEDLOGISTICSWAREHOUSEREGISTRATION = "지역별 물류창고업 등록현황"
    VEHICLETYPEFREIGHTTRUCKREGISTRATION = "차종별 화물차 등록현황(도로)"
    RAILFREIGHTDISPATCHANDARRIVALPERFORMANCE = "철도 화물 발송/도착실적(철도)"
    ITEMCARGOVOLUMESTATISTICS = "품목별 물동량 통계(해운)"
    ROADITEMFREIGHTTRANSPORTPERFORMANCE = "품목별 화물 수송 실적(도로)"
    RAILITEMFREIGHTTRANSPORTPERFORMANCE = "품목별 화물 수송 실적(철도)"
    PORTCARGOVOLUMESTATISTICS = "항만별 물동량 통계(해운)"
    