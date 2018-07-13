
# coding: utf-8

# In[ ]:

import time
from time import strftime
now=time.time()
start_time=strftime("%y%m%d-%H%M%S")


# In[1]:


import math

import numpy as np
import pandas as pd
from sqlalchemy import create_engine ,types


# In[2]:


# 파라미터 디폴트값 설정
ma_section1=17
ma_section2=9


# # 이동평균(판매추세량) 함수

# In[3]:


def sub_function(data):
    data = data.reset_index(drop=True)
    data["MA"]=data["QTY_NEW"].rolling(window=ma_section1,center=True).mean()
    
    suborder=math.floor(ma_section1/2)
   
    list=[]
    maxLength=len(data)-1
    for i in range(0,suborder):
        list.append(data['QTY_NEW'][0:i+suborder+1].mean())
        data.loc[i,"MA"]=list[i]
    
    list1=[]
    for i in range(0,suborder):
        list1.append(data['QTY_NEW'][-i-suborder-1:].mean())
        data['MA'][maxLength-i]=list1[i]
        
    return data


# # 정제된 판매량의 이동평균 함수

# In[4]:


def smoo_function(data):
    data = data.reset_index(drop=True)
    data["SMOOTH"]=data["REFINED_QTY"].rolling(window=ma_section2,center=True).mean()
    
    suborder=math.floor(ma_section2/2)
   
    list=[]
    maxLength=len(data)-1
    for i in range(0,suborder):
        list.append(data['REFINED_QTY'][0:i+suborder+1].mean())
        data.loc[i,"SMOOTH"]=list[i]
    
    list1=[]
    for i in range(0,suborder):
        list1.append(data['REFINED_QTY'][-i-suborder-1:].mean())
        data['SMOOTH'][maxLength-i]=list1[i]
        
    return data


# # 변동률(판매추세량의 표준편차) 함수

# In[5]:


def std_function(data):
    data = data.reset_index(drop=True)
    data["MSTD"]=data["MA"].rolling(window=ma_section2,center=True).std()
    
    suborder=math.floor(ma_section2/2)
   
    list=[]
    maxLength=len(data)-1
    for i in range(0,suborder):
        list.append(data['MA'][0:i+suborder+1].std())
        data.loc[i,"MSTD"]=list[i]
    
    list1=[]
    for i in range(0,suborder):
        list1.append(data['MA'][-i-suborder-1:].std())
        data['MSTD'][maxLength-i]=list1[i]
        
    return data


# ### 데이터 불러오기
print('Data Loading')
# In[6]:


# DB 커넥션 열기
engine1 = create_engine('oracle+cx_oracle://kopo:kopo@192.168.110.112:1521/orcl') 
engine2 = create_engine('postgresql://kopo:kopo@192.168.110.111:5432/kopo') 
# DB 테이블을 읽어 Data Frame 변수에 저장하기
customerData = pd.read_sql_query('SELECT * FROM KOPO_CHANNEL_SEASONALITY_NEW WHERE SUBSTR(YEARWEEK,5,2) <= 52', engine1) 
paramData = pd.read_sql_query('SELECT * FROM kopo_parameter_omz', engine1)


# In[7]:


# 컬럼명 대문자로
customerData.columns = [x.upper() for x in customerData.columns]
# 정렬
customerData=customerData.sort_values(["REGIONID","PRODUCT","YEARWEEK"], ascending=[True,True,True])
# 인덱스 다시 설정
sortedData = customerData.reset_index(drop=True)
#sortedData.head(3)


# ### 파라미터 테이블

# In[8]:


#paramData


# In[9]:


paramData.set_index("param_name",inplace=True)
#paramData


# In[10]:


ma_section1=int(paramData.loc['MA_SECTION1']['param_value'])
ma_section2=int(paramData.loc['MA_SECTION2']['param_value'])
table_name=paramData.loc['SAVE_TABLE_NAME']['param_value'].lower()


# ### YEAR / WEEK 컬럼

# In[11]:


sortedData['YEAR']=sortedData['YEARWEEK'].str[0:4]
sortedData['WEEK']=sortedData['YEARWEEK'].str[4:6]
#sortedData.head(2)


# ### 반품되거나 판매량0인 값은 1로 치환
print('Data Refining')
# In[12]:


sortedData["QTY_NEW"] = np.where(sortedData["QTY"] <= 0, 1, sortedData["QTY"])
#sortedData.head(2)


# ### 이동평균(판매추세량) 구하기

# In[13]:


groupResult = sortedData.groupby(['REGIONID','PRODUCT']).apply(sub_function)
ma_Result=groupResult.reset_index(drop=True)
#ma_Result.head(2)


# ### 변동률 구하기

# In[14]:


refined_Result = ma_Result.groupby(['REGIONID','PRODUCT']).apply(std_function)
#refined_Result.head(2)


# ### 상한/하한 구하기

# In[15]:


refined_Result["UPPER_BOUND"]=refined_Result["MA"]+refined_Result["MSTD"]
refined_Result["LOWER_BOUND"]=refined_Result["MA"]-refined_Result["MSTD"]
#refined_Result.head(2)


# ### 정제된 판매량 구하기

# In[16]:


refined_Result['REFINED_QTY']=np.where(refined_Result['QTY_NEW']>refined_Result['UPPER_BOUND'],refined_Result['MA'],        np.where(refined_Result['QTY_NEW']<refined_Result['LOWER_BOUND'],refined_Result['MA'],refined_Result['QTY_NEW']))
#refined_Result.head(2)


# ### 스무딩 처리

# In[17]:


refined_Result=refined_Result.reset_index(drop=True)
smoo_Result = refined_Result.groupby(['REGIONID','PRODUCT']).apply(smoo_function)
#smoo_Result.head(2)


# # 계절성지수 산출
# ### 안정된 시장 = 실제판매량/스무딩처리   || 불안정 시장 = 정제된 판매량/스무딩처리

# In[18]:


ratio_Result=smoo_Result.reset_index(drop=True)
ratio_Result["STABLE"] = np.where(ratio_Result['SMOOTH']==0,ratio_Result['QTY_NEW'],ratio_Result["QTY_NEW"]/ratio_Result["SMOOTH"])
ratio_Result["UNSTABLE"] = np.where(ratio_Result['SMOOTH']==0,ratio_Result['REFINED_QTY'],ratio_Result["REFINED_QTY"]/ratio_Result["SMOOTH"])
#ratio_Result.head(2)


# ### 오라클에 CLOB형태 저장할 경우 시간이 오래걸리기 때문에 object형을 varchar로 저장하도록 옵션 추가해야함

# In[19]:

print('Data Saving')

to_varchar = {c:types.VARCHAR(ratio_Result[c].str.len().max()) for c in ratio_Result.columns[ratio_Result.dtypes == 'object'].tolist()}


ratio_Result.to_sql(table_name, engine2, if_exists='replace', index=False, dtype=to_varchar)


# ### .py로 변환

# In[ ]:


#get_ipython().system(' jupyter nbconvert --to script seasonality_final.ipynb')


# In[ ]:


print('start_time',start_time)


# In[ ]:


print("----%s seconds----" %(time.time()-now))

