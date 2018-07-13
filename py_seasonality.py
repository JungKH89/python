
# coding: utf-8

# In[38]:


import math
import numpy as np
import pandas as pd
from sqlalchemy import create_engine ,types


# In[81]:


ma_section1=17
ma_section2=9


# In[57]:


# DB 커넥션 열기
engine1 = create_engine('oracle+cx_oracle://kopo:kopo@192.168.110.112:1521/orcl') 
#engine2 = create_engine('postgresql://kopo:kopo@192.168.110.111:5432/kopo')
# DB 테이블을 읽어 Data Frame 변수에 저장하기
customerData = pd.read_sql_query('SELECT * FROM KOPO_CHANNEL_SEASONALITY_NEW WHERE SUBSTR(YEARWEEK,5,2) <= 52', engine1) 
paramData = pd.read_sql_query('SELECT * FROM kopo_parameter_omz', engine1)
# paramData = pd.read_sql_query('SELECT * FROM kopo_parameter_omz where use_yn="Y"', engine1)


# In[40]:


customerData.columns = [x.upper() for x in customerData.columns]
# 정렬
customerData=customerData.sort_values(["REGIONID","PRODUCT","YEARWEEK"], ascending=[True,True,True])
# 인덱스 다시 설정
sortedData = customerData.reset_index(drop=True)


# # 파라미터 테이블

# In[58]:


paramData.set_index("param_name",inplace=True)


# In[59]:



# In[66]:


ma_section1=int(paramData.loc['MA_SECTION1']['param_value'])


# In[68]:


ma_section2=int(paramData.loc['MA_SECTION2']['param_value'])


# In[ ]:


# 이건 딕셔너리 형태일 때 
# dic=[]
# num=len(paramData)
# for i in range(0,num):
#     a={paramData['param_name'][i]:paramData['param_value'][i]}
#     dic.append(a)
# dic


# #### postgres에는 테이블명 소문자로

# In[69]:


table_name=paramData.loc['SAVE_TABLE_NAME']['param_value'].lower()


# In[70]:


# 컬럼명 대문자
customerData.columns = [x.upper() for x in customerData.columns]


# In[71]:


# 정렬
customerData=customerData.sort_values(["REGIONID","PRODUCT","YEARWEEK"], ascending=[True,True,True])


# In[72]:


# 인덱스 다시 설정
sortedData = customerData.reset_index(drop=True)


# # YEAR랑 WEEK만들기

# In[73]:


sortedData['YEAR']=sortedData['YEARWEEK'].str[0:4]
sortedData['WEEK']=sortedData['YEARWEEK'].str[5:6]



# In[ ]:


# list=[]
# num=len(sortedData)
# for i in range(0,num):
#     a=sortedData['YEARWEEK'][i]
#     sortedData.loc[i,'YEAR']=a[0:4]
#     sortedData.loc[i,'WEEK']=a[5:6]


# ### 음수(반품)는 0으로 고정

# In[74]:


sortedData["QTY_NEW"] = np.where(sortedData["QTY"] <= 0, 1, sortedData["QTY"])


# ### 이동평균(판매추세량) 함수

# In[80]:


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


# ### 정제된 판매량의 이동평균 함수

# In[76]:


# 앞뒤 자동으로 채워줌
# sortedData['MA']=sortedData['QTY'].rolling(window=4, min_periods=1, center=True).mean()


# In[15]:


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


# ### 변동률(판매추세량의 표준편차) 함수

# In[77]:


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


# #### 이동평균(판매추세량) 구하기

# In[78]:


groupResult = sortedData.groupby(['REGIONID','PRODUCT']).apply(sub_function)
aa=groupResult.reset_index(drop=True)


# In[79]:



# #### 변동률 구하기

# In[19]:


bb = aa.groupby(['REGIONID','PRODUCT']).apply(std_function)


# #### 상한/하한구하기

# In[20]:


bb["UPPER_BOUND"]=bb["MA"]+bb["MSTD"]
bb["LOWER_BOUND"]=bb["MA"]-bb["MSTD"]


# #### 정제된 판매량 구하기

# In[21]:


bb['REFINED_QTY']=np.where(bb['QTY_NEW']>bb['UPPER_BOUND'],bb['MA'], np.where(bb['QTY_NEW']<bb['LOWER_BOUND'],bb['MA'],bb['QTY_NEW']))


# #### 스무딩처리 구하기

# In[22]:

bb=bb.reset_index(drop=True)
cc = bb.groupby(['REGIONID','PRODUCT']).apply(smoo_function)


# In[23]:


dd=cc.reset_index(drop=True)


# ### 계절성지수산출(안정된 시장/ 불안정 시장)
# #### 안정된 시장 = 실제판매량/스무딩처리   || 불안정 시장 = 정제된 판매량/스무딩처리

# In[24]:


dd["STABLE"] = np.where(dd['SMOOTH']==0,dd['QTY_NEW'],dd["QTY_NEW"]/dd["SMOOTH"])
dd["UNSTABLE"] = np.where(dd['SMOOTH']==0,dd['REFINED_QTY'],dd["REFINED_QTY"]/dd["SMOOTH"])


# ### 오라클에 CLOB형태 저장할 경우 시간이 오래걸리기 때문에 object형을 varchar로 저장하도록 옵션 추가해야함 

# In[25]:


to_varchar = {c:types.VARCHAR(dd[c].str.len().max()) for c in dd.columns[dd.dtypes == 'object'].tolist()}
a='SEASONALITY_TEAM1'
dd.to_sql(a, engine1, if_exists='replace', index=False, dtype=to_varchar)


# In[35]:

# a='test_py_0'
# dd.to_sql(a, engine2, if_exists='replace', index=False)


# #### 오라클에는 for문 필요..............

# In[37]:


# resultname='RESULT_PY'
# for i in range(0,len(dd)):
#     a=dd[i:i+1][:] 
#     a.to_sql(resultname, engine, if_exists='append', index=False)

