#CF_recommender read

import os
import sys
import json
import time
import datetime

import numpy as np
import pandas as pd

from collections import defaultdict

save_path = './'

read_query = 'sql구문을 읽어오는 기능의 함수'

def read(self):
    #####################################
    #데이터별 테이블 로딩 및 DATAFRAME화#
    #####################################
    
    #은행 관련 고객
    sql_고객정보 = '''
        SELECT 
        고객번호,
        고객정보컬럼
        FROM 고객정보테이블명
        WHERE 고객번호 != '-'
        '''
    df_고객정보 = read_query(sql_고객정보)
    df_고객정보 = df_고객정보.repartition(100)
    id_고객정보 = df_고객정보.select('고객번호').toPandas()
    
    #'고객수: {len(id_고객정보)}'
    
    sql_은행자산정보 = '''
        SELECT
        고객번호,
        은행자산컬럼,
        상품군구분코드
        FROM 은행자산테이블명
        WHERE 고객번호 != '-'
        '''
    df_은행자산정보 = read_query(sql_은행자산정보).cache()
    df_은행자산정보 = df_은행자산정보.repartition(100)

    #카드 자산 수 소비 대출 테이블
    sql_카드자산정보 = '''
        SELECT
        고객번호,
        카드자산정보
        FROM 카드테이블명
        WHERE 고객번호 != '-'
        '''
    df_카드자산정보 = read_query(sql_카드자산정보)
    df_카드자산정보 = df_카드자산정보.repartition(100)

    #방카 자산 수 양 테이블
    sql_보험자산정보 = '''
        SELECT
        고객번호,
        보험자산정보
        FROM 보험테이블명
        '''
    df_보험자산정보 = read_query(sql_보험자산정보)
    df_보험자산정보 = df_보험자산정보.repartition(100)        
    
    #펀드 자산 수 양 테이블
    sql_펀드자산정보 = '''
        SELECT
        고객번호,
        펀드자산정보,
        펀드구분코드
        FROM 펀드테이블명
        '''
    df_펀드자산정보 = read_query(sql_펀드자산정보)
    df_펀드자산정보 = df_펀드자산정보.repartition(100) 

    #수신/여신/기타자산 함수화
    def deposit(자산구분코드, 상품보유인원제한수, 필터단어):
        cut_은행자산정보 = df_은행자산정보.filter((df_은행자산정보.상품군구분코드==자산구분코드))
        cut_은행자산정보 = cut_은행자산정보.select('고객번호','은행기관ID','은행상품명')
        prd_은행자산정보 = cut_은행자산정보.groupBy(['은행상품명']).count().toPandas()
        pdf_은행자산정보 = cut_은행자산정보.groupBy(['고객번호','은행기관ID','은행상품명']).count().toPandas()
        pdf_은행자산정보 = pdf_은행자산정보.loc[~pdf_은행자산정보.은행상품명.str.contains(필터단어)]
        상품보유정보 = pdf_은행자산정보[pdf_은행자산정보.은행상품명.isin(list(prd_은행자산정보.loc[prd_은행자산정보['count'] > 상품보유인원제한수].은행상품명))]
        상품보유정보 = 상품보유정보[상품보유정보.고객번호.isin(list(id_고객정보.고객번호.unique()))]
        return 상품보유정보
    
    inout_은행자산정보 = deposit(1001,200,'주택청약')
    bind_은행자산정보 = deposit(1002,200,'주택청약')
    save_은행자산정보 = deposit(1003,200,'주택청약')
    
    def loan(자산구분코드하한,자산구분코드상한, 상품보유인원제한수, 필터단어):
        cut_은행자산정보 = df_은행자산정보.filter((df_은행자산정보.상품군구분코드>=자산구분코드하한))
        cut_은행자산정보 = cut_은행자산정보.filter((df_은행자산정보.상품군구분코드<자산구분코드상한))
        cut_은행자산정보 = cut_은행자산정보.select('고객번호','은행기관ID','은행상품명')
        prd_은행자산정보 = cut_은행자산정보.groupBy(['은행상품명']).count().toPandas()
        pdf_은행자산정보 = cut_은행자산정보.groupBy(['고객번호','은행기관ID','은행상품명']).count().toPandas()
        pdf_은행자산정보 = pdf_은행자산정보.loc[~pdf_은행자산정보.은행상품명.str.contains(필터단어)]
        상품보유정보 = pdf_은행자산정보[pdf_은행자산정보.은행상품명.isin(list(prd_은행자산정보.loc[prd_은행자산정보['count'] > 상품보유인원제한수].은행상품명))]
        상품보유정보 = 상품보유정보[상품보유정보.고객번호.isin(list(id_고객정보.고객번호.unique()))]
        return 상품보유정보
    
    SIN_은행자산정보 = loan(3000,3200,100,'주택')
    DAM_은행자산정보 = loan(3200,4000,100,'주택담보대출')
    
    def etc(기타자산종류,상품보유인원제한수):
        if 기타자산종류 == '외환':
            cut_기타자산정보 = df_은행자산정보.filter((df_은행자산정보.외환자산여부=='Y'))
        elif 기타자산종류 == '카드':
            cut_기타자산정보 = df_카드자산정보.select('고객번호','은행기관ID','카드상품명')
            cut_기타자산정보['카드상품명'] == cut_기타자산정보['은행상품명']
        elif 기타자산종류 == '방카':
            cut_기타자산정보 = df_보험자산정보.select('고객번호','은행기관ID','방카상품명')
            cut_기타자산정보['방카상품명'] == cut_기타자산정보['은행상품명']
        elif 기타자산종류 == '펀드':
            cut_기타자산정보 = df_펀드자산정보.filter((df_펀드자산정보.펀드구분코드 == 413)|(df_펀드자산정보.펀드구분코드 == 414)|(df_펀드자산정보.펀드구분코드 == 415))
            cut_기타자산정보 = cut_기타자산정보.groupBy(['고객번호','은행기관ID','펀드상품명']).count().toPandas()
            cut_기타자산정보['펀드상품명'] == cut_기타자산정보['은행상품명']
        else:
            print('기타자산이 아닙니다')

        cut_기타자산정보 = cut_기타자산정보.select('고객번호','은행기관ID','은행상품명')
        prd_기타자산정보 = cut_기타자산정보.groupBy(['은행상품명']).count().toPandas()
        pdf_기타자산정보 = cut_기타자산정보.groupBy(['고객번호','은행기관ID','은행상품명']).count().toPandas()
        기타상품보유정보 = pdf_기타자산정보[pdf_기타자산정보.은행상품명.isin(list(prd_기타자산정보.loc[prd_기타자산정보['count'] > 상품보유인원제한수].은행상품명))]
        기타상품보유정보 = 기타상품보유정보[기타상품보유정보.고객번호.isin(list(id_고객정보.고객번호.unique()))]
        return 기타상품보유정보
    
    fract_은행자산정보 = etc('외환',100)
    CAD_카드자산정보 = etc('카드',800)
    INSU_보험자산정보 = etc('방카',500)
    FUND_펀드자산정보 = etc('펀드',50)
    
    #read한 테이블 정리하여 하나의 데이터프레임으로 합치기
    
    inout_은행자산정보['PRD_GRP'] = 'INOUT'
    bind_은행자산정보['PRD_GRP'] = 'BIND'
    save_은행자산정보['PRD_GRP'] = 'SAVE'
    SIN_은행자산정보['PRD_GRP'] = 'SIN'
    DAM_은행자산정보['PRD_GRP'] = 'DAM'
    fract_은행자산정보['PRD_GRP'] = 'FRACT'
    CAD_카드자산정보['PRD_GRP'] = 'CAD'
    FUND_펀드자산정보['PRD_GRP'] = 'FUND'
    INSU_보험자산정보['PRD_GRP'] = 'INSU'
    
    total_은행자산정보 = pd.concat([inout_은행자산정보,bind_은행자산정보,save_은행자산정보,
                              SIN_은행자산정보,DAM_은행자산정보,fract_은행자산정보,
                              FUND_펀드자산정보,INSU_보험자산정보,CAD_카드자산정보],axis=0)
    
    total_은행자산정보.to_csv(save_path + "/csv/CF_Read.csv", index=False)
        
    #'read process 완료 >>>'

    return total_은행자산정보

#ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

#CF_recommender_preprocess

def preprocess(self):
    total_은행자산정보 = pd.read_csv(save_path + "/csv/CF_Read.csv")

    #'read data loading 완료 >>>'
        
    ###################################
    #데이터별 취합 및 전처리 부분 시작#
    ###################################
    
    #'모델 입력 데이터 전처리 시작 >>>'
    
    #고객 단 1명만 가지고 있는 상품을 제거하기 위한 del1
    del1 = total_은행자산정보.groupby('고객번호').count()
    total_은행자산정보_2 = total_은행자산정보[total_은행자산정보.고객번호.isin(del1.loc[del1.은행상품명 > 2].index)]

    total_은행자산정보_tmp = total_은행자산정보_2.sort_values(by='고객번호')
    id_total_은행자산정보_tmp = total_은행자산정보_tmp.고객번호.drop_duplicates()
    id_total_은행자산정보_tmp = id_total_은행자산정보_tmp.reset_index(drop=True)
    id_total_은행자산정보_tmp = id_total_은행자산정보_tmp.reset_index()
    id_total_은행자산정보_tmp.columns = ['ID','고객번호']

    product = total_은행자산정보_tmp.sort_values(by='은행상품명').은행상품명.drop_duplicates().reset_index(drop=True)
    product = product.reset_index()
    product.columns = ['INDEX','은행상품명']
    
    total_은행자산정보_2 = pd.merge(total_은행자산정보_2,product,how='inner',on='은행상품명')
    total_은행자산정보_2 = pd.merge(total_은행자산정보_2,id_total_은행자산정보_tmp,how='inner',on='고객번호')
    
    total_은행자산정보_2.to_csv(save_path + '/csv/CF_Preprocess.csv',encoding='utf-8-sig',index=False)
    
    #'모델 입력 데이터 전처리 완료 >>>'

    return total_은행자산정보_2


#ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

#CF_recommender_prediction

from sklearn.metrics.pairwise import cosine_similarity

from multiprocessing import Process, Manager, Semaphore, Value, Pool
import multiprocessing
from datetime import datetime

def parallel_scoring(func, args, n_process=30):
    with Pool(n_process) as p:
        results = p.map(func, args)
    return results

def get_item_based_collabor(title):
    #연관도 상위 10개만 남김
    return item_based_collabor_df[title].sort_values(ascending=False)[:11]

def CF_scoring(ID):
    customer1_prd_list = CF_total['INDEX'].loc[CF_total.ID == ID]
    customer1_prd_list = customer1_prd_list.reset_index(drop=True)

    score_save = []
    for i in range(len(customer1_prd_list)):
        score_save.append(get_item_based_collabor(customer1_prd_list[i]))

    df_score = pd.DataFrame()
    for i in range(len(score_save)):
        df_score = pd.concat([df_score,score_save[i]],axis=0)

    df_score.columns = ['score']
    df_score['score'] = df_score['score'].astype(float)

    df_score = df_score.reset_index().sort_values(by='score',ascending=False)
    df_score = df_score.reset_index(drop=True)
    df_score.columns=['PRD_NM','score']
    df_score = df_score.iloc[df_score.PRD_NM.drop_duplicates().index]
    #연관도가 1인 경우는 같은 상품을 가리키니 필터
    df_score = df_score.loc[df_score['score'] < 0.99]

    return df_score.sort_values(by='score',ascending=False).reset_index(drop=True)

def prediction(self):
    #'CF 모델 추론 시작 >>>'
        
    total_은행자산정보 = pd.read_csv(save_path + "/csv/CF_Preprocess.csv")
    total_은행자산정보 = total_은행자산정보.reset_index(drop=True)
    total_은행자산정보_2 = total_은행자산정보.copy()

    #CF매트릭스의 속도 향상을 위해 string인 은행상품명 및 고객번호를 indexing    
    prduct_nm = total_은행자산정보_2[['은행상품명','INDEX']]
    customer_id = total_은행자산정보_2[['고객번호','ID']]
    total_은행자산정보_2.은행기관ID = total_은행자산정보_2.은행기관ID.replace('은행ID1','은행ID2')
    total_은행자산정보_2 = total_은행자산정보_2.sort_values(by='은행기관ID').reset_index(drop=True)
    prd_ist_id = total_은행자산정보_2.iloc[total_은행자산정보_2.은행상품명.drop_duplicates().index].reset_index(drop=True)
    prd_ist_id = prd_ist_id.drop(['고객번호','count','ID','INDEX'],axis=1)
    prd_nm_dropdu = prduct_nm.drop_duplicates().reset_index(drop=True)
    prd_ist_nm = pd.merge(prd_ist_id,prd_nm_dropdu,how='left',on='은행상품명')
    
    
    #CF 매트릭스 생성
    global CF_total
    CF_total = total_은행자산정보_2.copy()
    tmp_piv = CF_total.pivot_table('count',index = 'INDEX',columns = 'ID')
    
    #'CF 모델 매트릭스 생성 >>>'
    
    tmp_piv.fillna(0,inplace=True)
    item_based_collabor = cosine_similarity(tmp_piv)
    global item_based_collabor_df
    item_based_collabor_df = pd.DataFrame(data = item_based_collabor, index = tmp_piv.index, columns = tmp_piv.index)
    
    
    #CF ID 체번
    CF_ID = CF_total.ID.drop_duplicates().reset_index(drop=True)

    #multiprocessing CF모델 수행
    results_whole = parallel_scoring(CF_scoring, args= CF_ID.values[:], n_process=multiprocessing.cpu_count())
    
    #'CF 모델 추론 적용 완료 >>>'
    
    #ID와 통합고객번호를 매칭
    customer_id_dropdu = customer_id.drop_duplicates()
    CF_customer_id = pd.merge(CF_ID,customer_id_dropdu,how='inner',on='ID')
    for i in range(len(CF_customer_id)):
        results_whole[i]['고객번호'] = CF_customer_id['고객번호'][i]
    
    result_concat_whole = pd.concat(results_whole,axis=0)
    
    #INDEX와 상품명을 매칭
    result = pd.merge(result_concat_whole,prd_ist_nm,how='left',left_on='PRD_NM',right_on='INDEX')
    
    #'CF 모델 추론 결과 정리 완료 >>>'
    
    result = result.reset_index(drop=True).reset_index()
    
    #테이블 적재를 위해 테이블에 맞는 컬럼으로 이름 변경 및 필요 요소 채우기
    from datetime import datetime
    now = datetime.now()
    result['기준일자'] = now.strftime('%Y%m%d')
    result['추천번호'] = result.index
    result['DB저장_일시분'] = now.strftime('%Y%m%d%H%M')
    result['ETL기준일자'] = now.strftime('%Y%m%d')
    result['상품군추천_방법구분코드'] = '02'
    result['상품관리번호'] = '11111111'

    result['PRD_GRP'] = result['PRD_GRP'].replace('INOUT','01')
    result['PRD_GRP'] = result['PRD_GRP'].replace('GUCHI','02')
    result['PRD_GRP'] = result['PRD_GRP'].replace('SAVE','03')
    result['PRD_GRP'] = result['PRD_GRP'].replace('FUND','04')
    result['PRD_GRP'] = result['PRD_GRP'].replace('FRACT','05')
    result['PRD_GRP'] = result['PRD_GRP'].replace('IRP','06')
    result['PRD_GRP'] = result['PRD_GRP'].replace('INSU','07')
    result['PRD_GRP'] = result['PRD_GRP'].replace('SIN','08')
    result['PRD_GRP'] = result['PRD_GRP'].replace('DAM','09')
    result['PRD_GRP'] = result['PRD_GRP'].replace('CAD','10')        
    
    result = result.drop('index',axis=1)
    
    result = result[['고객번호','기준일자','상품군추천_구분코드','ETL기준일자','DB저장_일시분','은행상품명','score','상품군추천_방법구분코드','상품관리번호']]
    result.columns = ['고객번호','기준일자','상품군추천_구분코드','ETL기준일자','DB저장_일시분','추천상품명','추천상품스코어','상품군추천_방법구분코드','상품관리번호']
    
    result.to_csv(save_path + "/csv/CF_Prediction.csv", index=False)  
    
    #'CF 모델 추론 완료 >>>'
    
    return result

#ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

#CF_recommender_write

def write(self):
    result = pd.read_csv(save_path + "/csv/CF_Prediction.csv")
    
    #테이블적재
    delete_query_3 = f'''
            DELETE FROM CF추천결과테이블 
            WHERE 기준일자 = TO_CHAR(SYSDATE,'YYYYMMDD')
            '''
    adw_con.execute_sql(delete_query_3)

    adw_con.insert_data(result,'CF추천결과테이블')
            
    #'상세 상품 적재 완료 >>>'

    return {}
