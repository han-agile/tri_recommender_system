#LGBM_recommender_read

import os
import csv
import sys

save_path = './'

def read(self):
    '''
    sql = select data from table;
    '''
    
    data = ['sql']
    data.to_csv(out_path)

    out_path = "path"

    return {}

#ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

#LGBM_recommender_preprocess

import json
import time
import datetime

import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta

from collections import defaultdict

from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn import preprocessing

def preprocess(self):
    df_all = pd.read_csv.read()

    ###################################
    #데이터별 취합 및 전처리 부분 시작#
    ###################################

    #데이터가 하나도 없는 컬럼 삭제 (scaling시 데이터가 하나도 없어서 오류가 발생하기에 삭제)
    df_col = df_all[df_all.columns[:]]
    filter_ = df_col.notnull().sum()
    fil_df = pd.DataFrame(filter_,columns=['data_CNT'])
    df_all = df_all.drop(fil_df.loc[fil_df.data_CNT == 0].index,axis=1)
        
    # na값 0으로 채우기, 고객 번호 string 변환
    df_all = df_all.fillna(0)
    df_all.고객번호 = df_all.고객번호.astype(str)
    df_all = df_all.reset_index(drop=True)
    
    #생년월일로 나이 재계산
    from datetime import datetime
    def bday_to_age(x):
        b = datetime.strptime(x, '%Y%m%d').date()
        today = datetime.now().date()
        year = today.year - b.year
        if today.month < b.month or (today.month == b.month and today.day < b.day):
            year -= 1
        return year
    
    df_all['CUS_BRDT'] = df_all['CUS_BRDT'].astype(str)
    df_all['CUS_AGE_CN'] = df_all['CUS_BRDT'].apply(bday_to_age)
    
    #고객데이터 테이블 중 오류가 있는 컬럼들을 삭제 혹은 변환 (오류코드)
    for i, v in enumerate(df_all.columns):
        df_all['{}'.format(v)] = df_all['{}'.format(v)].replace('-',0)
        df_all['{}'.format(v)] = df_all['{}'.format(v)].replace(' ',0)

    df_all = df_all.drop(['미리 알 수 있는 오류컬럼'],axis=1)
    
    #유무가 중요한 코드 데이터를 binary 데이터로 바꾸기
    YN_set0 = df_all.loc[df_all.Some_Code_Column == 0]
    YN_set1 = df_all.loc[~(df_all.Some_Code_Column == 0)]
    YN_set0.Some_Code_Column = 0
    YN_set1.Some_Code_Column = 1
    df_all = pd.concat([YN_set0,YN_set1],axis=0)
    
    #전처리된 데이터 저장
    df_all.to_csv("save_path" + "/csv/LGBM_Read.csv", index=False)
    
    return {}

#ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

#LGBM_recommender_train

import pickle

from dateutil.relativedelta import relativedelta

from collections import defaultdict

from lightgbm import LGBMClassifier
from lightgbm import plot_importance
from sklearn.model_selection import train_test_split

import tensorflow as tf
from tensorflow.keras.layers import Input, Dense
from tensorflow.keras.models import Model

def train(self):
    
    #df_all = preprocess(data)
    df_all = pd.read_csv(save_path + '/csv/Read.csv') 
        
    sql_CUS = '''
        SELECT
        고객번호,
        고객정보1,
        ...,
        고객정보N
        FROM 고객테이블
        WHERE 고객번호 != '-'
        '''
    df_cus = read_query(sql_CUS)
    df_cus = df_cus.repartition(100)
    A = df_cus.toPandas()
    
    ###############################
    #고객정보 데이터의 데이터셋 화#
    ###############################
    
    df_all['고객번호'] = df_all['고객번호'].astype(str)
    A['고객번호'] = A['고객번호'].astype(str)
    A['상품군구분코드'] = A['상품군구분코드'].astype(int)

    #은행상품군별 관련 고객 필터링
    Insave1 = A.loc[A['상품군구분코드'] == 1] #자유입출식
    Insave2 = A.loc[A['상품군구분코드'] == 2] #예금
    Insave3 = A.loc[A['상품군구분코드'] == 3] #적금
    Loan1 = A.loc[(A['상품군구분코드'] == 4) | (A['상품군구분코드'] == 5)] #신용대출
    Loan2 = A.loc[A['상품군구분코드']== 6] #담보대출
    Fund = A.loc[A['상품군구분코드'] == 7] #펀드
    Foreign = A.loc[A['상품군구분코드'] == 8] #외화
    CAD = A.loc[A['상품군구분코드'] == 9] #카드
    IRP = A.loc[A['상품군구분코드'] == 10] #IRP
    INSU = A.loc[A['상품군구분코드'] == 11] #보험

    aa = Insave1['고객번호'].drop_duplicates()
    bb = Insave2['고객번호'].drop_duplicates()
    cc = Insave3['고객번호'].drop_duplicates()
    dd = Loan1['고객번호'].drop_duplicates()
    ee = Loan2['고객번호'].drop_duplicates()
    ff = Fund['고객번호'].drop_duplicates()
    gg = Foreign['고객번호'].drop_duplicates()
    hh = IRP['고객번호'].drop_duplicates()
    ii = INSU['고객번호'].drop_duplicates()
    jj = CAD['고객번호'].drop_duplicates()
    
    #'모델 입력 데이터 상품군별 구분 완료'
            
    df_Inout = df_all[df_all.고객번호.isin(aa)]
    df_Insave = df_all[df_all.고객번호.isin(bb)]
    df_Install = df_all[df_all.고객번호.isin(cc)]
    df_Credit = df_all[df_all.고객번호.isin(dd)]
    df_Coll = df_all[df_all.고객번호.isin(ee)]
    df_Fund = df_all[df_all.고객번호.isin(ff)]
    df_Frgn = df_all[df_all.고객번호.isin(gg)]
    df_IRP = df_all[df_all.고객번호.isin(hh)]
    df_INSU = df_all[df_all.고객번호.isin(ii)]
    df_CAD = df_all[df_all.고객번호.isin(jj)]
    
    df_list = [df_Inout,df_Insave,df_Install,df_Credit,df_Coll,df_Fund,df_Frgn,df_IRP,df_INSU,df_CAD]
    
    for i in range(len(df_list)):
        df_list[i] = df_list[i].fillna(0)
        
    df_not_Inout = df_all[~df_all.고객번호.isin(list(df_Inout.고객번호.unique()))]
    df_not_Insave = df_all[~df_all.고객번호.isin(list(df_Insave.고객번호.unique()))]
    df_not_Install = df_all[~df_all.고객번호.isin(list(df_Install.고객번호.unique()))]
    df_not_Credit = df_all[~df_all.고객번호.isin(list(df_Credit.고객번호.unique()))]
    df_not_Coll = df_all[~df_all.고객번호.isin(list(df_Coll.고객번호.unique()))]
    df_not_Fund = df_all[~df_all.고객번호.isin(list(df_Fund.고객번호.unique()))]
    df_not_Frgn = df_all[~df_all.고객번호.isin(list(df_Frgn.고객번호.unique()))]
    df_not_IRP = df_all[~df_all.고객번호.isin(list(df_IRP.고객번호.unique()))]
    df_not_INSU = df_all[~df_all.고객번호.isin(list(df_INSU.고객번호.unique()))]
    df_not_CAD = df_all[~df_all.고객번호.isin(list(df_CAD.고객번호.unique()))]
    
    #'모델 입력 데이터 상품군별 처리 완료'
    
    df_not_list = [df_not_Inout,df_not_Insave,df_not_Install,df_not_Credit,df_not_Coll,df_not_Fund,df_not_Frgn,df_not_IRP,df_not_INSU,df_not_CAD]
    
    for i in range(len(df_list)):
        labels = []
        df_list[i]['label'] = df_list[i]['CUS_AGE_CN']

        for row in df_list[i]['label']:
            if row >= 0:
                labels.append(1)
            else:
                labels.append(1)
        df_list[i]['label'] = labels

    for i in range(len(df_not_list)):
        labels = []
        df_not_list[i]['label'] = df_not_list[i]['CUS_AGE_CN']

        for row in df_not_list[i]['label']:
            if row >= 0:
                labels.append(0)
            else:
                labels.append(0)
        df_not_list[i]['label'] = labels
    
    #상품군별 label 프레임 병합
    binary_dataset = []
    for i in range(len(df_list)):
        binary_dataset.append(pd.concat([df_list[i],df_not_list[i]],axis=0))
    
    #상품군별 스케일링
    for i in range(len(binary_dataset)):
        col_nm = binary_dataset[i].columns
        scaler = preprocessing.MinMaxScaler()
        binary_dataset[i] = scaler.fit_transform(binary_dataset[i])
        binary_dataset[i] = pd.DataFrame(binary_dataset[i],columns=col_nm)
    
    #prediction을 위한 train셋 저장
    df_all2 = df_all.drop('고객번호',axis=1)
    col_nm = df_all2.columns

    scaler = preprocessing.MinMaxScaler()
    df_all2 = scaler.fit_transform(df_all2)
    df_all2 = pd.DataFrame(df_all2 ,columns=col_nm)
    df_all2 = df_all2.drop(['상품군구분코드','삭제할 컬럼들'],axis=1)
    df_all2.to_csv(save_path + "/csv/LGBM_Train.csv", index=False)
    
    #'모델 입력 데이터 셋 구성'
    
    INOUT = binary_dataset[0]
    BIND = binary_dataset[1]
    SAVE = binary_dataset[2]
    SIN = binary_dataset[3]
    DAM = binary_dataset[4]
    FUND = binary_dataset[5]
    FRACT = binary_dataset[6]
    IRP = binary_dataset[7]
    INSU = binary_dataset[8]
    CAD = binary_dataset[9]
    
    #'모델 학습 시작'
        
    #상품군별 모델 함수
    def lgbm_model_func(PRD_CD):

        del_cols = ['공통으로 삭제할 컬럼 미리 정의']

        #data_label_balancing
        if len(PRD_CD.loc[PRD_CD['label'] == 1]) > len(PRD_CD.loc[PRD_CD['label'] == 0]):
            bigger_label = PRD_CD.loc[PRD_CD['label']==1].sample(n=len(PRD_CD.loc[PRD_CD['label']==0]),random_state=915)
            smaller_label = PRD_CD.loc[PRD_CD['label']==0]
        else:
            bigger_label = PRD_CD.loc[PRD_CD['label']==0].sample(n=len(PRD_CD.loc[PRD_CD['label']==1]),random_state=915)
            smaller_label = PRD_CD.loc[PRD_CD['label']==1]

        if PRD_CD == IRP:
            del_cols.append(['IRP모델에서 추가로 삭제 되어야 할 컬럼'])
        elif PRD_CD == INSU:
            del_cols.append(['보험모델에서 추가로 삭제 되어야 할 컬럼'])
        elif PRD_CD == INSU:
            del_cols.append(['카드모델에서 추가로 삭제 되어야 할 컬럼'])
        else:
            print('nothing added on del_cols')

        binary_set = pd.concat([bigger_label,smaller_label],axis=0)
        dataset = binary_set.drop(['고객번호','label','상품군구분코드','저축방법코드']+del_cols, axis=1)
        target = binary_set['label']
        X_train,X_test,y_train,y_test = train_test_split(dataset,target,test_size=0.2,random_state=915)
        lgbm_wrapper = LGBMClassifier(n_estimators=999)
        evals = [(X_test, y_test)]
        lgbm_wrapper.fit(X_train,y_train,early_stopping_rounds=20, eval_metric='logloss',
                        eval_set = evals, verbose = True)
        preds = lgbm_wrapper.predict(X_test)

        pickle.dump(lgbm_wrapper, open(save_path + '/csv/lgbm_model_{}.pkl'.format(PRD_CD), 'wb'))
        y_test_df = pd.DataFrame(y_test)
        y_test_df.to_csv(save_path + '/csv/y_test_{}.csv'.format(PRD_CD), index=False)
        preds_df = pd.DataFrame(preds)
        preds_df.to_csv(save_path + '/csv/preds_{}.csv'.format(PRD_CD), index=False)
            
    PRD_CD = [INOUT,BIND,SAVE,SIN,DAM,FUND,FRACT,IRP,INSU,CAD]
    
    #상품군별 데이터셋을 lgbm_model_func 함수에 넣어서 순차적으로 학습 실행
    for i,v in enumerate(PRD_CD):
        lgbm_model_func(v)
    
    #'상품군 학습 완료'
    
    #write를 위해 각 모델별 학습된 파일을 저장
    
    #'모델 학습 완료'

    return PRD_CD

#ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

#LGBM_recommender_prediction

from lightgbm import LGBMClassifier

from sklearn.metrics import accuracy_score, recall_score, f1_score, precision_score, roc_auc_score
from sklearn.metrics import confusion_matrix

def prediction(self):
    #PRD_CD = train(PRD_CD)
    PRD_CD_NM = ['INOUT','BIND','SAVE','SIN','DAM','FUND','FRACT','IRP','INSU','CAD']
    del_cols2 = ['공통으로 삭제할 컬럼 미리 정의']

    SCALED_DATA = pd.read_csv(save_path + '/csv/LGBM_Train.csv')
    df_all2 = SCALED_DATA.copy()

    results = []

    for i,v in enumerate(PRD_CD_NM):
        y_test = pd.read_csv(save_path + '/csv/y_test_{}.csv'.format(PRD_CD_NM))
        preds = pd.read_csv(save_path + '/csv/preds_{}.csv'.format(PRD_CD_NM))

        #상품군별 모델 불러오기
        lgbm_wrapper = pickle.load(open(save_path + '/csv/lgbm_model_{}.pkl'.format(PRD_CD_NM), 'rb'))

        if PRD_CD_NM == 'IRP':
            del_cols2.append(['IRP모델에서 추가로 삭제 되어야 할 컬럼'])
        elif PRD_CD_NM == 'INSU':
            del_cols2.append(['보험모델에서 추가로 삭제 되어야 할 컬럼'])
        elif PRD_CD_NM == 'CAD':
            del_cols2.append(['카드모델에서 추가로 삭제 되어야 할 컬럼'])
        else:
            print('nothing added on del_cols')

        #상품군별 전체 고객 추론
        df_all3 = df_all2.drop(del_cols2,axis=1)
        preds = lgbm_wrapper.predict(df_all3)

        results[i] = preds

    preds_all = pd.concat(results,axis=1)
    preds_all.columns = ['INOUT','BIND','SAVE','FUND','FRACT','IRP','INSU','SIN','DAM','CAD']
    preds_all.to_csv(save_path + '/csv/LGBM_Prediction.csv', index=False)

    return PRD_CD_NM


#ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

#LGBM_recommender_write

def write():

    PRD_CD_NM = prediction(PRD_CD_NM)
    preds_all = pd.read_csv(save_path + '/csv/LGBM_Prediction.csv')
    cus_id = pd.read_csv(save_path + '/csv/LGBM_Read.csv') 

    #학습 데이터로 쓰기 위해서 drop 했던 고객번호를 다시 추가
    preds_all['고객번호'] = cus_id.고객번호

    result = [0,1,2,3,4,5,6,7,8,9]

    numbering = ['01','02','03','04','05','06','07','08','09','10']

    for i,v in enumerate(PRD_CD_NM):
        result[i] = preds_all.loc[preds_all.PRD_CD_NM == 1]
        result[i]['상품군추천_구분코드'] = numbering[i]

    result_list = [result[i] for i in range(len(result))]
    result_grp = pd.concat(result_list,axis=0)

    from datetime import datetime
    now = datetime.now()

    result_grp['기준일자'] = now.strftime('%Y%m%d')
    result_grp['DB저장_일시분'] = now.strftime('%Y%m%d%H%M')
    result_grp['상품군추천_방법구분코드'] = '01'
    result_grp['ETL기준일자'] = now.strftime('%Y%m%d')
    
    #테이블 컬럼 순서에 맞춰 정렬
    result_grp = result_grp[['고객번호','기준일자','상품군추천_구분코드','ETL기준일자','DB저장_일시분','상품군추천_방법구분코드']]
    
    #테이블적재
    delete_query = f'''
            DELETE FROM 추천결과TABLE 
            WHERE 기준일자 = TO_CHAR(SYSDATE,'YYYYMMDD')
            '''
    adw_con.execute_sql(delete_query)
    
    adw_con.insert_data(result_grp, '추천결과TABLE')

    return {}
#ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

#LGBM_recommender_eval

def eval(self):

    #'상품군 추천 결과 검증 시작'

    PRD_CD_NM = prediction(PRD_CD_NM)

    test_dataset = []
    pred_dataset = []

    for i,v in enumerate(PRD_CD_NM):

        y_test = pd.read_csv(save_path + '/csv/y_test_{}.csv'.format(v))
        preds = pd.read_csv(save_path + '/csv/preds_{}.csv'.format(v))

        test_dataset.append(y_test)
        pred_dataset.append(preds)
    
    def get_clf_eval(y_test, y_pred):
        f1 = f1_score(y_test, y_pred)
        return f1
        
    total_len = []
    score_lst = []
    score_weight_lst = []

    for i in range(len(test_dataset)):
        len_tmp = len(test_dataset[i])
        total_len.append(len_tmp)
        len_df = sum(total_len)

        score_tmp = round(get_clf_eval(test_dataset[i], pred_dataset[i]), 3)
        score_lst.append(score_tmp)

        weight_tmp = round(score_lst[i]*len(test_dataset[i]), 3)
        score_weight_lst.append(weight_tmp)

    weighted_score = sum(score_weight_lst)/len_df
        
    df_score_result = pd.DataFrame({'상품군추천구분코드' : ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10'], 
                                        'F1_SCRE_ORG' : score_lst,
                                        'F1 * LENGTH' : score_weight_lst}).sort_values(by='상품군추천구분코드')
    
    #'상품군 추천 결과 검증 완료'
    
    for idx in range(0, 10):
        print("상품군 {df_score_result.iloc[idx, 0]} - f1 score : {df_score_result.iloc[idx, 1]}")
        
    #검증결과 저장
    df_score_result.to_csv(root_path + "/csv/LGBM_Eval.csv", index=False)
    
    #'전체 상품군 weighted f1 score
    print(weighted_score)
    
    return weighted_score