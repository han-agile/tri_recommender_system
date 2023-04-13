#Cluster_recommender_read


import os
import sys
import json
import time
import datetime

import numpy as np
import pandas as pd

from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from dateutil.relativedelta import relativedelta

from collections import defaultdict

save_path = './'

read_query = 'sql구문을 읽어오는 기능의 함수'

def read(self):
    #####################################
    #데이터별 테이블 로딩 및 DATAFRAME화#
    #####################################
    
    #인구통계정보 테이블
    sql_고객정보 = '''
        SELECT 
        고객번호,
        고객정보컬럼
        FROM 고객정보테이블
        WHERE 고객번호 != '-'
        '''
    df_고객정보 = read_query(sql_고객정보)
    df_고객정보 = df_고객정보.repartition(100)
    pdf_고객정보 = df_고객정보.toPandas()
    pdf_고객정보.to_csv(save_path + "/csv/Cluster_Read_1.csv", index=False)
    
    
    #은행계좌원장 테이블
    sql_은행자산정보 = '''
        SELECT
        고객번호,
        은행기관ID,
        은행계좌번호,
        은행외환계좌여부,
        은행상품명,
        상품군구분코드
        FROM 은행자산정보테이블
        WHERE 고객번호 != '-'
        '''
    df_은행자산정보 = read_query(sql_은행자산정보).cache()
    df_은행자산정보 = df_은행자산정보.repartition(100)
    
    
    #대출 최대 평균 선납 테이블
    sql_은행대출정보1 = '''
        SELECT 
        고객번호,
        은행계좌번호,
        은행대출정보
        FROM 은행대출정보테이블1
        WHERE 고객번호 != '-'
        '''
    df_은행대출정보1 = read_query(sql_은행대출정보1)
    df_은행대출정보1 = df_은행대출정보1.repartition(100)
    cut_은행자산정보 = df_은행자산정보.filter((df_은행자산정보.상품군구분코드>3000))
    cut_은행자산정보 = cut_은행자산정보.select('은행계좌번호','은행상품명')
    nm_cut_은행대출정보1 = df_은행대출정보1.join(cut_은행자산정보,on='은행계좌번호').toPandas()
    nm_cut_은행대출정보1.to_csv(save_path + "/csv/Cluster_Read_2.csv", index=False)
    
    sql_은행대출정보2 = '''
        SELECT 
        고객번호,
        은행대출금액
        FROM 은행대출정보테이블2
        WHERE 고객번호 != '-'
        '''
    df_은행대출정보2 = read_query(sql_은행대출정보2)
    df_은행대출정보2 = df_은행대출정보2.repartition(100)
    group_은행대출정보2 = df_은행대출정보2.select('고객번호','은행대출금액')
    cut_은행대출정보2 = group_은행대출정보2.groupBy('고객번호').sum().toPandas()
    cut_은행대출정보2.to_csv(save_path + "/csv/Cluster_Read_3.csv", index=False)

    #'대출 테이블 읽기 완료 >>>'

    
    #카드 자산 수 소비 대출 테이블
    sql_카드자산정보 = '''
        SELECT
        고객번호,
        카드번호,
        카드상품명
        FROM 카드자산정보테이블
        WHERE 고객번호 != '-'
        '''
    df_카드자산정보 = read_query(sql_카드자산정보)
    df_카드자산정보 = df_카드자산정보.repartition(100)
    cad_cnt_카드자산정보 = df_카드자산정보.groupBy('고객번호').count().toPandas()
    cad_cnt_카드자산정보.to_csv(save_path + "/csv/Cluster_Read_4.csv", index=False)
    
    sql_카드소비정보 = '''
        SELECT
        고객번호,
        카드소비금액
        FROM 카드소비정보테이블
        WHERE 고객번호 != '-'
        '''
    df_카드소비정보 = read_query(sql_카드소비정보)
    df_카드소비정보 = df_카드소비정보.repartition(100)
    cad_spend_amount2_카드소비정보 = df_카드소비정보.groupBy(['고객번호']).sum().toPandas()
    cad_spend_amount2_카드소비정보.to_csv(save_path + "/csv/Cluster_Read_5.csv", index=False)
    
    sql_카드단기대출정보 = '''
        SELECT
        고객번호,
        카드단기대출금액
        FROM 카드단기대출정보테이블
        WHERE 고객번호 != '-'
        '''
    df_카드단기대출정보 = read_query(sql_카드단기대출정보)
    df_카드단기대출정보 = df_카드단기대출정보.repartition(100)
    
    cut_카드단기대출정보 = df_카드단기대출정보.groupBy('고객번호').sum().toPandas()
    cut_카드단기대출정보.to_csv(save_path + "/csv/Cluster_Read_6.csv", index=False)
    
    sql_카드장기대출정보 = '''
        SELECT
        고객번호,
        카드장기대출금액
        FROM 카드장기대출정보테이블
        WHERE 고객번호 != '-'
        '''
    df_카드장기대출정보 = read_query(sql_카드장기대출정보)
    df_카드장기대출정보 = df_카드장기대출정보.repartition(100)
    
    cut_카드장기대출정보 = df_카드장기대출정보.groupBy('고객번호').sum().toPandas()
    cut_카드장기대출정보.to_csv(save_path + "/csv/Cluster_Read_7.csv", index=False)
    
    #'카드 테이블 읽기 완료 >>>'
    
    
    #앱 사용량 상위 페이지 테이블
    sql_유저로그정보 = '''
        SELECT
        고객번호,
        방문세션ID,
        방문페이지ID
        FROM 유저로그정보테이블
        WHERE 고객번호 != '-'
        '''
    df_유저로그정보 = read_query(sql_유저로그정보)
    df_유저로그정보 = df_유저로그정보.repartition(100)
    group_유저로그정보 = df_유저로그정보.select('고객번호','방문세션ID').distinct()
    cnt_sess_유저로그정보 = group_유저로그정보.groupBy('고객번호').count().toPandas()
    print("cnt_sess_유저로그정보 spark load")
    cnt_sess_유저로그정보.to_csv(save_path + "/csv/Cluster_Read_8.csv", index=False)

    sql_앱페이지정보 = '''
        SELECT
        관리메뉴ID,
        방문페이지카테고리명
        FROM 앱페이지정보테이블
        '''
    df_앱페이지정보 = read_query(sql_앱페이지정보)
    df_앱페이지정보 = df_앱페이지정보.repartition(100)
    df_앱페이지정보 = df_앱페이지정보.select(
        F.col('관리메뉴ID').alias('방문페이지ID'),
        F.col('방문페이지카테고리명')
    )
    df_유저로그정보_PL = df_유저로그정보.filter(df_유저로그정보.유저행동구분코드 == 'PL')
    df_유저로그정보_PL = df_유저로그정보_PL.join(df_앱페이지정보,on='방문페이지ID')
    group_유저로그정보 = df_유저로그정보_PL.select('고객번호','방문페이지카테고리명','방문페이지ID')
    most_page_유저로그정보 = group_유저로그정보.groupBy('고객번호','방문페이지카테고리명').count().toPandas()
    ##print("most_page_유저로그정보 spark load")
    most_page_유저로그정보.to_csv(save_path + "/csv/Cluster_Read_9.csv", index=False)


    #'앱 사용량 테이블 읽기 완료 >>>'


    #수신 자산 수 양 테이블
    sql_수신자산정보 = '''
        SELECT
        은행계좌번호,
        수신계좌잔액,
        수신계좌금액
        FROM 수신자산정보테이블
        '''
    df_수신자산정보 = read_query(sql_수신자산정보)
    df_수신자산정보 = df_수신자산정보.repartition(100)
    
    cut_은행자산정보 = df_은행자산정보.filter((df_은행자산정보.상품군구분코드==1001)|(df_은행자산정보.상품군구분코드==1002)|(df_은행자산정보.상품군구분코드==1003))

    sel_은행자산정보 = cut_은행자산정보.select('고객번호',
        '은행계좌번호',
        '은행상품명',
        '상품군구분코드')

    cut_수신자산정보 = df_수신자산정보.select(
        '은행계좌번호',
        '수신계좌잔액',
        '수신계좌금액')

    df_은행자산정보2 = cut_수신자산정보.join(sel_은행자산정보,on='은행계좌번호').toPandas()
    df_은행자산정보2.to_csv(save_path + "/csv/Cluster_Read_10.csv", index=False)
    
    #'수신 테이블 읽기 완료 >>>'
    
    
    #방카 자산 수 양 테이블
    sql_보험상세정보 = '''
        SELECT
        고객번호,
        보험상품관리번호,
        은행기관ID,
        보험상품정보
        FROM 보험상세정보테이블
        '''
    df_보험상세정보 = read_query(sql_보험상세정보)
    df_보험상세정보 = df_보험상세정보.repartition(100)        
    cut_보험상세정보 = df_보험상세정보.toPandas()
    cut_보험상세정보.to_csv(save_path + "/csv/Cluster_Read_11.csv", index=False)
    
    sql_보험자산정보 = '''
        SELECT
        고객번호,
        보험상품관리번호
        FROM 보험자산정보테이블
        '''
    df_보험자산정보 = read_query(sql_보험자산정보)
    df_보험자산정보 = df_보험자산정보.repartition(100)        
    ASET_CNT_보험자산정보 = df_보험자산정보.groupBy('고객번호').count().toPandas()
    ASET_CNT_보험자산정보.to_csv(save_path + "/csv/Cluster_Read_12.csv", index=False)
    
    #'방카 테이블 읽기 완료 >>>'
    
    
    #펀드 자산 수 양 테이블
    sql_펀드자산정보 = '''
        SELECT
        고객번호,
        은행계좌번호,
        펀드계좌평가금액
        FROM 펀드자산정보테이블
        '''
    df_펀드자산정보 = read_query(sql_펀드자산정보)
    df_펀드자산정보 = df_펀드자산정보.repartition(100) 
    group_펀드자산정보 = df_펀드자산정보.select('고객번호','은행계좌번호')
    cnt_펀드자산정보 = group_펀드자산정보.groupBy('고객번호').count().toPandas()
    group_펀드자산정보 = df_펀드자산정보.select('고객번호','펀드계좌평가금액')
    amount_펀드자산정보 = group_펀드자산정보.groupBy('고객번호').sum().toPandas()
    cnt_펀드자산정보.to_csv(save_path + "/csv/Cluster_Read_13.csv", index=False)
    amount_펀드자산정보.to_csv(save_path + "/csv/Cluster_Read_14.csv", index=False)
    
    #'펀드 테이블 읽기 완료 >>>'
    
    
    #외환 자산 수 양 테이블
    sql_수신외환자산정보 = '''
        SELECT
        은행계좌번호,
        수신계좌잔액,
        수신계좌금액
        FROM 수신외환자산정보테이블
        '''
    df_수신외환자산정보 = read_query(sql_수신외환자산정보)
    df_수신외환자산정보 = df_수신외환자산정보.repartition(100)
    cut_은행자산정보 = df_은행자산정보.filter((df_은행자산정보.은행외환계좌여부=='Y'))
    cut_은행자산정보 = cut_은행자산정보.select('고객번호',
        '은행계좌번호',
        '은행상품명',
        '상품군구분코드')
    cut_수신외환자산정보 = df_수신외환자산정보.select('은행계좌번호','수신계좌잔액')
    pdf_은행자산정보_수신외환자산정보 = cut_은행자산정보.join(cut_수신외환자산정보,on='은행계좌번호').toPandas()
    pdf_은행자산정보_수신외환자산정보.to_csv(save_path + "/csv/Cluster_Read_15.csv", index=False)
    
    fract_group_은행자산정보 = cut_은행자산정보.groupBy('고객번호').count().toPandas()
    fract_group_은행자산정보.to_csv(save_path + "/csv/Cluster_Read_16.csv", index=False)
    
    sql환율정보 = '''
        SELECT
        환율기준일자,
        통화코드,
        환율정보,
        환율비율,
        달러기준일
        FROM SMDDSMDA4345TG
        '''
    df환율정보 = read_query(sql환율정보)
    df환율정보 = df환율정보.repartition(100) 
    exchange환율정보 = df환율정보.toPandas()
    exchange환율정보.to_csv(save_path + "/csv/Cluster_Read_17.csv", index=False)
    
    #'외환 테이블 읽기 완료 >>>'
    
    
    #IRP 자산 수 양 테이블
    sql_IRP자산정보 = '''
        SELECT
        고객번호,
        IRP관리번호
        FROM IRP자산정보테이블
        '''
    df_IRP자산정보 = read_query(sql_IRP자산정보)
    df_IRP자산정보 = df_IRP자산정보.repartition(100)        
    prd_cnt_IRP자산정보 = df_IRP자산정보.groupBy('고객번호').count().toPandas()
    #print("prd_cnt_IRP자산정보 spark load")
    prd_cnt_IRP자산정보.to_csv(save_path + "/csv/Cluster_Read_18.csv", index=False)
    
    sql_IRP자산량정보 = '''
        SELECT
        IRP관리번호,
        IRP평가금액,
        FROM IRP자산량정보테이블
        '''
    df_IRP자산량정보 = read_query(sql_IRP자산량정보)
    df_IRP자산량정보 = df_IRP자산량정보.repartition(100)  
    cut_IRP자산량정보 = df_IRP자산량정보.select('MYDT_IRP_ACNO','MYDT_IRP_ACT_EVL_AM')
    df_IRP자산정보1 = df_IRP자산정보.join(cut_IRP자산량정보,on='MYDT_IRP_ACNO')
    group_IRP자산정보1 = df_IRP자산정보1.select('고객번호','MYDT_IRP_ACT_EVL_AM')
    aset_IRP자산정보1 = group_IRP자산정보1.groupBy('고객번호').sum().toPandas()
    aset_IRP자산정보1.to_csv(save_path + "/csv/Cluster_Read_19.csv", index=False)
    
    #'IRP 테이블 읽기 완료 >>>'
    
    #'Read module 완료 >>>'
        
    return {}

#ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

#Cluster_recommender_preprocess

def preprocess(self):
    
    Read_data = []

    for i in range(1,19):
        Read_data[i] = pd.read_csv(save_path + '/csv/Cluster_Read_{}.csv'.format(i))

    #'read data loading 완료 >>>'

    ###################################
    #데이터별 취합 및 전처리 부분 시작#
    ###################################
    
    #인구통계정보 데이터 처리
    from datetime import datetime

    def bday_to_age(x):
        b = datetime.strptime(x, '%Y%m%d').date()
        today = datetime.now().date()
        year = today.year - b.year
        if today.month < b.month or (today.month == b.month and today.day < b.day):
            year -= 1
        return year
    
    Read_data[1]['고객생년'] = Read_data[1]['고객생년'].astype(str)
    Read_data[1]['고객나이'] = Read_data[1]['고객생년'].apply(bday_to_age)
    
    Read_data[1]['우편번호'] = Read_data[1]['우편번호'].astype(str)

    #우편번호가 5자리인 경우와 6자리 이상인 경우를 나누어 지역을 구분    
    cut_2020 = Read_data[1].loc[Read_data[1].우편번호.str.len() == 5]
    cut_6over = Read_data[1].loc[Read_data[1].우편번호.str.len() >= 6]
    
    cut_2020['우편번호'] = cut_2020['우편번호'].astype(str)
    
    zip50 = cut_2020.loc[cut_2020.우편번호.str[:1] == '0']
    zip50['우편번호코드'] = 0
    zip51 = cut_2020.loc[cut_2020.우편번호.str[:1] == '1']
    zip51['우편번호코드'] = 1
    zip52 = cut_2020.loc[cut_2020.우편번호.str[:1] == '2']
    zip52['우편번호코드'] = 2
    zip53 = cut_2020.loc[cut_2020.우편번호.str[:1] == '3']
    zip53['우편번호코드'] = 3
    zip54 = cut_2020.loc[cut_2020.우편번호.str[:1] == '4']
    zip54['우편번호코드'] = 4
    zip55 = cut_2020.loc[cut_2020.우편번호.str[:1] == '5']
    zip55['우편번호코드'] = 5
    zip56 = cut_2020.loc[cut_2020.우편번호.str[:1] == '6']
    zip56['우편번호코드'] = 6
    zip57 = cut_2020.loc[cut_2020.우편번호.str[:1] == '7']
    zip57['우편번호코드'] = 7

    zip61 = cut_6over.loc[cut_6over.우편번호.str[:1] == '1']
    zip61['우편번호코드'] = 0
    zip62 = cut_6over.loc[cut_6over.우편번호.str[:1] == '2']
    zip62['우편번호코드'] = 2
    zip63 = cut_6over.loc[cut_6over.우편번호.str[:1] == '3']
    zip63['우편번호코드'] = 3
    zip64 = cut_6over.loc[cut_6over.우편번호.str[:1] == '4']
    zip64['우편번호코드'] = 1
    zip65 = cut_6over.loc[cut_6over.우편번호.str[:1] == '5']
    zip65['우편번호코드'] = 5
    zip66 = cut_6over.loc[cut_6over.우편번호.str[:1] == '6']
    zip66['우편번호코드'] = 4
    zip67 = cut_6over.loc[cut_6over.우편번호.str[:1] == '7']
    zip67['우편번호코드'] = 7

    population_stat = pd.concat([zip50,zip51,zip52,zip53,zip54,zip55,zip56,zip57,zip61,zip62,zip63,zip64,zip65,zip66,zip67],axis=0)
    population_stat_result = population_stat.drop(['고객생년','우편번호','주소텍스트정보'],axis=1)
    population_stat_result = population_stat_result.reset_index(drop=True)
    population_stat_result2 = population_stat_result.iloc[population_stat_result.고객번호.drop_duplicates().index]
    
    POP = population_stat_result2.copy()
    
    #'인구통계 데이터 전처리 완료 >>>'
    
    
    #대출 최대 평균 선납량 데이터 처리
    Read_data[2].은행대출정보 = Read_data[2].은행대출정보.astype(int)
    max_대출정보 = Read_data[2].groupby('고객번호')['은행대출정보'].max().reset_index()

    Read_data[2].은행대출정보 = Read_data[2].은행대출정보.astype(int)
    avg_대출정보 = Read_data[2].groupby('고객번호')['은행대출정보'].mean().reset_index()

    max_avg_대출정보 = pd.merge(max_대출정보,avg_대출정보,on='고객번호')
    max_avg_대출정보.columns=['고객번호','MAX_LOAN','AVG_LOAN']

    loan_max_avg_prepay = pd.merge(max_avg_대출정보,Read_data[3],how='outer',on='고객번호')
    
    LOAN = loan_max_avg_prepay.copy()
    
    #'대출 데이터 전처리 완료 >>>'
    
    
    #앱 사용량 상위 페이지 데이터 처리
    cnt1 = Read_data[8].loc[Read_data[8]['count'] == 1]
    cnt2 = Read_data[8].loc[Read_data[8]['count'] == 2]
    cnt34 = Read_data[8].loc[(Read_data[8]['count'] == 3)|(Read_data[8]['count'] == 4)]
    cnt_over4 = Read_data[8].loc[Read_data[8]['count'] > 4]

    cnt1['sessCD'] = 1
    cnt2['sessCD'] = 2
    cnt34['sessCD'] = 3
    cnt_over4['sessCD'] = 4

    cnt_total = pd.concat([cnt1,cnt2,cnt34,cnt_over4],axis=0)
    cnt_total2 = cnt_total.drop('count',axis=1)
    
    Read_data[9] = Read_data[9].loc[Read_data[9].방문페이지카테고리명 != '홈']

    Read_data[9].방문페이지카테고리명 = Read_data[9].방문페이지카테고리명.replace('데이터설정','기타')
    Read_data[9].방문페이지카테고리명 = Read_data[9].방문페이지카테고리명.replace('데이터 설정','기타')
    Read_data[9].방문페이지카테고리명 = Read_data[9].방문페이지카테고리명.replace('서비스 가입','기타')
    Read_data[9].방문페이지카테고리명 = Read_data[9].방문페이지카테고리명.replace('긴급공지 안내','기타')
    Read_data[9].방문페이지카테고리명 = Read_data[9].방문페이지카테고리명.replace('공통','기타')
    Read_data[9].방문페이지카테고리명 = Read_data[9].방문페이지카테고리명.replace('데이터·설정','기타')

    sorted_app_data = Read_data[9].sort_values(by='count').reset_index(drop=True)

    sorted_app_data = sorted_app_data.reset_index(drop=True)

    most_page_per_cus = sorted_app_data.iloc[sorted_app_data.고객번호.drop_duplicates().index]

    most_page_per_cus2 = most_page_per_cus.drop('count',axis=1)
    
    cnt_page_유저로그정보 = pd.merge(cnt_total2,most_page_per_cus2,how='outer',on='고객번호')
    
    APP = cnt_page_유저로그정보.copy()
    
    #'앱 사용량 데이터 전처리 완료 >>>'


    #카드 자산 수 소비 대출
    cad_cnt_amount = pd.merge(Read_data[4],Read_data[5],how='outer',on='고객번호')
    cad_cnt_amount['sum(카드소비량)'] = abs(cad_cnt_amount['sum(카드소비량)'])
    CAD = cad_cnt_amount.copy()
    
    cad_total_loan = pd.merge(Read_data[6],Read_data[7],how='outer',on='고객번호').fillna(0)
    cad_total_loan['cad_total_loan'] = cad_total_loan['sum(카드대출량)'] + cad_total_loan['sum(카드장기대출량)']
    cad_total_loan2 = cad_total_loan.drop(['sum(카드대출량)','sum(카드장기대출량)'],axis=1)
    CAD_LOAN = cad_total_loan2.copy()
    
    #'카드 데이터 전처리 완료 >>>'
    
    
    #수신 자산 수 양
    cnt_수신자산정보 = Read_data[10].groupby(by='고객번호').은행계좌.count().reset_index()

    amount_수신자산정보 = Read_data[10].groupby(by='고객번호').수신계좌금액.sum().reset_index()
    amount_수신자산정보.수신계좌금액 = abs(amount_수신자산정보.수신계좌금액)

    cnt_amount_수신자산정보 = pd.merge(cnt_수신자산정보,amount_수신자산정보,on='고객번호')
    
    SU = cnt_amount_수신자산정보.copy()
    
    #'수신 데이터 전처리 완료 >>>'
    
    
    #방카 자산 수 양 데이터 처리
    sort_보험자산정보 = Read_data[11].sort_values(by=['보험관리번호'],ascending=False)
    sort_보험자산정보 = sort_보험자산정보.reset_index(drop=True)

    max_pid_sq_보험자산정보 = sort_보험자산정보.iloc[sort_보험자산정보.보험관리번호.drop_duplicates().index]
    max_pid_sq_보험자산정보['total_insu'] = max_pid_sq_보험자산정보['보험관리번호'] * max_pid_sq_보험자산정보['MYDT_INSU_ACL_PID_ISFE']
    max_pid_sq_보험자산정보.loc[max_pid_sq_보험자산정보.보험관리번호 == 0]

    cus_insu_aset = max_pid_sq_보험자산정보.groupby('고객번호').total_insu.sum()

    ASET_보험자산정보 = pd.DataFrame(cus_insu_aset)
    ASET_보험자산정보 = ASET_보험자산정보.reset_index()
    ASET_보험자산정보.columns = ['고객번호','INSU_ASET']

    abs_test = ASET_보험자산정보.sort_values(by='INSU_ASET')
    abs_test.INSU_ASET = abs(abs_test.INSU_ASET)
    
    INSU_ASET_amount_cnt = pd.merge(Read_data[12],abs_test,how='left',on='고객번호')
    
    INSU = INSU_ASET_amount_cnt.copy()
    
    #'방카 데이터 전처리 완료 >>>'
    

    #펀드 자산 양 데이터 처리
    cnt_Read_data = pd.merge(Read_data[13],Read_data[14],on='고객번호')
    
    FUND = cnt_Read_data.copy()
    
    #'펀드 데이터 전처리 완료 >>>'
    
    
    #외환 자산 양 데이터 처리
    Read_data[17] = Read_data[17].sort_values(by='환율고시일자').reset_index(drop=True)
    base_환율정보 = Read_data[17].iloc[Read_data[17].CUCD.drop_duplicates(keep='last').index]

    base_환율정보['base'] = base_환율정보['환율비율'] * base_환율정보['USD_환율기준일자']
    base2_환율정보 = base_환율정보.drop(['환율고시일자','환율기준일자','환율비율','USD_환율기준일자'],axis=1)
    base2_환율정보.columns = ['통화코드','base']

    pdf2_수신자산정보 = pd.merge(Read_data[15],base2_환율정보,how='left',on='통화코드')
    pdf2_수신자산정보['FRACT_amount'] = pdf2_수신자산정보.수신계좌금액 * pdf2_수신자산정보.base

    pdf2_수신자산정보 = pdf2_수신자산정보.sort_values(by='기준일자').reset_index(drop=True)
    drop_du_수신자산정보 = pdf2_수신자산정보.iloc[pdf2_수신자산정보.은행계좌.drop_duplicates(keep='last').index]

    KRW_ASET_FRACT = drop_du_수신자산정보.loc[drop_du_수신자산정보.통화코드 == 'KRW']

    KRW_ASET_FRACT2 = KRW_ASET_FRACT.groupby(['고객번호']).수신계좌금액.sum().reset_index()
    KRW_ASET_FRACT2.수신계좌금액 = abs(KRW_ASET_FRACT2.수신계좌금액)
    KRW_ASET_FRACT2.수신계좌금액 = KRW_ASET_FRACT2.수신계좌금액.astype(int)

    NOT_KRW_ASET_FRACT = drop_du_수신자산정보.loc[drop_du_수신자산정보.통화코드 != 'KRW']
    NOT_KRW_ASET_FRACT2 = NOT_KRW_ASET_FRACT.groupby(['고객번호']).FRACT_amount.sum().reset_index()
    NOT_KRW_ASET_FRACT2.columns = ['고객번호','수신계좌금액']

    NOT_KRW_ASET_FRACT2.수신계좌금액 = NOT_KRW_ASET_FRACT2.수신계좌금액.astype(int)

    FRACT_amount = pd.concat([KRW_ASET_FRACT2,NOT_KRW_ASET_FRACT2],axis=0)

    FRACT_amount2 = FRACT_amount.groupby(by='고객번호').수신계좌금액.sum().reset_index()

    cnt_amount_fract = pd.merge(Read_data[16],FRACT_amount2,how='outer',on='고객번호')

    FRACT = cnt_amount_fract.copy()
    
    #'외환 데이터 전처리 완료 >>>'
    
    
    #IRP 자산 양 데이터 처리
    cnt_Read_data[19] = pd.merge(Read_data[18],Read_data[19],how='outer',on='고객번호')
    
    IRP = cnt_Read_data[19].copy()
    
    #'IRP 데이터 전처리 완료 >>>'
    
    
    ##############################
    #통합 및 클러스터링 부분 시작#
    ##############################
    
    #컬럼 이름 정리
    SU.columns = ['고객번호','SU_cnt','SU_aset']
    IRP.columns = ['고객번호','IRP_cnt','IRP_aset']
    INSU.columns = ['고객번호','INSU_cnt','INSU_aset']
    FUND.columns = ['고객번호','FUND_cnt','FUND_aset']
    FRACT.columns = ['고객번호','FRACT_cnt','FRACT_aset']
    LOAN.columns = ['고객번호','MAX_LOAN','AVG_LOAN','PREPAY_aset']
    CAD_LOAN.columns = ['고객번호','CAD_loan']
    CAD.columns = ['고객번호','CAD_cnt','CAD_aset']
    APP.columns = ['고객번호','APP_sess_cnt','APP_most_page']

    #BASE에 통합
    BASE = POP['고객번호']

    BASE = pd.merge(BASE,SU,how='left',on='고객번호')
    BASE = pd.merge(BASE,IRP,how='left',on='고객번호')
    BASE = pd.merge(BASE,INSU,how='left',on='고객번호')
    BASE = pd.merge(BASE,FUND,how='left',on='고객번호')
    BASE = pd.merge(BASE,FRACT,how='left',on='고객번호')
    BASE = pd.merge(BASE,POP,how='left',on='고객번호')
    BASE = pd.merge(BASE,LOAN,how='left',on='고객번호')
    BASE = pd.merge(BASE,CAD_LOAN,how='left',on='고객번호')
    BASE = pd.merge(BASE,CAD,how='left',on='고객번호')
    BASE = pd.merge(BASE,APP,how='left',on='고객번호')

    BASE = BASE.fillna(0)

    BASE.APP_most_page = BASE.APP_most_page.replace('소비','1')
    BASE.APP_most_page = BASE.APP_most_page.replace('자산','2')
    BASE.APP_most_page = BASE.APP_most_page.replace('플랜','3')
    BASE.APP_most_page = BASE.APP_most_page.replace('기타','4')

    BASE.고객번호 = BASE.고객번호.astype(str)
    
    #다음 모듈에서 사용하기 위해 데이터프레임을 저장
    BASE2.to_csv(save_path + "/csv/Cluster_Preprocess.csv", index=False)
    
    
    #'모델 입력 데이터 전처리 완료 >>>'
    
    #'Preprocess module 완료 >>>'
    

    return {}
#ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

#Cluster_recommender_execute

from sklearn.cluster import KMeans
from sklearn.cluster import DBSCAN
from sklearn.cluster import OPTICS
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn import preprocessing
from sklearn.metrics import silhouette_score

def execute(self):
    #start = time.time()
    #'대표상품 프로세스>>>'
    
    BASE2 = pd.read_csv(save_path + '/csv/Cluster_Preprocess.csv')
    
    #'데이터 로드 완료 >>>'
    
    #전처리된 데이터 스케일링 및 클러스터링 수행
    df_for_cluster = BASE2.copy()
    df_for_cluster = df_for_cluster.reset_index(drop=True)

    scaler = preprocessing.MinMaxScaler()
    df_for_cluster = scaler.fit_transform(df_for_cluster)
    df_for_cluster = pd.DataFrame(df_for_cluster,columns=BASE2.columns)
    df_for_cluster = df_for_cluster.drop('고객번호',axis=1)
    
    #n_cluster를 silhouette_score에 따라서 변화하게 만드는 코드 짜기
    clusterN = 5
    km = KMeans(n_clusters=clusterN, n_jobs=5, random_state=777, max_iter=10000, tol=1e-04, init='k-means++')
    km.fit(df_for_cluster)
    center = km.cluster_centers_
    pred = km.predict(df_for_cluster)

    clust_df = BASE2.copy()
    clust_df['clust'] = pred
    
    #고객별 태그 정보 (자산,부채)
    clust_df['ASET'] = clust_df['SU_aset'] + clust_df['IRP_aset'] + clust_df['INSU_aset'] + clust_df['FUND_aset'] + clust_df['FRACT_aset'] + clust_df['CAD_aset']
    clust_df['DEBT'] = clust_df['MAX_LOAN'] + clust_df['AVG_LOAN'] + clust_df['PREPAY_aset'] + clust_df['CAD_loan']

    #클러스터구분
    clust = []
    for i in range(clusterN):
        clust[i] = clust_df.loc[clust_df.clust == i]


    #클러스터별 필요 컬럼 선택
    cut_for_tag = []
    for i in range(clusterN):
        cut_for_tag[i] = clust_df.loc[clust_df.clust == i]

    #클러스터별 평균값 계산
    clust_mean = []
    for i in range(clust):
        clust_mean[i] = clust[i].mean()

    #클러스터 평균값 통합
    clust_mean_concat = pd.concat([clust_mean],axis=1)

    #자산,부채 랭크
    aset_rank = clust_mean_concat.transpose().ASET.rank()
    debt_rank = clust_mean_concat.transpose().DEBT.rank()

    #자산 태그 부여
    for i in range(clust):
        clust[i]['ASET_RANK'] = aset_rank[i]

    #부채 태그 부여
    for i in range(clust):
        clust[i] = debt_rank[i]

    clust_mean_concat_transpose = clust_mean_concat.transpose()

    #성별 이상치 제거
    clust_mean_concat_transpose.고객성별.loc[clust_mean_concat_transpose.고객성별 < 1.1] = 1
    clust_mean_concat_transpose.고객성별.loc[clust_mean_concat_transpose.고객성별 > 1.9] = 2
    clust_mean_concat_transpose.고객성별.loc[(clust_mean_concat_transpose.고객성별 > 1.1)&(clust_mean_concat_transpose.고객성별 < 1.9)] = 3

    #성별 태그 부여
    for i in range(clust):
        clust[i]['CUS_SEX_TAG'] = clust_mean_concat_transpose.고객성별[i]
    
    clust_merge_for_tag = pd.concat([clust],axis=0)
    
    clust_tag_match_df = clust_merge_for_tag[['clust','ASET_RANK','DEBT_RANK','CUS_SEX_TAG']]
    clust_tag_match_df = clust_tag_match_df.drop_duplicates()

    clust_merge_for_tag = clust_merge_for_tag.reset_index(drop=True)
    clust_merge_for_tag = clust_merge_for_tag[['고객번호','ASET_RANK','DEBT_RANK','CUS_SEX_TAG']]
    
    tag_result = clust_merge_for_tag.copy()

    from datetime import datetime
    now = datetime.now()

    tag_result['기준일자'] = now.strftime('%Y%m%d')
    tag_result['DB변경_일시월'] = now.strftime('%Y%m%d%H%M')
    tag_result['ETL기준일자'] = now.strftime('%Y%m%d')
    
    tag_result.columns = ['고객번호', '자산태그', '부채태그', '성별태그',
    '기준일자', 'DB변경_일시월', 'ETL기준일자']
    
    tag_result = tag_result[['고객번호','기준일자','ETL기준일자','DB변경_일시월','자산태그','부채태그','성별태그']]

    delete_query_3 = f'''
    DELETE FROM 태그기준추천테이블 
    WHERE 기준일자 = TO_CHAR(SYSDATE,'YYYYMMDD')
    '''
    adw_con.execute_sql(delete_query_3)

    adw_con.insert_data(tag_result,'태그기준추천테이블')
    
    #'태그 정보 적재 완료 >>>'
    
    
    #클러스터별 대표상품 뽑기
    cus_id_df = clust_df.copy()
    
    #'클러스터링 완료 >>>'
    clust_id = []

    for i in range(clust):
        clust_id[i] = cus_id_df.고객번호.loc[cus_id_df.clust == i]

    clust_list = clust_id.copy()

    #은행계좌원장 수신 여신 외화 데이터 처리
    은행상품쿼리 = '''
        SELECT
        고객번호,
        은행기관ID,
        은행계좌번호,
        외환상품여부,
        은행상품명,
        최초가입일시,
        상품군구분코드
        FROM 은행상품테이블
        WHERE 고객번호 != '-'
        '''
    df_대표상품정보 = read_query(은행상품쿼리).cache()
    df_대표상품정보 = df_대표상품정보.repartition(100)      
    
    cut_대표상품정보 = df_대표상품정보.filter((df_대표상품정보.상품군구분코드==1001))
    cut_대표상품정보 = cut_대표상품정보.select('고객번호','은행기관ID','최초가입일시','은행상품명')
    prd_대표상품정보 = cut_대표상품정보.groupBy(['은행상품명']).count().toPandas()
    pdf_대표상품정보 = cut_대표상품정보.groupBy(['고객번호','최초가입일시','은행상품명']).count().toPandas()
    상품군별_대표상품정보 = pdf_대표상품정보[pdf_대표상품정보.은행상품명.isin(list(prd_대표상품정보.loc[prd_대표상품정보['count'] > 2].은행상품명))]

    상품군별_대표상품정보 = 상품군별_대표상품정보.loc[상품군별_대표상품정보.최초가입일시 != 'null']
    상품군별_대표상품정보.최초가입일시 = 상품군별_대표상품정보.최초가입일시.astype(int)

    sort_대표상품정보 = 상품군별_대표상품정보.sort_values(by='최초가입일시').reset_index(drop=True)
    sort_대표상품정보 = sort_대표상품정보.loc[sort_대표상품정보.최초가입일시 > 20000000]
    sort_대표상품정보 = sort_대표상품정보.loc[~sort_대표상품정보.은행상품명.str.contains('필터상품')]
    sort_대표상품정보 = sort_대표상품정보.reset_index(drop=True)
    DT_prd_대표상품정보 = sort_대표상품정보.iloc[sort_대표상품정보.은행상품명.drop_duplicates().index]
    DT_prd_대표상품정보 = DT_prd_대표상품정보.drop(['고객번호','count'],axis=1)
    DT_prd_대표상품정보 = DT_prd_대표상품정보.reset_index(drop=True)
    
    상품군별_most_prd_per_clust = []
    for i in range(len(clust_list)):
        sort_대표상품정보.고객번호 = sort_대표상품정보.고객번호.astype(int)
        prd_per_clust = sort_대표상품정보[sort_대표상품정보.고객번호.isin(list(clust_list[i]))]
        상품군별_most_prd_per_clust.append(prd_per_clust.groupby(by='은행상품명').고객번호.count().reset_index().sort_values(by='고객번호',ascending=False).reset_index(drop=True))

    for i in range(len(상품군별_most_prd_per_clust)):
        상품군별_most_prd_per_clust[i] = pd.merge(상품군별_most_prd_per_clust[i],DT_prd_대표상품정보,how='left',on='은행상품명')

    #최근 3개월 날짜 구하여 string으로 반환 > 
    def date_cal(dataframe,date_column):
        max_date = dataframe[date_column].max()
        now = datetime.strptime('{}'.format(max_date),'%Y%m%d')
        before_3_month = now - relativedelta(months=3)
        before_3_month = datetime.strftime(before_3_month,'%Y%m%d')
        return before_3_month
    
    #date_cal(DataFrame,'가입일자')

    #입출식 대표상품
    cut_대표상품정보 = df_대표상품정보.filter((df_대표상품정보.MYDT_ACT_DSCD==1001))
    cut_대표상품정보 = cut_대표상품정보.select('고객번호','은행기관ID','최초가입일시','은행상품명')

    prd_대표상품정보 = cut_대표상품정보.groupBy(['은행상품명']).count().toPandas()
    pdf_대표상품정보 = cut_대표상품정보.groupBy(['고객번호','최초가입일시','은행상품명']).count().toPandas()

    pdf_대표상품정보 = pdf_대표상품정보.loc[~pdf_대표상품정보.은행상품명.str.contains('필터상품')]

    상품군별_대표상품정보 = pdf_대표상품정보[pdf_대표상품정보.은행상품명.isin(list(prd_대표상품정보.loc[prd_대표상품정보['count'] > 2].은행상품명))]
    
    상품군별_대표상품정보 = 상품군별_대표상품정보.loc[상품군별_대표상품정보.최초가입일시 != 'null']
    상품군별_대표상품정보 = 상품군별_대표상품정보.loc[상품군별_대표상품정보.최초가입일시 < '21000000']
    #상품군별_대표상품정보.최초가입일시.max()
    
    ## 이상값 제거
    sort_대표상품정보 = 상품군별_대표상품정보[~상품군별_대표상품정보['최초가입일시'].str.contains('11110101|00010101|00000000')]
    sort_대표상품정보 = sort_대표상품정보.reset_index(drop=True)

    DT_prd_대표상품정보 = sort_대표상품정보.iloc[sort_대표상품정보.은행상품명.drop_duplicates().index]
    DT_prd_대표상품정보 = DT_prd_대표상품정보.drop(['고객번호','count'],axis=1)
    DT_prd_대표상품정보 = DT_prd_대표상품정보.reset_index(drop=True)
    
    #전체시간 최다 판매 상품
    cnt_prd_full_month_대표상품정보 = 상품군별_대표상품정보.groupby(by='은행상품명').고객번호.count()
    cnt_prd_full_month_대표상품정보 = cnt_prd_full_month_대표상품정보.reset_index().sort_values(by='고객번호',ascending=False)
    cnt_prd_full_month_대표상품정보 = cnt_prd_full_month_대표상품정보.reset_index(drop=True)
    cnt_prd_full_month_대표상품정보 = pd.merge(cnt_prd_full_month_대표상품정보, DT_prd_대표상품정보, how='left', on='은행상품명')
    
    #최근 3개월 최다 판매 상품
    prd_3mon_상품군별_대표상품정보 = 상품군별_대표상품정보.loc[상품군별_대표상품정보.최초가입일시 > date_cal(상품군별_대표상품정보,'최초가입일시')]

    cnt_prd_3mon_대표상품정보 = prd_3mon_상품군별_대표상품정보.groupby(by='은행상품명').고객번호.count()
    cnt_prd_3mon_대표상품정보 = cnt_prd_3mon_대표상품정보.reset_index().sort_values(by='고객번호',ascending=False)
    cnt_prd_3mon_대표상품정보 = pd.merge(cnt_prd_3mon_대표상품정보, DT_prd_대표상품정보, how='left', on='은행상품명')
    
    ## 기간별 가중치 부여하여 최종 대표 상품 추출
    #* 상품별 실제 판매량을 기간별 전체 고객 및 상품 수로 나눈 후 가중치 부여 
    #* 최근 3개월 : 전체 기간 = 7 : 3
    cnt_prd_full_month_대표상품정보['가중치값'] = (cnt_prd_full_month_대표상품정보['고객번호']/len(cnt_prd_full_month_대표상품정보))*3
    cnt_prd_3mon_대표상품정보['가중치값'] = (cnt_prd_3mon_대표상품정보['고객번호']/len(cnt_prd_3mon_대표상품정보))*7
    
    상품군별_rpst_prd = pd.concat([cnt_prd_full_month_대표상품정보, cnt_prd_3mon_대표상품정보])
    상품군별_rpst_prd = 상품군별_rpst_prd[['은행상품명', '가중치값']]
    상품군별_rpst_prd = 상품군별_rpst_prd.sort_values(by='가중치값', ascending=False)

    BK_RPRS_PRD3 = 상품군별_rpst_prd[:3]
    BK_RPRS_PRD3.columns = ['은행추천상품명','가중치값']
    
    #write

    delete_query_3 = f'''
            DELETE FROM 대표상품추천테이블 
            WHERE 기준일자 = TO_CHAR(SYSDATE,'YYYYMMDD')
            '''

    adw_con.execute_sql(delete_query_3)

    adw_con.insert_data(BK_RPRS_PRD3,'대표상품추천테이블')

    return {}