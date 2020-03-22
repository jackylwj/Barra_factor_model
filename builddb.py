import numpy as np
import pandas as pd
import os
import sqlite3
from jqdatasdk import *
from tqdm import tqdm

#转换函数
def transf_bydate(factor_data):
    df_factor = pd.DataFrame()
    for factor in factor_data:
        df_temp = pd.Series(factor_data[factor].iloc[0,:])
        df_factor[factor] = df_temp
    return df_factor

def download_factor_bydate(db_path,stock_list,factor_list,start_date,end_date,freq):
    conn = sqlite3.connect(db_path)
    date_index = pd.date_range(start_date,end_date,freq=freq)
    date_index = [date.strftime('%Y-%m-%d') for date in date_index]
    date_index = tqdm(date_index)
    try:
        for date in date_index:
            date_index.set_description('Processing %s' % date )
            factor_data = get_factor_values(stock_list_all,factor_list,date,date)
            if len(factor_data)!=0:
                date_short = date[:4]+date[5:7]+date[8:10]
                factor_bydate = transf_bydate(factor_data)
                factor_bydate.to_sql(name=date_short,con=conn,if_exists='replace',index=True)
            else:
                continue
            conn.commit()
        conn.close()
        print('Factors data download success')
    except KeyboardInterrupt:
        conn.close()
        date_index.close()
        print('Database closed')

def download_price_bydate(db_path,stock_list,start_date,end_date,frequency='daily',fq='post'):
    conn_price = sqlite3.connect(db_path)
    Price_data = get_price(stock_list, start_date=start_date, end_date=end_date, frequency=frequency, fq=fq)
    close_data = Price_data['close']
    open_data = Price_data['open']

    time_index = close_data.index[:-1]
    #根据两天的收盘价计算收益率
    rt_cc = close_data.shift(-1)/close_data-1
    #根据第二天开盘和收盘价计算收益率
    rt_oc = close_data/open_data-1
    rt_oc = rt_oc.shift(-1)
    
    for date in time_index:
        price_df = pd.DataFrame()
        df_temp_oc = pd.Series(rt_oc.loc[date,:])
        df_temp_cc = pd.Series(rt_cc.loc[date,:])
        price_df['oc'] = df_temp_oc
        price_df['cc'] = df_temp_cc
        for item in Price_data.items:
            price_df[item] = pd.Series(Price_data[item].loc[date,:])
        date = date.strftime('%Y%m%d')
        price_df.to_sql(name=date,con=conn_price,if_exists='replace',index=True)
        conn_price.commit()
    conn_price.close()
    print('Market data download success')

def download_industry_bydate(db_path,stock_list,industry_factors,start_date,end_date,freq='B'):
    conn_industry = sqlite3.connect(db_path)
    time_index = pd.date_range(start_date,end_date,freq=freq)
    time_index = tqdm(time_index)
    try:
        for date in time_index:
            time_index.set_description('Processing %s' % date.strftime('%Y-%m-%d'))
            industry_df = pd.DataFrame()
            df_temp_ids = get_industry(security=stock_list_all,date=date)
            for stock in stock_list:
                try:
                    stock_industry = df_temp_ids[stock]
                    industry_df.loc[stock,'industry_name'] = stock_industry['sw_l1']['industry_name']
                    industry_df.loc[stock,'industry_code'] = stock_industry['sw_l1']['industry_code']
                except:
                    continue
            industry_df = industry_df.join(pd.get_dummies(industry_df['industry_code']))
            for industry in industry_factors:
                if (industry_df.columns==industry).any():
                    pass
                else:
                    industry_df[industry] = 0
            date = date.strftime('%Y%m%d')
            industry_df.to_sql(name=date,con=conn_industry,if_exists='replace',index=True)
        conn_industry.close()
        print('Industry data download success')
    except KeyboardInterrupt:
        conn_industry.close()
        time_index.close()
        print('all closed')

def merge_database(db_all_path,db_factor_path,db_price_path,db_industry_path):
    conn_factor = sqlite3.connect(db_factor_path)
    conn_price= sqlite3.connect(db_price_path)
    conn_industry = sqlite3.connect(db_industry_path)
    conn_all = sqlite3.connect(db_all_path)
    
    #读取市场数据库中所有表名
    cur = conn_price.cursor()
    cur.execute("SELECT name from sqlite_master where type='table' order by name")
    table_names = cur.fetchall()
    table_names = [table[0] for table in table_names]
    table_names = tqdm(table_names)
    for table in table_names:
        table_names.set_description('Processing ',table)
        sql = 'SELECT * FROM "{}"'.format(table)
        table_factor = pd.read_sql(sql,con=conn_factor)
        table_price = pd.read_sql(sql,con=conn_price)
        table_industry = pd.read_sql(sql,con=conn_industry)
        table_all = pd.merge(table_industry,table_price,how='inner')
        table_all = pd.merge(table_all,table_factor,left_on='index',right_on='code')
        table_all = table_all.drop(['code'],axis=1)
        table_all['cfactor'] = 1
        table_all.to_sql(name=table,con=conn_all,if_exists='replace',index=False)
        conn_all.commit()
    conn_all.close()
    print('Database merged success')
    
def main():
    
    cwd = os.path.abspath(os.path.curdir)
    auth('Username', 'Password')
    get_query_count()
   
    start_date = '2019-01-01'
    end_date = '2019-12-31'
    index_code = '000985.XSHG'
    stock_list = get_index_stocks(index_code,start_date)    
    
    #Part1 下载因子数据
    #数据库路径
    db_factor_path = cwd + '\\' + 'factors_all_2019.sqlite'
    #风格因子列表
    style_factors = ['size','beta','momentum','residual_volatility','non_linear_size','book_to_price_ratio','liquidity','earnings_yield','growth','leverage']
    market_cap = ['market_cap']
    factor_list = market_cap + style_factors
    #下载因子数据
    download_factor_bydate(db_factor_path,stock_list,factor_list,start_date,end_date,freq='B')

    #Part2 下载价量数据
    #数据库路径
    db_price_path = cwd + '\\' + 'Market_all_2019.sqlite'
    #下载价量数据
    download_price_bydate(db_price_path,stock_list,start_date,end_date,frequency='daily',fq='post')
    
    #Part3 下载行业信息数据
    #数据库路径
    db_industry_path = cwd + '\\' + 'industry_all_2019.sqlite'
    #申万一级行业分类
    industry_factors = [str(code) for code in pd.read_csv('sw_l1.csv').iloc[:,0]]    
    #下载行业因子数据
    download_industry_bydate(db_industry_path,stock_list,industry_factors,start_date,end_date,freq='B')

    #Part4 将三个数据库合并为一个数据库
    #获取三个数据库和合并数据库路径
    db_factor_path = cwd + '\\' + 'factors_all_2019.sqlite'
    db_price_path = cwd + '\\' + 'Price_all_2019.sqlite'
    db_industry_path = cwd + '\\' + 'industry_all_2019.sqlite'
    db_all_path = cwd + '\\' + 'AllData_2019.sqlite'
    #合并数据库
    merge_database(db_all_path,db_factor_path,db_price_path,db_industry_path)

if __name__ == '__main__':
    main()












