import numpy as np
import pandas as pd
import os
import sqlite3
import matplotlib.pyplot as plt
from jqdatasdk import *
from tqdm import tqdm
from barra_function import pure_factor_model


def main():
    cwd = os.path.abspath(os.path.curdir)
    db_path = cwd + '\\' + 'AllData_2019.sqlite'
    
    style_factors = ['size','beta','momentum','residual_volatility','non_linear_size','book_to_price_ratio','liquidity','earnings_yield','growth','leverage']
    industry_factors = [str(code) for code in pd.read_csv('sw_l1.csv').iloc[:,0]]
    factor_list = ['cfactor'] + style_factors + industry_factors
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE TYPE='table' ORDER BY name")
    tables_name = cur.fetchall()
    tables_name = [table[0] for table in tables_name]
    
    factor_return_dict = dict()
    tables_name = tqdm(tables_name)
    for date in tables_name:
        tables_name.set_description('Processing %s' %date)
        sql = "SELECT * FROM '{}'".format(date)
        table = pd.read_sql(sql,con=conn,index_col='index')
        table['cfactor'] = 1
        rt = table['cc']
        factor_data = table[factor_list]
        _,factor_return,_ = pure_factor_model(rt,factor_data,style_factors,industry_factors,if_output=False)
        factor_return_dict[date] = factor_return
    tables_name.close()
    
    factor_return_df = pd.DataFrame(columns=factor_list)
    for date in factor_return_dict:
        factor_return_df.loc[date,] = factor_return_dict[date]
    
    #style_factor
    style_factors_return = factor_return_df[style_factors]
    style_factors_cum = (style_factors_return+1).cumprod(axis=0)
    style_factors_cum.to_csv('style_factors_cum.csv')
    ax = style_factors_cum.plot()
    fig = ax.get_figure()
    fig.savefig('style_factors_return.png')
    
if __name__ =='__main__':
    main()








