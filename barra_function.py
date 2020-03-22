import numpy as np
import pandas as pd
import os
import sqlite3
from jqdatasdk import *
from tqdm import tqdm

#style_factors = ['size','beta','momentum','residual_volatility','non_linear_size','book_to_price_ratio','liquidity','earnings_yield','growth','leverage']
#industry_factors = [str(code) for code in pd.read_csv('sw_l1.csv').iloc[:,0]]

def pure_factor_model(rt,factor_data,style_factors,industry_factors,if_output=False):
    #函数文档：
    '''
    :rt:收益率，Series
    :factor_data:因子暴露，DataFrame
    :style_factors:风格因子类型，list
    :industry_factors:行业因子类型，list
    :if_output:是否将结果输出为csv，Boolean
    :输出为(纯因子组合权重矩阵,因子收益率向量,纯因子组合因子暴露矩阵)(Omega,factors_return,Z_df)
    '''
    cwd = os.path.abspath(os.path.curdir)
    Q = len(style_factors)
    P = len(industry_factors)
    factor_list = ['cfactor']+style_factors+industry_factors    
    mkt_cap = factor_data['market_cap']
    factor_data['cfactor'] = 1
    factor_data = factor_data[factor_list]
    factor_data = factor_data.dropna()
    mkt_cap = mkt_cap.loc[factor_data.index,]
    mkt_cap_weights = np.sqrt(mkt_cap)/np.sum(np.sqrt(mkt_cap)) 
    rt = rt.loc[factor_data.index,]
    N = factor_data.shape[0]
    #1.构建权重矩阵V，残差波动率反比于股票市值平方根
    V = np.zeros((N,N))
    for i in range(N):
        V[i,i]= mkt_cap_weights[i]
    #2.构建限制矩阵，由行业市值加权行业因子收益率和为0
    industry_matrix = np.matrix(factor_data[industry_factors])
    mkt_cap_weights = np.matrix(mkt_cap_weights)
    industry_weights = np.matmul(mkt_cap_weights,industry_matrix)
    R1 = np.identity(Q+P)
    R2 = np.zeros((1,Q+P))
    R2[0,(1+Q):] = -industry_weights[0,:-1]/industry_weights[0,-1] 
    R = np.vstack((R1,R2))
#    print(R.shape)
    #3.因子暴露矩阵
    X = np.matrix(factor_data)
#    print(X.shape)
    #4.计算纯因子投资组合权重矩阵OMEGA
    Omega1 = np.linalg.inv(np.matmul(R.T,np.matmul(X.T,np.matmul(V,np.matmul(X,R)))))
    Omega = np.matmul(R,np.matmul(Omega1,np.matmul(R.T,np.matmul(X.T,V))))
    #5.通过纯因子投资组合权重矩阵和资产收益率计算因子收益率
    rt = np.matrix(rt,dtype=float).T
    factors_return = np.matmul(Omega,rt)
    factors_return = pd.DataFrame(factors_return)
    factors_return = pd.Series(factors_return.iloc[:,0])
    factors_return.index = factor_list
    factors_return.columns = ['factors_return']
    Z = np.matmul(Omega,X)
    Z = pd.DataFrame(Z)
    Z_columns = factor_list
    Z.columns = Z_columns
    Z_df = Z.set_index([Z_columns],drop=False)
    Omega = pd.DataFrame(Omega)
    Omega.index = factor_list  
    if if_output:
        factors_return.to_csv(cwd+'\\'+'factors_return.csv')
        Omega.to_csv(cwd+'\\'+'OMEGA_Mat.csv')
        Z_df.to_csv(cwd+'\\'+'factors_exposer.csv')
    return(Omega,factors_return,Z_df)
    
    
'''
def main():
    factor_data = pd.read_csv('factor_df20191225.csv',index_col=1)
    factor_data = factor_data.iloc[:,1:]
    market_data = pd.read_csv('Market_Price.csv',index_col=0)
    rt = market_data['rt1']    
    Omega,factors_return,_ = pure_factor_model(rt,factor_data,style_factors,industry_factors,if_output=False)
'''










