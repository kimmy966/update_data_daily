# -*- coding: utf-8 -*-
"""
Created on Tue Jan 15 09:30:48 2019

@author: yuman.hu
"""

import requests
from virgo import market

def get_factorCov_data(from_date, to_date):
    trading_days = market.trading_days(from_date, to_date)
    for trading_day in trading_days:
        path = 'http://192.168.1.158/virgo/risk/barra_data/FactorCov/factor_cov_{}.pkl'.format(trading_day)
        f =  requests.get(path)
        with open('./FactorCov/factor_cov_{}.pkl'.format(trading_day), 'wb') as code:     
            code.write(f.content)
            
get_factorCov_data('2007-01-08','2019-01-14')
            
def get_speCov_data(from_date, to_date):
    trading_days = market.trading_days(from_date, to_date)
    for trading_day in trading_days:
        path = 'http://192.168.1.158/virgo/risk/barra_data/SpecificCov/specific_cov_{}.pkl'.format(trading_day)
        f =  requests.get(path)
        with open('./SpecificCov/specific_cov_{}.pkl'.format(trading_day), 'wb') as code:     
            code.write(f.content)
            
get_speCov_data('2007-01-08','2019-01-14')

def get_factorExp_data(from_date, to_date):
    trading_days = market.trading_days(from_date, to_date)
    for trading_day in trading_days:
        path = 'http://192.168.1.158/virgo/risk/barra_data/FactorExposure/csv/{}_FactorExposure_000000.csv'.format(trading_day)
        f =  requests.get(path)
        with open('./FactorExposure/csv/{}_FactorExposure_000000.csv'.format(trading_day), 'wb') as code:     
            code.write(f.content)
        path = 'http://192.168.1.158/virgo/risk/barra_data/FactorExposure/pkl/{}_FactorExposure_000000.pkl'.format(trading_day)
        f =  requests.get(path)
        with open('./FactorExposure/pkl/{}_FactorExposure_000000.pkl'.format(trading_day), 'wb') as code:     
            code.write(f.content)            
            
get_factorExp_data('2005-12-30','2019-01-14')

f =  requests.get('http://192.168.1.158/virgo/risk/barra_data/FactorReturn_CNE5_T+1_000000.csv')
with open('FactorReturn_CNE5_T+1_000000.csv', 'wb') as code:     
    code.write(f.content)
f =  requests.get('http://192.168.1.158/virgo/risk/barra_data/Residual_CNE5_T+1_000000.csv')
with open('Residual_CNE5_T+1_000000.csv', 'wb') as code:     
    code.write(f.content)
f =  requests.get('http://192.168.1.158/virgo/risk/barra_data/T_Value_CNE5_T+1_000000.csv')
with open('T_Value_CNE5_T+1_000000.csv', 'wb') as code:     
    code.write(f.content)