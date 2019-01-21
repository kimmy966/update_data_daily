# -*- coding: utf8 -*-
"""
Created on Tue Jan 15 09:30:48 2019

@author: yuman.hu
"""

import requests
from virgo import market
from datetime import datetime, timedelta

startDate_default = '2005-12-30'
endDate_default = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')


def get_factorCov_data(from_date, to_date):
    if from_date == to_date:
        trading_days = [market.trading_day(span=-1,date=from_date)]
    else:
        trading_days = market.trading_days(from_date, to_date)
    for trading_day in trading_days:
        try:
            path = 'http://192.168.1.158/virgo/risk/barra_data/FactorCov/factor_cov_{}.pkl'.format(trading_day)
            f =  requests.get(path)
            with open('../data/BarraData/FactorCov/factor_cov_{}.pkl'.format(trading_day), 'wb') as code:
                code.write(f.content)
        except:
            print(trading_day,'get_factorCov_data,error')

def get_speCov_data(from_date, to_date):
    if from_date == to_date:
        trading_days = [market.trading_day(span=-1,date=from_date)]
    else:
        trading_days = market.trading_days(from_date, to_date)
    for trading_day in trading_days:
        try:
            path = 'http://192.168.1.158/virgo/risk/barra_data/SpecificCov/specific_cov_{}.pkl'.format(trading_day)
            f =  requests.get(path)
            with open('../data/BarraData/SpecificCov/specific_cov_{}.pkl'.format(trading_day), 'wb') as code:
                code.write(f.content)
        except:
            print(trading_day,'get_speCov_data,error')

def get_factorExp_data(from_date, to_date):
    if from_date == to_date:
        trading_days = [market.trading_day(span=-1,date=from_date)]
    else:
        trading_days = market.trading_days(from_date, to_date)
    for trading_day in trading_days:
        try:
            path = 'http://192.168.1.158/virgo/risk/barra_data/FactorExposure/csv/{}_FactorExposure_000000.csv'.format(trading_day)
            f =  requests.get(path)
            with open('../data/BarraData/FactorExposure/csv/{}_FactorExposure_000000.csv'.format(trading_day), 'wb') as code:
                code.write(f.content)
            path = 'http://192.168.1.158/virgo/risk/barra_data/FactorExposure/pkl/{}_FactorExposure_000000.pkl'.format(trading_day)
            f =  requests.get(path)
            with open('../data/BarraData/FactorExposure/pkl/{}_FactorExposure_000000.pkl'.format(trading_day), 'wb') as code:
                code.write(f.content)
        except:
            print(trading_day,'get_factorExp_data,error')

def update_today(startDate=startDate_default,endDate=endDate_default):

    f =  requests.get('http://192.168.1.158/virgo/risk/barra_data/FactorReturn_CNE5_T+1_000000.csv')
    with open('../data/BarraData/FactorReturn_CNE5_T+1_000000.csv', 'wb') as code:
        code.write(f.content)
    f =  requests.get('http://192.168.1.158/virgo/risk/barra_data/Residual_CNE5_T+1_000000.csv')
    with open('../data/BarraData/Residual_CNE5_T+1_000000.csv', 'wb') as code:
        code.write(f.content)
    f =  requests.get('http://192.168.1.158/virgo/risk/barra_data/T_Value_CNE5_T+1_000000.csv')
    with open('../data/BarraData/T_Value_CNE5_T+1_000000.csv', 'wb') as code:
        code.write(f.content)

    get_factorCov_data(startDate, endDate)
    get_speCov_data(startDate, endDate)
    get_factorExp_data(startDate, endDate)

if __name__ == '__main__':
    update_today(endDate_default,endDate_default)