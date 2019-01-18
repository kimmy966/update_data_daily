# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from functools import reduce
import pickle
import os
import pymssql
from virgo import market

today = datetime.now().strftime('%Y-%m-%d')
market.trading_day(date=today)

startDate_default = '20060101'
endDate_default = datetime.now().strftime('%Y%m%d')
tradingDateV = pd.read_csv('../data/rawData/tradingDateV.csv',header=None).values
tickerUnivSR = pd.read_csv('../data/rawData/tickerUnivSR.csv',header=None).values

def fullUpdate(startDate=startDate_default,endDate=endDate_default):
    tradingDays = tradingDateV[(tradingDateV>=int(startDate)).squeeze() & (tradingDateV<=int(endDate)).squeeze()].squeeze()

    preClose_list = []
    open_list = []
    high_list = []
    low_list = []
    close_list = []
    volume_list = []
    amount_list = []
    ret_list = []
    adjFactor_list = []
    listed_list = []
    susp_list = []
    stState_list = []
    hs300ConstC_IndexConst_list = []
    sz50ConstC_IndexConst_list = []
    zz500ConstC_IndexConst_list = []

    for tradingDay in tradingDays:
        try:
            file2 = open('../data/rawData/{}.pkl'.format(tradingDay),'rb')
            p = pickle.load(file2)
            preCloseM = p['preCloseM']
            openM = p['openM']
            highM = p['highM']
            lowM = p['lowM']
            closeM = p['closeM']
            volumeM = p['volumeM']
            amountM = p['amountM']
            retM = p['retM']
            adjFactorM = p['adjFactorM']
            listedM = p['listedM']
            suspM = p['suspM']
            stStateM = p['stStateM']
            hs300ConstC_IndexConstM = p['hs300ConstC_IndexConstM']
            sz50ConstC_IndexConstM = p['sz50ConstC_IndexConstM']
            zz500ConstC_IndexConstM = p['zz500ConstC_IndexConstM']
            file2.close()
        except:
            preCloseM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            openM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            highM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            lowM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            closeM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            volumeM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            amountM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            retM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            adjFactorM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            listedM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            suspM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            stStateM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            hs300ConstC_IndexConstM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            sz50ConstC_IndexConstM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)
            zz500ConstC_IndexConstM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)

        finally:
            preClose_list.append(preCloseM)
            open_list.append(openM)
            high_list.append(highM)
            low_list.append(lowM)
            close_list.append(closeM)
            volume_list.append(volumeM)
            amount_list.append(amountM)
            ret_list.append(retM)
            adjFactor_list.append(adjFactorM)
            listed_list.append(listedM)
            susp_list.append(suspM)
            stState_list.append(stStateM)
            hs300ConstC_IndexConst_list.append(hs300ConstC_IndexConstM)
            sz50ConstC_IndexConst_list.append(sz50ConstC_IndexConstM)
            zz500ConstC_IndexConst_list.append(zz500ConstC_IndexConstM)

    preCloseM = pd.concat(preClose_list)
    openM = pd.concat(open_list)
    highM = pd.concat(high_list)
    lowM = pd.concat(low_list)
    closeM = pd.concat(close_list)
    volumeM = pd.concat(volume_list)
    amountM = pd.concat(amount_list)
    retM = pd.concat(ret_list)
    adjFactorM = pd.concat(adjFactor_list)
    listedM = pd.concat(listed_list)
    suspM = pd.concat(susp_list)
    stStateM = pd.concat(stState_list)
    hs300ConstC_IndexConstM = pd.concat(hs300ConstC_IndexConst_list)
    sz50ConstC_IndexConstM = pd.concat(sz50ConstC_IndexConst_list)
    zz500ConstC_IndexConstM = pd.concat(zz500ConstC_IndexConst_list)


