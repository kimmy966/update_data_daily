# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pickle

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
    industryCiticsM_list = []

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
            industryCiticsM = p['industryCiticsM']
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
            industryCiticsM = pd.DataFrame(index=[str(tradingDay)],columns=tickerUnivSR)

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
            industryCiticsM_list.append(industryCiticsM)

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
    industryCiticsM = pd.concat(industryCiticsM_list)

    ST_raw = stStateM.rolling(window=60, min_periods=1, center=False).sum()
    listed_raw = -(listedM - 1).rolling(window=60, min_periods=1, center=False).sum()

    flagStState = (ST_raw == 0)
    flagList = (listed_raw == 0)
    flagsusp = (suspM == 0)

    flag = flagList & flagStState & flagsusp
    flag = flag.astype('int')

    file2 = open('../data/rawData/totalTradingData.pkl', 'wb')
    dic = {'preCloseM': preCloseM,
           'openM': openM,
           'highM': highM,
           'lowM': lowM,
           'closeM': closeM,
           'volumeM': volumeM,
           'amountM': amountM,
           'retM': retM,
           'adjFactorM': adjFactorM,
           'listedM': listedM,
           'suspM': suspM,
           'stStateM': stStateM,
           'flag':flag,
           'hs300ConstC_IndexConstM': hs300ConstC_IndexConstM,
           'sz50ConstC_IndexConstM': sz50ConstC_IndexConstM,
           'zz500ConstC_IndexConstM': zz500ConstC_IndexConstM,
           'industryCiticsM': industryCiticsM
           }
    pickle.dump(dic, file2)
    file2.close()

    preCloseM.to_csv('../dataProcess/rawData/preCloseM.csv')
    openM.to_csv('../dataProcess/rawData/openM.csv')
    highM.to_csv('../dataProcess/rawData/highM.csv')
    lowM.to_csv('../dataProcess/rawData/lowM.csv')
    closeM.to_csv('../dataProcess/rawData/closeM.csv')
    retM.to_csv('../dataProcess/rawData/retM.csv')
    volumeM.to_csv('../dataProcess/rawData/volumeM.csv')
    amountM.to_csv('../dataProcess/rawData/amountM.csv')
    adjFactorM.to_csv('../dataProcess/rawData/adjFactorM.csv')
    listedM.to_csv('../dataProcess/rawData/listedM.csv')
    suspM.to_csv('../dataProcess/rawData/suspM.csv')
    stStateM.to_csv('../dataProcess/rawData/stStateM.csv')
    flag.to_csv('../dataProcess/rawData/flag.csv')
    hs300ConstC_IndexConstM.to_csv('../dataProcess/rawData/hs300ConstC_IndexConstM.csv')
    sz50ConstC_IndexConstM.to_csv('../dataProcess/rawData/sz50ConstC_IndexConstM.csv')
    zz500ConstC_IndexConstM.to_csv('../dataProcess/rawData/zz500ConstC_IndexConstM.csv')
    industryCiticsM.to_csv('../dataProcess/rawData/industryCiticsM.csv')

# def partlyUpdate(startDate=endDate_default,endDate=endDate_default):
#
#     if startDate == endDate:
#         tradingDays = [tradingDateV[(tradingDateV<=int(endDate)).squeeze()].squeeze()[-1]]
#     else:
#         tradingDays = tradingDateV[(tradingDateV>=int(startDate)).squeeze() & (tradingDateV<=int(endDate)).squeeze()].squeeze()
#     if len(tradingDays) == 1:
#
#


if __name__ == '__main__':
    fullUpdate(startDate='20180101')
    # partlyUpdate()