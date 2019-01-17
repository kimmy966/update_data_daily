# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from functools import reduce
import pickle
import pymssql

startDate_default = '20060101'
# endDate_default = (datetime.now() + timedelta(days=-1)).strftime('%Y%m%d')
endDate_default = datetime.now().strftime('%Y%m%d')
indexTickerUnivSR_default = np.array(['000300.SH', '000016.SH', '000905.SH'])
indexTickerNameUnivSR_default = np.array(['沪深300', '上证50', '中证500'])

# Global val
conn243 = pymssql.connect(server='192.168.1.243', user="yuman.hu", password="yuman.hu")
conn247 = pymssql.connect(server='192.168.1.247', user="yuman.hu", password="yuman.hu")

# daily data download
class dailyQuant(object):

    def __init__(self, startDate=startDate_default, endDate=endDate_default,
                 indexTickerUnivSR=indexTickerUnivSR_default, indexTickerNameUnivSR=indexTickerNameUnivSR_default):
        self.startDate = startDate
        self.endDate = endDate
        self.rawData_path = '../data/rawData/'
        self.fundamentalData_path = '../data/fundamentalData/'
        self.indexTickerUnivSR = indexTickerUnivSR
        self.indexTickerNameUnivSR = indexTickerNameUnivSR
        self.tradingDateV, self.timeSeries = self.get_dateData()
        self.tickerUnivSR, self.stockTickerUnivSR, self.tickerNameUnivSR, self.stockTickerNameUnivSR, self.tickerUnivTypeR = self.get_tickerData()

    def get_dateData(self):

        sql = '''
        SELECT [tradingday]
        FROM [Group_General].[dbo].[TradingDayList]
        where tradingday>='%s'
        and tradingday<='%s'
        order by tradingday asc
        ''' % (self.startDate, self.endDate)

        dateSV = pd.read_sql(sql, conn247)

        tradingdays = dateSV.tradingday.unique()
        tradingDateV = np.array([x.replace('-', '') for x in tradingdays])
        timeSeries = pd.Series(pd.to_datetime(tradingDateV))
        pd.Series(tradingDateV).to_csv(self.rawData_path+ 'tradingDateV.csv', index=False)

        return tradingDateV, timeSeries

    def get_tickerData(self):

        # and B.[SecuAbbr] not like '%%ST%%'
        #  where ChangeDate>='%s'

        sql = '''
        SELECT A.[ChangeDate],A.[ChangeType],B.[SecuCode],B.[SecuMarket],B.[SecuAbbr] 
        FROM [JYDB].[dbo].[LC_ListStatus] A 
        inner join [JYDB].[dbo].SecuMain B 
        on A.[InnerCode]=B.[InnerCode] 
        and B.SecuMarket in (83,90) 
        and B.SecuCategory=1 
        order by SecuCode asc 
        '''

        dataV = pd.read_sql(sql, conn243)

        flagMarket = dataV.SecuMarket == 83
        dataV['SecuCode'][flagMarket] = dataV['SecuCode'].map(lambda x: x + '.SH')
        dataV['SecuCode'][~flagMarket] = dataV['SecuCode'].map(lambda x: x + '.SZ')

        # dataV.ChangeDate = pd.Series([x.strftime('%Y%m%d') for x in dataV.ChangeDate.values])
        dataV.ChangeDate = dataV.ChangeDate.map(lambda x: x.strftime('%Y%m%d'))

        flagV = np.full(len(dataV), True)
        flagList = []
        for i in range(len(dataV)):
            if dataV.iat[i, 1] == 4:
                if dataV.iat[i, 0] < self.tradingDateV[0]:
                    flagList.append(dataV.iat[i, 2])

        for i in range(len(dataV)):
            if dataV.iat[i, 2] in flagList:
                flagV[i] = False

        dataV = dataV[flagV]
        stockTickerUnivSR = dataV.SecuCode.unique()

        tickerUnivSR = np.append(self.indexTickerUnivSR, stockTickerUnivSR)

        stockTickerNameUnivSR = dataV.SecuAbbr.unique()
        tickerNameUnivSR = np.append(self.indexTickerNameUnivSR, stockTickerNameUnivSR)

        tickerUnivTypeR = np.append(np.full(len(self.indexTickerUnivSR), 3), np.ones(len(dataV)))

        pd.DataFrame(self.indexTickerUnivSR).T.to_csv(self.rawData_path+'indexTickerUnivSR.csv', header=False, index=False)
        pd.DataFrame(stockTickerUnivSR).T.to_csv(self.rawData_path+'stockTickerUnivSR.csv', header=False, index=False)
        pd.DataFrame(tickerUnivSR).T.to_csv(self.rawData_path+'tickerUnivSR.csv', header=False, index=False)
        pd.DataFrame(self.indexTickerNameUnivSR).T.to_csv(self.rawData_path+'indexTickerNameUnivSR.csv', header=False, index=False)
        pd.DataFrame(stockTickerNameUnivSR).T.to_csv(self.rawData_path+'stockTickerNameUnivSR.csv', header=False, index=False)
        pd.DataFrame(tickerNameUnivSR).T.to_csv(self.rawData_path+'tickerNameUnivSR.csv', header=False, index=False)
        pd.DataFrame(tickerUnivTypeR).T.to_csv(self.rawData_path+'tickerUnivTypeR.csv', header=False, index=False)

        return tickerUnivSR, stockTickerUnivSR, tickerNameUnivSR, stockTickerNameUnivSR, tickerUnivTypeR

    def get_tradingData(self):

        sql = '''
        SELECT A.[TradingDay], B.[SecuMarket], B.[SecuCode], A.[PrevClosePrice],
        A.[OpenPrice],A.[HighPrice],A.[LowPrice],A.[ClosePrice], A.[TurnoverVolume],A.[TurnoverValue]
        FROM [JYDB].[dbo].[QT_DailyQuote] A 
        inner join [JYDB].[dbo].[SecuMain] B 
        on A.[InnerCode]=B.[InnerCode] 
        and B.SecuMarket in (83,90) 
        and B.SecuCategory=1 
        where A.tradingday>='%s' 
        ''' % self.startDate
        dataStock = pd.read_sql_query(sql, conn243)

        sql = '''
        SELECT A.[TradingDay], B.[SecuMarket], B.[SecuCode], A.[PrevClosePrice],
        A.[OpenPrice],A.[HighPrice],A.[LowPrice],A.[ClosePrice], A.[TurnoverVolume],A.[TurnoverValue]
        FROM [JYDB].[dbo].[QT_IndexQuote] A 
        inner join [JYDB].[dbo].[SecuMain] B 
        on A.[InnerCode]=B.[InnerCode] 
        and B.SecuMarket in (83,90) 
        and (B.SecuCode = '000300' or B.SecuCode = '000016' or B.SecuCode = '000905')
        and B.SecuCategory=4 
        where A.tradingday>='%s' 
        ''' % self.startDate
        dataIndex = pd.read_sql_query(sql, conn243)

        dataV = pd.concat([dataIndex,dataStock])

        sql = '''
        SELECT [TradingDay], [SecuCode], [StockReturns]  
        FROM [Group_General].[dbo].[DailyQuote]
        where tradingday>='%s' 
        ''' % self.startDate

        dataStock = pd.read_sql_query(sql, conn247)

        sql = '''
        SELECT A.[TradingDay], B.[SecuCode], A.[ChangePCT] 
        FROM [JYDB].[dbo].[QT_IndexQuote] A 
        inner join [JYDB].[dbo].[SecuMain] B 
        on A.[InnerCode]=B.[InnerCode] 
        and B.SecuMarket in (83,90) 
        and (B.SecuCode = '000300' or B.SecuCode = '000016' or B.SecuCode = '000905')
        and B.SecuCategory=4 
        where A.tradingday>='%s' 
        ''' % self.startDate

        dataIndex = pd.read_sql_query(sql, conn243)
        dataIndex.ChangePCT = dataIndex.ChangePCT / 100
        dataIndex = dataIndex.rename({'ChangePCT': 'StockReturns'}, axis='columns')

        dataR = pd.concat([dataIndex, dataStock])

        data = pd.merge(dataV,dataR)

        flagMarket = data.SecuMarket==83
        data['SecuCode'][flagMarket] = data['SecuCode'].map(lambda x: x + '.SH')
        data['SecuCode'][~flagMarket] = data['SecuCode'].map(lambda x: x + '.SZ')
        data.TradingDay = data.TradingDay.map(lambda x: x.strftime('%Y%m%d'))

        preCloseM = pd.DataFrame(pd.pivot_table(data,values='PrevClosePrice',index='TradingDay',columns='SecuCode'),index=self.tradingDateV,columns=self.tickerUnivSR)
        openM = pd.DataFrame(pd.pivot_table(data,values='OpenPrice',index='TradingDay',columns='SecuCode'),index=self.tradingDateV,columns=self.tickerUnivSR)
        highM = pd.DataFrame(pd.pivot_table(data,values='HighPrice',index='TradingDay',columns='SecuCode'),index=self.tradingDateV,columns=self.tickerUnivSR)
        lowM =pd.DataFrame(pd.pivot_table(data,values='LowPrice',index='TradingDay',columns='SecuCode'),index=self.tradingDateV,columns=self.tickerUnivSR)
        closeM = pd.DataFrame(pd.pivot_table(data,values='ClosePrice',index='TradingDay',columns='SecuCode'),index=self.tradingDateV,columns=self.tickerUnivSR)
        volumeM = pd.DataFrame(pd.pivot_table(data,values='TurnoverVolume',index='TradingDay',columns='SecuCode'),index=self.tradingDateV,columns=self.tickerUnivSR)
        amountM = pd.DataFrame(pd.pivot_table(data,values='TurnoverValue',index='TradingDay',columns='SecuCode'),index=self.tradingDateV,columns=self.tickerUnivSR)
        retM = pd.DataFrame(pd.pivot_table(data,values='StockReturns',index='TradingDay',columns='SecuCode'),index=self.tradingDateV, columns=self.tickerUnivSR)

        sql = '''
        SELECT A.[ExDiviDate], B.[SecuMarket], B.[SecuCode], A.[AdjustingFactor] 
        FROM [JYDB].[dbo].[QT_AdjustingFactor] A 
        inner join [JYDB].[dbo].[SecuMain] B 
        on A.[InnerCode]=B.[InnerCode] 
        and B.SecuMarket in (83,90) 
        and B.SecuCategory=1 
        '''

        dataAF = pd.read_sql_query(sql, conn243)
        dataAF = dataAF.rename({'ExDiviDate':'TradingDay'},axis=1)

        flagMarket = dataAF.SecuMarket == 83
        dataAF['SecuCode'][flagMarket] = dataAF['SecuCode'].map(lambda x: x + '.SH')
        dataAF['SecuCode'][~flagMarket] = dataAF['SecuCode'].map(lambda x: x + '.SZ')

        dataAF.ExDiviDate = dataAF.ExDiviDate.map(lambda x: x.strftime('%Y%m%d'))

        adjFactorM = pd.pivot_table(dataAF, values='AdjustingFactor', index='TradingDay', columns='SecuCode')
        adjFactorM.fillna(method='pad', inplace=True)
        adjFactorM = pd.DataFrame(adjFactorM, index=self.tradingDateV, columns=self.tickerUnivSR)
        adjFactorM.fillna(method='pad', inplace=True)


    def get_StockEODDerivativeIndicator(self):

        sql = '''
        SELECT A.[TradingDay],A.[PELYR],A.[PE],A.[PB],A.[PCF],A.[PCFTTM],
                A.[PCFS],A.[PCFSTTM],A.[PS],A.[PSTTM],A.[DividendRatioLYR],A.[DividendRatio],B.[SecuCode],B.[SecuMarket]
        FROM [JYDB].[dbo].[LC_DIndicesForValuation] A
        inner join [JYDB].[dbo].[SecuMain] B
        on A.[InnerCode]=B.[InnerCode]
        and B.SecuMarket in (83,90)
        and B.SecuCategory=1
        where tradingday>='%s'
        and tradingday<='%s'
        order by tradingday asc
        '''%(self.startDate,self.endDate)

        dataC = pd.read_sql(sql,conn243)

        flagMarket = dataC.SecuMarket==83
        dataC['SecuCode'][flagMarket] = dataC['SecuCode'].map(lambda x: x+'.SH')
        dataC['SecuCode'][~flagMarket] = dataC['SecuCode'].map(lambda x: x+'.SZ')
        dataC.TradingDay = dataC.TradingDay.map(lambda x: x.strftime('%Y%m%d'))
        dataC.drop('SecuMarket',axis=1,inplace=True)

        sql='''
        SELECT A.[TradingDay], A.[TotalShares], A.[AFloats], A.[NonRestrictedShares], A.[MarketCapTotal],
        A.[MarketCapAFloat],B.[SecuMarket], B.[SecuCode]
        FROM [Group_General].[dbo].[DailyQuote] A
        inner join [Group_General].[dbo].[SecuMain] B
        on A.[InnerCode]=B.[InnerCode]
        and B.SecuMarket in (83,90)
        and B.SecuCategory=1
        where tradingday>='%s'
        and tradingday<='%s'
        order by tradingday asc
        '''%(self.startDate,self.endDate)

        data = pd.read_sql(sql,conn247)

        flagMarket = data.SecuMarket==83
        data.loc[flagMarket,'SecuCode'] = data['SecuCode'][flagMarket].map(lambda x: x+'.SH')
        data.loc[~flagMarket,'SecuCode'] = data['SecuCode'][~flagMarket].map(lambda x: x+'.SZ')
        data.TradingDay = data.TradingDay.map(lambda x: x.strftime('%Y%m%d'))
        data.drop('SecuMarket',axis=1,inplace=True)
        data = pd.merge(dataC,data,how='outer',on=['SecuCode','TradingDay'])

        code = data['SecuCode']
        data.drop(labels=['SecuCode'],axis=1,inplace=True)
        data.insert(1,'SecuCode',code)

        data.to_pickle(self.fundamentalData_path+'fundamentalData_StockAnalysis.pickle')

        def get_sql(name):
            sql = '''
            SELECT A.[PointDate],A.[EndDate],A.[TotalSales],A.[TotalAsset],A.[GrossProfit], 
            A.[InterestExpenses],A.[EBITDA],A.[EBIT],A.[NetIncome],A.[SEWithoutMI], B.[SecuMarket], B.[SecuCode]  
            FROM [Group_General].[dbo].[QuarterlyAccount{}] A
            inner join [Group_General].[dbo].[SecuMain] B 
            on A.[CompanyCode]=B.[CompanyCode] 
            and B.SecuMarket in (83,90) 
            and B.SecuCategory=1 
            where A.[PointDate]>='{}'
            and A.[PointDate]<='{}'
            and A.[EndDate]>='{}'
            and A.[EndDate] = (select MAX(C.[EndDate]) 
            from [Group_General].[dbo].[QuarterlyAccount{}] C 
            where C.[PointDate]=A.[PointDate]
            and C.[CompanyCode]=A.[CompanyCode]
            and C.[EndDate]>='{}')
            order by PointDate asc
            '''.format(name, self.startDate, self.endDate, str(int(self.startDate) - 10000), name,
                       str(int(self.startDate) - 10000))
            return sql

        datalist = []
        for name in ['_20141231', '_20151231', '_20160630', '_20161231', '_20170630', '_20171231', '_20180630', '']:
            sql = get_sql(name)
            data = pd.read_sql(sql, conn247)
            datalist.append(data)
        data = pd.concat(datalist)

        flagMarket = data.SecuMarket == 83
        data.loc[flagMarket, 'SecuCode'] = data['SecuCode'][flagMarket].map(lambda x: x + '.SH')
        data.loc[~flagMarket, 'SecuCode'] = data['SecuCode'][~flagMarket].map(lambda x: x + '.SZ')
        data.PointDate = data.PointDate.map(lambda x: x.strftime('%Y%m%d'))
        data.EndDate = data.EndDate.map(lambda x: x.strftime('%Y%m%d'))

        colFlagV = np.in1d(data.values[:, -1], self.tickerUnivSR)
        data = data[colFlagV]

        data.drop('SecuMarket', axis=1, inplace=True)
        code = data['SecuCode']
        data.drop(labels=['SecuCode'], axis=1, inplace=True)
        data.insert(1, 'SecuCode', code)

        data.to_pickle(self.fundamentalData_path + 'fundamentalData_StockFinance.pickle')



if __name__ == '__main__':
    db = dailyQuant(startDate=20190101, endDate=20190114)
    db.get_tradingData()
























