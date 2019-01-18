# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from functools import reduce
import pickle
import os
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
        where tradingday>='20060101'
        and tradingday<='%s'
        order by tradingday asc
        ''' % self.endDate

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

    def __tradingData(self,tradingDay):

        sql = '''
        SELECT A.[TradingDay], B.[SecuMarket], B.[SecuCode], A.[PrevClosePrice],
        A.[OpenPrice],A.[HighPrice],A.[LowPrice],A.[ClosePrice], A.[TurnoverVolume],A.[TurnoverValue]
        FROM [JYDB].[dbo].[QT_DailyQuote] A 
        inner join [JYDB].[dbo].[SecuMain] B 
        on A.[InnerCode]=B.[InnerCode] 
        and B.SecuMarket in (83,90) 
        and B.SecuCategory=1 
        where A.tradingday='%s' 
        ''' % tradingDay
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
        where A.tradingday='%s' 
        ''' % tradingDay
        dataIndex = pd.read_sql_query(sql, conn243)

        dataV = pd.concat([dataIndex,dataStock])

        sql = '''
        SELECT [TradingDay], [SecuCode], [StockReturns]  
        FROM [Group_General].[dbo].[DailyQuote]
        where tradingday='%s' 
        ''' % tradingDay

        dataStock = pd.read_sql_query(sql, conn247)

        sql = '''
        SELECT A.[TradingDay], B.[SecuCode], A.[ChangePCT] 
        FROM [JYDB].[dbo].[QT_IndexQuote] A 
        inner join [JYDB].[dbo].[SecuMain] B 
        on A.[InnerCode]=B.[InnerCode] 
        and B.SecuMarket in (83,90) 
        and (B.SecuCode = '000300' or B.SecuCode = '000016' or B.SecuCode = '000905')
        and B.SecuCategory=4 
        where A.tradingday='%s' 
        ''' % tradingDay

        dataIndex = pd.read_sql_query(sql, conn243)
        dataIndex.ChangePCT = dataIndex.ChangePCT / 100
        dataIndex = dataIndex.rename({'ChangePCT': 'StockReturns'}, axis='columns')

        dataR = pd.concat([dataIndex, dataStock])

        data = pd.merge(dataV,dataR)

        flagMarket = data.SecuMarket==83
        data['SecuCode'][flagMarket] = data['SecuCode'].map(lambda x: x + '.SH')
        data['SecuCode'][~flagMarket] = data['SecuCode'].map(lambda x: x + '.SZ')
        data.TradingDay = data.TradingDay.map(lambda x: x.strftime('%Y%m%d'))

        preCloseM = pd.DataFrame(pd.pivot_table(data,values='PrevClosePrice',index='TradingDay',columns='SecuCode'),index=[str(tradingDay)],columns=self.tickerUnivSR)
        openM = pd.DataFrame(pd.pivot_table(data,values='OpenPrice',index='TradingDay',columns='SecuCode'),index=[str(tradingDay)],columns=self.tickerUnivSR)
        highM = pd.DataFrame(pd.pivot_table(data,values='HighPrice',index='TradingDay',columns='SecuCode'),index=[str(tradingDay)],columns=self.tickerUnivSR)
        lowM =pd.DataFrame(pd.pivot_table(data,values='LowPrice',index='TradingDay',columns='SecuCode'),index=[str(tradingDay)],columns=self.tickerUnivSR)
        closeM = pd.DataFrame(pd.pivot_table(data,values='ClosePrice',index='TradingDay',columns='SecuCode'),index=[str(tradingDay)],columns=self.tickerUnivSR)
        volumeM = pd.DataFrame(pd.pivot_table(data,values='TurnoverVolume',index='TradingDay',columns='SecuCode'),index=[str(tradingDay)],columns=self.tickerUnivSR)
        amountM = pd.DataFrame(pd.pivot_table(data,values='TurnoverValue',index='TradingDay',columns='SecuCode'),index=[str(tradingDay)],columns=self.tickerUnivSR)
        retM = pd.DataFrame(pd.pivot_table(data,values='StockReturns',index='TradingDay',columns='SecuCode'),index=[str(tradingDay)], columns=self.tickerUnivSR)

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

        dataAF.TradingDay = dataAF.TradingDay.map(lambda x: x.strftime('%Y%m%d'))

        adjFactorM = pd.pivot_table(dataAF, values='AdjustingFactor', index='TradingDay', columns='SecuCode')
        adjFactorM.fillna(method='pad', inplace=True)
        adjFactorM = pd.DataFrame(adjFactorM ,index=self.tradingDateV, columns=self.tickerUnivSR)
        adjFactorM.fillna(method='pad', inplace=True)
        adjFactorM =pd.DataFrame(adjFactorM ,index=[str(tradingDay)])

        sql = '''
        SELECT A.[ChangeDate],A.[ChangeType],B.[SecuCode],B.[SecuMarket] 
        FROM [JYDB].[dbo].[LC_ListStatus] A 
        inner join [JYDB].[dbo].SecuMain B 
        on A.[InnerCode]=B.[InnerCode] 
        and B.SecuMarket in (83,90) 
        and B.SecuCategory=1 
        where (A.ChangeType = 1 or A.ChangeType = 4)
        '''

        dataStock = pd.read_sql_query(sql, conn243)

        sql = '''
        SELECT A.[PubDate],B.[SecuCode],B.[SecuMarket] 
        FROM [JYDB].[dbo].[LC_IndexBasicInfo] A 
        inner join [JYDB].[dbo].[SecuMain] B 
        on A.[IndexCode]=B.[InnerCode] 
        and B.SecuMarket in (83,90) 
        and (B.SecuCode = '000300' or B.SecuCode = '000016' or B.SecuCode = '000905')
        and B.SecuCategory=4 
        '''

        dataIndex = pd.read_sql_query(sql, conn243)
        dataIndex['ChangeType'] = 1
        dataIndex = dataIndex.rename({'PubDate': 'ChangeDate'}, axis='columns')

        dataV = pd.concat([dataIndex, dataStock])

        flagMarket = dataV.SecuMarket == 83
        dataV['SecuCode'][flagMarket] = dataV['SecuCode'].map(lambda x: x + '.SH')
        dataV['SecuCode'][~flagMarket] = dataV['SecuCode'].map(lambda x: x + '.SZ')

        # dataV.ChangeDate = pd.Series([x.strftime('%Y%m%d') for x in dataV.ChangeDate.values])
        dataV.ChangeDate = dataV.ChangeDate.map(lambda x: x.strftime('%Y%m%d'))

        listedM = pd.pivot_table(dataV, values='ChangeType', index='ChangeDate', columns='SecuCode')
        dateTotal = np.union1d(listedM.index.values, [str(tradingDay)])
        listedM = pd.DataFrame(listedM, index=dateTotal, columns=self.tickerUnivSR)

        listedM[listedM == 4] = 0
        listedM.fillna(method='pad', inplace=True)
        listedM = pd.DataFrame(listedM,index= [str(tradingDay)])
        listedM = listedM.fillna(0)

        sql = '''
        SELECT A.[SuspendDate],A.[ResumptionDate],A.[SuspendTime], A.[ResumptionTime], B.[SecuCode],B.[SecuMarket] 
        FROM [JYDB].[dbo].[LC_SuspendResumption] A 
        inner join [JYDB].[dbo].[SecuMain] B 
        on A.[InnerCode]=B.[InnerCode] 
        and B.SecuMarket in (83,90) 
        and B.SecuCategory=1 
        where A.[SuspendDate] = '%s'
        '''%tradingDay

        if tradingDay == self.tradingDateV[0]:
            sql = sql.replace('A.[SuspendDate] = ','A.[SuspendDate] <= ')
            dataSusp = pd.read_sql_query(sql, conn243)

            flagMarket = dataSusp.SecuMarket == 83
            dataSusp['SecuCode'][flagMarket] = dataSusp['SecuCode'].map(lambda x: x + '.SH')
            dataSusp['SecuCode'][~flagMarket] = dataSusp['SecuCode'].map(lambda x: x + '.SZ')
            dataSusp.SuspendDate = dataSusp.SuspendDate.map(lambda x: x.strftime('%Y%m%d'))

            dataSusp['flag'] = 1
            startFlag = pd.pivot_table(dataSusp, values='flag',index='SuspendDate', columns='SecuCode')
            try:
                startFlag = pd.DataFrame(startFlag, index=[str(tradingDay)], columns=self.tickerUnivSR)
            except:
                startFlag = pd.DataFrame(index=[str(tradingDay)], columns=self.tickerUnivSR)
            endFlag = pd.DataFrame(index=[str(tradingDay)], columns=self.tickerUnivSR)

            amount = amountM.fillna(0)
            flag = (amount == 0)

            endFlag[startFlag == 1] = 1
            endFlag[flag] = 1
            suspM = endFlag.fillna(0)
            suspM[(listedM==0)] = 1

        else:
            dataSusp = pd.read_sql_query(sql, conn243)

            flagMarket = dataSusp.SecuMarket == 83
            dataSusp['SecuCode'][flagMarket] = dataSusp['SecuCode'].map(lambda x: x + '.SH')
            dataSusp['SecuCode'][~flagMarket] = dataSusp['SecuCode'].map(lambda x: x + '.SZ')
            dataSusp.SuspendDate = dataSusp.SuspendDate.map(lambda x: x.strftime('%Y%m%d'))

            file2 = open('../data/rawData/{}.pkl'.format(self.tradingDateV[self.tradingDateV.tolist().index(tradingDay)-1]), 'rb')
            suspPre = pickle.load(file2)['suspM']
            file2.close()

            dataSusp['flag'] = 1
            startFlag = pd.pivot_table(dataSusp, values='flag',index='SuspendDate', columns='SecuCode')
            try:
                startFlag = pd.DataFrame(startFlag, index=[str(tradingDay)], columns=self.tickerUnivSR)
            except:
                startFlag = pd.DataFrame(index=[str(tradingDay)], columns=self.tickerUnivSR)
            endFlag = pd.DataFrame(index=[str(tradingDay)], columns=self.tickerUnivSR)

            amount = amountM.fillna(0)
            flag = (amount == 0)

            endFlag[startFlag == 1] = 1
            endFlag[~flag] = 0

            suspM = pd.concat([suspPre,endFlag]).fillna(method='pad')

            suspM = pd.DataFrame(suspM,index=[str(tradingDay)])

            suspM[(listedM==0)] = 1

        sql='''
        SELECT A.[SpecialTradeTime],A.[SpecialTradeType],B.[SecuCode],B.[SecuMarket] 
        FROM [JYDB].[dbo].[LC_SpecialTrade] A 
        inner join [JYDB].[dbo].[SecuMain] B 
        on A.[InnerCode]=B.[InnerCode] 
        and B.SecuMarket in (83,90) 
        and B.SecuCategory=1 
        where (A.[SpecialTradeType]=1 or A.[SpecialTradeType] = 2 or A.[SpecialTradeType] = 5 or A.[SpecialTradeType] = 6)
        and A.[SpecialTradeTime] = '%s'
        '''% tradingDay

        if tradingDay == self.tradingDateV[0]:
            sql = sql.replace('A.[SpecialTradeTime] = ','A.[SpecialTradeTime] <= ')
            dataV = pd.read_sql_query(sql, conn243)

            flagMarket = dataV.SecuMarket == 83
            dataV['SecuCode'][flagMarket] = dataV['SecuCode'].map(lambda x: x + '.SH')
            dataV['SecuCode'][~flagMarket] = dataV['SecuCode'].map(lambda x: x + '.SZ')
            dataV.SpecialTradeTime = dataV.SpecialTradeTime.map(lambda x: x.strftime('%Y%m%d'))

            dataV['SpecialTradeType'][dataV['SpecialTradeType'] == 5] = 1
            dataV['SpecialTradeType'][dataV['SpecialTradeType'] == 2] = 0
            dataV['SpecialTradeType'][dataV['SpecialTradeType'] == 6] = 0

            stStateM = pd.pivot_table(dataV, values='SpecialTradeType', index='SpecialTradeTime', columns='SecuCode')
            dateTotal = np.union1d(stStateM.index.values,  [str(tradingDay)])
            stStateM = pd.DataFrame(stStateM, index=dateTotal, columns=self.tickerUnivSR)
            stStateM = stStateM.fillna(method='pad')
            stStateM = pd.DataFrame(stStateM, index=[str(tradingDay)])
            stStateM = stStateM.fillna(0)

        else:
            try:
                file2 = open('../data/rawData/{}.pkl'.format(self.tradingDateV[self.tradingDateV.tolist().index(tradingDay)-1]), 'rb')
                stStatePre = pickle.load(file2)['stStateM']
                file2.close()

                dataV = pd.read_sql_query(sql, conn243)

                flagMarket = dataV.SecuMarket == 83
                dataV['SecuCode'][flagMarket] = dataV['SecuCode'].map(lambda x: x + '.SH')
                dataV['SecuCode'][~flagMarket] = dataV['SecuCode'].map(lambda x: x + '.SZ')
                dataV.SpecialTradeTime = dataV.SpecialTradeTime.map(lambda x: x.strftime('%Y%m%d'))

                dataV['SpecialTradeType'][dataV['SpecialTradeType'] == 5] = 1
                dataV['SpecialTradeType'][dataV['SpecialTradeType'] == 2] = 0
                dataV['SpecialTradeType'][dataV['SpecialTradeType'] == 6] = 0

                stStateM = pd.pivot_table(dataV, values='SpecialTradeType', index='SpecialTradeTime', columns='SecuCode')
                stStateM = pd.concat([stStatePre,stStateM]).fillna(method='pad')

            except:

                file2 = open('../data/rawData/{}.pkl'.format(self.tradingDateV[self.tradingDateV.tolist().index(tradingDay)-1]), 'rb')
                stStatePre = pickle.load(file2)['stStateM']
                file2.close()

                stStateM = pd.DataFrame(index=[str(tradingDay)], columns=self.tickerUnivSR)
                stStateM = pd.concat([stStatePre,stStateM]).fillna(method='pad')

                # stStateM = pd.DataFrame(stStatePre,index=np.concatenate([stStatePre.index.values,str(tradingDay)]))
                # stStateM = stStateM.fillna(method='pad')
            finally:

                stStateM = pd.DataFrame(stStateM, index=[str(tradingDay)])
                stStateM = stStateM.fillna(0).astype(int)

        file2 = open(self.rawData_path+tradingDay+'.pkl', 'wb')
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
               'stStateM': stStateM
               }
        pickle.dump(dic, file2)
        file2.close()

    def get_tradingData(self):

        tradingdays = self.tradingDateV[(self.tradingDateV>=str(self.startDate))&(self.tradingDateV<=str(self.endDate))]

        for tradingday in tradingdays:
            self.__tradingData(tradingday)

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
    db = dailyQuant(startDate=20060101, endDate=20190114)
    db.get_tradingData()























