import pandas as pd
import os
import numpy as np
import talib

class FactorBase():
    def __init__(self, alpha, scale, data_id, data_file, data_start, data_end):
        self.alpha = alpha
        self.scale = scale
        self.data_id = data_id
        self.data_file = data_file
        self.data_start = data_start
        self.data_end = data_end

    def LoadData(self):
        # df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.data_file + '.csv'))
        pass   
    def Compare(self, F):
        pass

class Price(FactorBase):
    df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.data_file + '.csv'))
    def LoadData(self):        
        data = Price.df[Price.df.date>self.data_start]   
        data = data[data.date<self.data_end]     
        data['typical'] = data['close']/3 + data['high']/3 + data['low']/3
        dt_list = np.array(data['typical'])
        dtmin, dtmax = dt_list.min(), dt_list.max() # 求最大最小值
        dt = (dt_list-dtmin)/(dtmax-dtmin) # (矩阵元素-最小值)/(最大值-最小值)
        return dt

    def Compare(self, C):
        vec1 = Price.LoadData()
        vec2 = C.LoadData()
        dist = np.linalg.norm(vec1 - vec2)
        return (dist - self.scale)*self.alpha

class Time(FactorBase):
    def Compare(self,C):
        return (abs(self.data_end - C.data_end) - self.scale)*self.alpha

class RSI(FactorBase):
    df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.data_file + '.csv'))
    def LoadData(self):
        data = RSI.df        
        dt_list = np.array(data['close'])
        dt = talib.RSI(dt_list)
        data['ind'] = dt
        data = data[data.date>self.data_start]   
        data = data[data.date<self.data_end] 
        dt_list = np.array(data['ind'])
        dtmin, dtmax = dt_list.min(), dt_list.max() # 求最大最小值
        dt = (dt_list-dtmin)/(dtmax-dtmin) # (矩阵元素-最小值)/(最大值-最小值)
        return dt

    def Compare(self,C):
        pass

class EMA(FactorBase):
    df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.data_file + '.csv'))
    def LoadData(self):
        data = EMA.df        
        dt_list = np.array(data['close'])
        dt = talib.EMA(dt_list)
        data['ind'] = dt
        data = data[data.date>self.data_start]   
        data = data[data.date<self.data_end] 
        dt_list = np.array(data['ind'])
        dtmin, dtmax = dt_list.min(), dt_list.max() # 求最大最小值
        dt = (dt_list-dtmin)/(dtmax-dtmin) # (矩阵元素-最小值)/(最大值-最小值)
        return dt

    def Compare(self,C):
        pass

class MACD(FactorBase):
    df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.data_file + '.csv'))
    def LoadData(self):
        data = MACD.df        
        dt_list = np.array(data['close'])
        dt = talib.MACD(dt_list)
        data['ind'] = dt
        data = data[data.date>self.data_start]   
        data = data[data.date<self.data_end] 
        dt_list = np.array(data['ind'])
        dtmin, dtmax = dt_list.min(), dt_list.max() # 求最大最小值
        dt = (dt_list-dtmin)/(dtmax-dtmin) # (矩阵元素-最小值)/(最大值-最小值)
        return dt

    def Compare(self,C):
        pass




