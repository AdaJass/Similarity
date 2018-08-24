import pandas as pd
import os
import numpy as np
import talib
from datetime import datetime

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
    def ToJson(self):
        json = {}
        json['alpha'] = self.alpha
        json['scale'] = self.scale
        json['data_id'] = self.data_id
        json['data_file'] = self.data_file
        json['data_start'] = self.data_start
        json['data_end'] = self.data_end
        return json

class Price(FactorBase):
    
    def LoadData(self):  
        df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.data_file + '.csv'))  
        data = df[df.date>self.data_start]   
        data = data[data.date<self.data_end]     
        data['typical'] = data['close']/3 + data['high']/3 + data['low']/3
        dt_list = np.array(data['typical'])
        dtmin, dtmax = dt_list.min(), dt_list.max() # 求最大最小值
        dt = (dt_list-dtmin)/(dtmax-dtmin) # (矩阵元素-最小值)/(最大值-最小值)
        return dt

    def Compare(self, C):
        vec1 = self.LoadData()
        vec2 = C.LoadData()
        dist = np.linalg.norm(vec1 - vec2)
        return (dist - self.scale)*self.alpha

class TIME(FactorBase):
    def Compare(self,C):
        thistime = datetime.strptime(self.data_end,'%Y.%m.%d-%H:%M')
        thattime = datetime.strptime(C.data_end,'%Y.%m.%d-%H:%M')
        return (abs(thistime.hour - thattime.hour) - self.scale)*self.alpha

class RSI(FactorBase):    
    def LoadData(self):
        df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.data_file + '.csv'))
        data = df        
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
        vec1 = self.LoadData()
        vec2 = C.LoadData()
        dist = np.linalg.norm(vec1 - vec2)
        return (dist - self.scale)*self.alpha

class EMA(FactorBase):    
    def LoadData(self):
        data = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.data_file + '.csv'))    
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
        vec1 = self.LoadData()
        vec2 = C.LoadData()
        dist = np.linalg.norm(vec1 - vec2)
        return (dist - self.scale)*self.alpha

class MACD(FactorBase):    
    def LoadData(self):
        data =  pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.data_file + '.csv'))
        dt_list = np.array(data['close'])
        macd, macdsignal, macdhist = talib.MACD(dt_list)
        data['ind1'] = macd
        data['ind2'] = macdsignal
        data['ind3'] = macdhist
        data = data[data.date>self.data_start]   
        data = data[data.date<self.data_end] 
        dt_list1 = np.array(data['ind1'])
        dt_list2 = np.array(data['ind2'])
        dtmin, dtmax = dt_list1.min(), dt_list1.max() # 求最大最小值
        dt1 = (dt_list-dtmin)/(dtmax-dtmin) # (矩阵元素-最小值)/(最大值-最小值)
        dtmin, dtmax = dt_list2.min(), dt_list2.max() # 求最大最小值
        dt2 = (dt_list-dtmin)/(dtmax-dtmin) # (矩阵元素-最小值)/(最大值-最小值)
        return dt1, dt2

    def Compare(self,C):
        vec11,vec12 = self.LoadData()
        vec21,vec22 = C.LoadData()
        dist1 = np.linalg.norm(vec11 - vec21)
        dist2 = np.linalg.norm(vec12 - vec22)
        return (dist1 + dist2 - self.scale)*self.alpha

def init_factor_set(S):
    out={}
    if S.get('M15'):
        out['M15'] = Price(**S['M15'])
    if S.get('M30'):
        out['M30'] = Price(**S['M30'])
    if S.get('H1'):
        out['H1'] = Price(**S['H1'])
    if S.get('H4'):
        out['H4'] = Price(**S['H4'])
    if S.get('D1'):
        out['D1'] = Price(**S['D1'])
    if S.get('W1'):
        out['W1'] = Price(**S['W1'])
    if S.get('TIME'):
        out['TIME'] =TIME(**S['TIME'])
    if S.get('MACD'):
        out['MACD'] = MACD(**S['MACD'])
    if S.get('RSI'):
        out['RSI'] = RSI(**S['RSI'])
    if S.get('EMA'):
        out['EMA'] = EMA(**S['EMA'])
    return out

def serialize_factor_set(S):
    # import pdb;pdb.set_trace()    
    out = {}
    for k in S:
        out[k] = S[k].ToJson()
    return out

def compare_factor_set(S1,S2):
    cmp=0.0
    for k in S1:
        cmp += S1[k].Compare(S2[k])
    return cmp