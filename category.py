"""
subdirectory description, it means that in this directory, the category obey the same rule.
{
    M15_alpha
    M15_scale
    M30_alpha
    M30_scale
    H1_alpha
    H1_scale
    H4_alpha
    H4_scale
    D1_alpha
    D1_scale
    W1_alpha
    W1_scale
    Time_alpha
    Time_scale
    Macd_alpha
    Macd_scale
    Rsi_alpha
    Rsi_scale
    EMA_alpha
    EMA_scale
}
##above parameters decide the instinct of a category. they are invariable inside a category.

directory of catecory, each pickle file present a category. the pickle file format:
{
    Data: [{M15:{}, ....EMA:{}}, ....]
    Predict_M15x4: {up, down, null}
    Predict_M30x4
    Predict_H1x4
    Predict_H4x4
    Predict_D1x4
    _id
}
"""
import pickle
import pandas as pd
import os
import json
from pathlib import Path
from factor import *
from datetime import datetime
import random,string
from mongoconnect import *

SubDir = './category/'
f = open(SubDir+'eur_h4_cfg.json')
cat_parameters = json.loads(f.read())
f.close()
# p_order = ['M15', 'M30', 'H1', 'H4', 'D1', 'W1']
# df_set = {}
# for p in p_order:
#     df_set[p] = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), filebasename + '_' + p +'.csv'))  

def MakeCurrentSet(filebasename,current_time, min_period, day_change):
    """make current data to a factor set
    
    Returns:
        json -- pickle like obj
    """
    p_change={'M15':0.1, 'M30':0.2, 'H1':0.25, 'H4':0.3, 'D1':0.8}
    valid_p = p_order[p_order.index(min_period):]
    out = {
        'data': []       
    }
    f_set = {}
    macd_period=cat_parameters.get('MACD_period')
    rsi_period=cat_parameters.get('RSI_period')
    time_period=cat_parameters.get('TIME_period')
    ema_period=cat_parameters.get('EMA_period')

    for p in valid_p:
        # import pdb;pdb.set_trace()
        c_index = df_set[p][df_set[p].date <= current_time].index[-1] 
        f_set[p]={}
        #alpha, scale, data_id, data_file, data_start, data_end
        f_set[p]['alpha'] = cat_parameters[p+'_alpha'] 
        f_set[p]['scale'] = cat_parameters[p+'_scale'] 
        f_set[p]['data_id'] = p
        f_set[p]['data_file'] = p
        s_index = c_index - cat_parameters[p+'_length']
        f_set[p]['data_start'] = df_set[p].iloc[s_index].date
        f_set[p]['data_end'] = df_set[p].iloc[c_index].date
        
        if not p == 'W1':
            change = (df_set[p].iloc[c_index+4].close - df_set[p].iloc[c_index].close) / df_set[p].iloc[c_index].close
            if change > day_change * p_change[p] :
                out['Predict_'+p+'x4'] = {'up':1,'down':0,'null':0}
            elif change < - day_change * p_change[p] :
                out['Predict_'+p+'x4'] = {'up':0,'down':1,'null':0}
            else:
                out['Predict_'+p+'x4'] = {'up':0,'down':0,'null':1}
    
    if cat_parameters.get('MACD_alpha'):
        f_set['MACD']={}
        f_set['MACD']['alpha'] = cat_parameters['MACD_alpha'] 
        f_set['MACD']['scale'] = cat_parameters['MACD_scale'] 
        f_set['MACD']['data_id'] = 'MACD'
        f_set['MACD']['data_file'] = macd_period
        f_set['MACD']['data_start'] = f_set[macd_period]['data_start']
        f_set['MACD']['data_end'] = f_set[macd_period]['data_end']
    if cat_parameters.get('RSI_alpha'):
        f_set['RSI']={}
        f_set['RSI']['alpha'] = cat_parameters['RSI_alpha'] 
        f_set['RSI']['scale'] = cat_parameters['RSI_scale'] 
        f_set['RSI']['data_id'] = 'RSI'
        f_set['RSI']['data_file'] = rsi_period
        f_set['RSI']['data_start'] = f_set[rsi_period]['data_start']
        f_set['RSI']['data_end'] = f_set[rsi_period]['data_end']
    if cat_parameters.get('TIME_alpha'):
        f_set['TIME']={}
        f_set['TIME']['alpha'] = cat_parameters['TIME_alpha'] 
        f_set['TIME']['scale'] = cat_parameters['TIME_scale'] 
        f_set['TIME']['data_id'] = 'TIME'
        f_set['TIME']['data_file'] = time_period
        f_set['TIME']['data_start'] = f_set[time_period]['data_start']
        f_set['TIME']['data_end'] = f_set[time_period]['data_end']
    if cat_parameters.get('MEA_alpha'):
        f_set['EMA']={}
        f_set['EMA']['alpha'] = cat_parameters['EMA_alpha'] 
        f_set['EMA']['scale'] = cat_parameters['EMA_scale'] 
        f_set['EMA']['data_id'] = 'EMA'
        f_set['EMA']['data_file'] = ema_period
        f_set['EMA']['data_start'] = f_set[ema_period]['data_start']
        f_set['EMA']['data_end'] = f_set[ema_period]['data_end']

    out['data'].append(f_set)
    return out

def BuildCat2DB(C, db):
    """just save the category to database.
    """
    rand_str = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    C['id'] = rand_str          
    db.insert(C)

def BuildSimTable2DB(indb, outdb, cp_set):
    cat_list = list(indb.find({}))    
    for index, ca in enumerate(cat_list):  
        result_list = [] 
        key1 = ca['id']     
        for nn in range(index+1,len(cat_list)):
            key2 = cat_list[nn]['id']
            c1 = init_factor_set(ca['data'][0])
            c2 = init_factor_set(cat_list[nn]['data'][0])
            result = compare_factor_set(c1, c2, cp_set)
            # print('current compare of ',key,', result is: ',result)
            result_list.append({'key1': key1, 'key2':key2, 'result':result})
        if len(result_list) >0:
            outdb.insert(result_list)


def BuildCat(C):
    """just build the category to save to pkl.
    
    Arguments:
        C {dict} -- reference to pickle format file description above.    
    """
    rand_str = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    with open(SubDir+rand_str+'.pkl', 'wb') as f:  
            # print(C) 
        C['_id'] = rand_str          
        pickle.dump(C, f)
    return rand_str

def AddOneCat(C):
    """add one cat and cal sim at the same time.
    """
    cat_list = []
    for x in Path(SubDir).iterdir():
        if x.is_file() and x.match('*.pkl'):  
            with open(str(x), 'rb') as f:
                cat = pickle.load(f) 
                cat_list.append(cat)
    BuildCat(C)
    with open(SubDir+'result_table.pickle', 'rb') as f:
        sim_table = pickle.load(f) 
    
    for ca in enumerate(cat_list):        
        key = ca['_id'] + '_'+C['_id']
        c1 = init_factor_set(ca['data'][0])
        c2 = init_factor_set(C['data'][0])
        result = compare_factor_set(c1, c2)
        print('current compare of ',key,', result is: ',result)
        sim_table[key] = result

    with open(SubDir+'result_table.pickle', 'wb') as f:  
        pickle.dump(sim_table, f)

def BuildSimTable():
    """fistly build the similarity table if it isn't exist.

    Arguments:
        n {int} -- nth.
    
    Returns:
        nth list factor set.
    """
    cat_list = []
    for x in Path(SubDir).iterdir():
        if x.is_file() and x.match('*.pkl'):  
            with open(str(x), 'rb') as f:
                cat = pickle.load(f) 
                cat_list.append(cat)
    result_list = {}
    for index, ca in enumerate(cat_list):
        for nn in range(index+1,len(cat_list)):
            key = ca['_id'] + '_'+cat_list[nn]['_id']
            c1 = init_factor_set(ca['data'][0])
            c2 = init_factor_set(cat_list[nn]['data'][0])
            result = compare_factor_set(c1, c2)
            print('current compare of ',key,', result is: ',result)
            result_list[key] = result

    with open(SubDir+'result_table.pickle', 'wb') as f:  
            # print(C) 
        pickle.dump(result_list, f)

def CompleteSimTable():
    """when there are some of p9kl file without culculating the sim result, use this function
    """

    if os.path.exists(SubDir+'result_table.pickle'):
        with open(SubDir+'result_table.pickle', 'rb') as f:
            sim_table = pickle.load(f) 
    else:
        return BuildSimTable()
        
    # sim_tuple = sorted(sim_table.items(), key=lambda d:d[1], reverse = True)  

    file_list = os.listdir(SubDir+'result_table.pickle')
    if len(file_list) > len(sim_table) +10:
        cat_list = []
        for x in Path(SubDir).iterdir():
            if x.is_file() and x.match('*.pkl'):  
                with open(str(x), 'rb') as f:
                    cat = pickle.load(f) 
                    cat_list.append(cat)

        for index, ca in enumerate(cat_list):
            for nn in range(index+1,len(cat_list)):
                key = ca['_id'] + '_'+cat_list[nn]['_id']
                if sim_table.get(key):
                    break
                c1 = init_factor_set(ca['data'][0])
                c2 = init_factor_set(cat_list[nn]['data'][0])
                result = compare_factor_set(c1, c2)
                print('current compare of ',key,', result is: ',result)
                sim_table[key] = result

        with open(SubDir+'result_table.pickle', 'wb') as f:  
            pickle.dump(sim_table, f)

def FindFirstN(n=10):
    """return format [('id',sim_result), (,) ,...]
    """

    if os.path.exists(SubDir+'result_table.pickle'):
        with open(SubDir+'result_table.pickle', 'rb') as f:
            sim_table = pickle.load(f) 
    else:
        return BuildSimTable()

    sim_tuple = sorted(sim_table.items(), key=lambda d:d[1], reverse = True)  
    return sim_tuple[:n]   

def FindMatchCat(C):
    """the main algorithm, when C is belong to a exist category, return this cat.
    else make a new cat.
    
    Arguments:
        C {dict} -- reference to pickle format file description above.
    
    Returns:
        C {dict} -- reference to pickle format file des. above.  return the cat. of current data belong to. preparing for further process.    
        
    """
    breakout = False
    count = 0
    curent_set = init_factor_set(C['data'][0])
    for x in Path(SubDir).iterdir():
        if x.is_file() and x.match('*.pkl'):  
            with open(str(x), 'rb') as f:
                cat = pickle.load(f) 
            for S in cat['data']:
                count=count+1
                cat_set = init_factor_set(S)                
                result = compare_factor_set(cat_set, curent_set)
                print('current compare with ',cat['_id'],', result is: ',result)
                if result < cat_parameters['S']:
                    cat['data'].append(serialize_factor_set(curent_set))
                    breakout = True
                    # produce predict data
                    cat['Predict_M15x4']['up'] += C['Predict_M15x4']['up']
                    cat['Predict_M15x4']['down'] += C['Predict_M15x4']['down']
                    cat['Predict_M15x4']['null'] += C['Predict_M15x4']['null']
                    cat['Predict_M30x4']['up'] += C['Predict_M30x4']['up']
                    cat['Predict_M30x4']['down'] += C['Predict_M30x4']['down']
                    cat['Predict_M30x4']['null'] += C['Predict_M30x4']['null']
                    cat['Predict_H1x4']['up'] += C['Predict_H1x4']['up']
                    cat['Predict_H1x4']['down'] += C['Predict_H1x4']['down']
                    cat['Predict_H1x4']['null'] += C['Predict_H1x4']['null']
                    cat['Predict_H4x4']['up'] += C['Predict_H4x4']['up']
                    cat['Predict_H4x4']['down'] += C['Predict_H4x4']['down']
                    cat['Predict_H4x4']['null'] += C['Predict_H4x4']['null']
                    cat['Predict_D1x4']['up'] += C['Predict_D1x4']['up']
                    cat['Predict_D1x4']['down'] += C['Predict_D1x4']['down']
                    cat['Predict_D1x4']['null'] += C['Predict_D1x4']['null']
                    with open(str(x), 'wb') as f:              
                        pickle.dump(cat, f)
                    return cat
                
    if not breakout: 
        with open(SubDir+str(count)+'.pkl', 'wb') as f:  
            # print(C) 
            C['_id'] = str(count)           
            pickle.dump(C, f)
        return curent_set

if __name__ == '__main__':
    print('unit test')
    BuildSimTable2DB(eur_h4, eur_h4_dist,['H4','D1','MACD','TIME'])
