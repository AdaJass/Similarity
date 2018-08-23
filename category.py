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
}
"""

import pickle
import pandas as pd
import os
import json
from pathlib import Path
from factor import init_factor_set, serialize_factor_set,compare_factor_set
from datetime import datetime


SubDir = './category/eur_m15/'
f = open(SubDir+'cfg.json')
cat_parameters = json.loads(f.read())
f.close()

def MakeCurrentSet(filebasename,curent_time, min_period, day_change, macd_period='D1', rsi_period='D1', time_period='H1'):
    """make current data to a factor set
    
    Returns:
        json -- pickle like obj
    """
    p_order = ['M15', 'M30', 'H1', 'H4', 'D1', 'W1']
    p_change={'M15':0.3, 'M30':0.3, 'H1':0.4, 'H4':0.5, 'D1':1}
    valid_p = p_order[p_order.index(min_period):]
    df_set={}
    out = {
        'Data': []       
    }
    f_set = {}
    for p in valid_p:
        df_set[p] = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), filebasename + '_' + p +'.csv'))  
        c_index = df_set[p][df_set[p].date <= curent_time][-1] 
        f_set[p]={}
        #alpha, scale, data_id, data_file, data_start, data_end
        f_set[p]['alpha'] = cat_parameters[p+'_alpha'] 
        f_set[p]['scale'] = cat_parameters[p+'_scale'] 
        f_set[p]['data_id'] = p
        f_set[p]['data_file'] = cat_parameters[filebasename + '_' + p] 
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
    
    f_set['MACD']['alpha'] = cat_parameters['MACD_alpha'] 
    f_set['MACD']['scale'] = cat_parameters['MACD_scale'] 
    f_set['MACD']['data_id'] = 'MACD'
    f_set['MACD']['data_file'] = cat_parameters[filebasename + '_'+macd_period] 
    f_set['MACD']['data_start'] = f_set[macd_period]['data_start']
    f_set['MACD']['data_end'] = f_set[macd_period]['data_end']

    f_set['RSI']['alpha'] = cat_parameters['RSI_alpha'] 
    f_set['RSI']['scale'] = cat_parameters['RSI_scale'] 
    f_set['RSI']['data_id'] = 'RSI'
    f_set['RSI']['data_file'] = cat_parameters[filebasename + '_'+rsi_period] 
    f_set['RSI']['data_start'] = f_set[rsi_period]['data_start']
    f_set['RSI']['data_end'] = f_set[rsi_period]['data_end']

    f_set['TIME']['alpha'] = cat_parameters['TIME_alpha'] 
    f_set['TIME']['scale'] = cat_parameters['TIME_scale'] 
    f_set['TIME']['data_id'] = 'TIME'
    f_set['TIME']['data_file'] = cat_parameters[filebasename + '_'+time_period] 
    f_set['TIME']['data_start'] = f_set[time_period]['data_start']
    f_set['TIME']['data_end'] = f_set[time_period]['data_end']

    out['data'].append(f_set)
    return out

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
                cat = pickle.loads(f) 
            for S in cat['data']:
                count=count+1
                cat_set = init_factor_set(S)                
                result = compare_factor_set(cat_set, curent_set)
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
                    cat['Predict_H1']['up'] += C['Predict_H1']['up']
                    cat['Predict_H1']['down'] += C['Predict_H1']['down']
                    cat['Predict_H1']['null'] += C['Predict_H1']['null']
                    cat['Predict_H4x4']['up'] += C['Predict_H4x4']['up']
                    cat['Predict_H4x4']['down'] += C['Predict_H4x4']['down']
                    cat['Predict_H4x4']['null'] += C['Predict_H4x4']['null']
                    cat['Predict_D1x4']['up'] += C['Predict_D1x4']['up']
                    cat['Predict_D1x4']['down'] += C['Predict_D1x4']['down']
                    cat['Predict_D1x4']['null'] += C['Predict_D1x4']['null']
                    with open(str(x), 'wb') as f:              
                        pickle.dumps(cat, f)
                    return cat
                
    if not breakout: 
        with open(SubDir+str(count)+'.pkl', 'wb') as f:              
            pickle.dumps(C, f)
        return curent_set
                

if __name__ == '__main__':
    print('unit test')