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

SubDir = './category/eur_m15/'
f = open(SubDir+'cfg.json')
cat_parameters = json.loads(f.read())
f.close()

def MakeCurrentSet(dtf, curent_time):
    """make current data to a factor set
    
    Returns:
        json -- pickle like obj
    """
    dtf = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.data_file + '.csv'))
    dtf['']
    return {} 

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
    