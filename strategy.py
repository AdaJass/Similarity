from mongoconnect import *
from category import *

para_period = 'd1'
para_percent = 20
compare_set = ['D1','W1','MACD','TIME','RSI','EMA']
db_dist = database['eur_'+para_period+'_dist']
db_pattern = database['eur_'+para_period]

dist_list = list(db_dist.find({},projection={'_id':False}))
dist_list = sorted(dist_list, key=lambda x:x['result'])
pattern_list = list(db_pattern.find({},projection={'_id':False}))
pattern_dict={}
for pt in pattern_list:
    pattern_dict[pt['id']] = pt

percent_dist_list = dist_list[:int(len(dist_list)/para_percent)]
treshold = percent_dist_list[-1]['result']

def PatternRate(pattern):
    available_set = []
    for fset in percent_dist_list:
        key1 = fset['key1']
        key2 = fset['key2']
        pt1 = pattern_dict[key1]
        pt2 = pattern_dict[key2]
        c1 = init_factor_set(pt1['data'][0])
        c2 = init_factor_set(pt2['data'][0])
        c = init_factor_set(pattern['data'][0])
        r1 = compare_factor_set(c, c1, compare_set)
        r2 = compare_factor_set(c, c2, compare_set)
        if r1 < treshold:
            available_set.append(key1)
        if r2 < treshold:
            available_set.append(key2)

    up, down, null = 0,0,0
    pivotP = compare_set[0]
    for se in available_set:
        pt = pattern_dict[se]
        predict = pt.get('Predict_'+pivotP+'x4')
        up+=predict['up']
        down+=predict['down']
        null+=predict['null']

    return up, down, null

if __name__ == '__main__':
    min_period = 'D1'
    day_change = 0.008
    df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), filebasename + '_'+min_period+'.csv'))  
    # here should be a larger for loop 
    # count the category pickle files 
    # and fine tuning value S to make pickle files little
    decision, right_decision = 0,0
    for i in range(15690, len(df), 3):
        current_time = df.iloc[i].date
        cu_set = MakeCurrentSet(filebasename, current_time, min_period, day_change)
        up,down,null = PatternRate(cu_set)


        