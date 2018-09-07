from category import *
from mongoconnect import *

if __name__ == '__main__':    
    min_period = 'H4'
    day_change = 0.008
    df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), filebasename + '_'+min_period+'.csv'))  
    count = 0
    # here should be a larger for loop 
    # count the category pickle files 
    # and fine tuning value S to make pickle files little
    for i in range(11006, len(df), 2):
        current_time = df.iloc[i].date
        cu_set = MakeCurrentSet(filebasename, current_time, min_period, day_change)
        count += 1
        BuildCat2DB(cu_set, eur_d1)
        print(count) 
    