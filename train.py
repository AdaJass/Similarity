from category import *

if __name__ == '__main__':
    filebasename = 'EURUSD'
    min_period = 'M15'
    day_change = 0.01
    df = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), filebasename + '_'+min_period+'.csv'))  
    count = 0
    # here should be a larger for loop 
    # count the category pickle files 
    # and fine tuning value S to make pickle files little
    for i in range(0, len(df), 96):
        current_time = df.iloc[i].date
        cu_set = MakeCurrentSet(filebasename, curent_time, min_period, day_change)
        count += 1
        FindMatchCat(cu_set)
        print(count)
    