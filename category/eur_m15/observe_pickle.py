import pickle

with open('./category/eur_m15/result_table.pickle', 'rb') as f:
    data=pickle.load(f)
    print(data)