import pickle

with open('./category/eur_m15/0.pkl', 'rb') as f:
    data=pickle.load(f)
    print(data)