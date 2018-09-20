from pymongo import MongoClient
__client = MongoClient('mongodb://admin:%2Bbeijing2017@node0:27017')

eur_m15 = __client.forex_category.eur_m15
eur_m15_dist = __client.forex_category.eur_m15_dist
eur_h1 = __client.forex_category.eur_h1
eur_h1_dist = __client.forex_category.eur_h1_dist
eur_h4 = __client.forex_category.eur_h4
eur_h4_dist = __client.forex_category.eur_h4_dist
eur_d1 = __client.forex_category.eur_d1
eur_d1_dist = __client.forex_category.eur_d1_dist

database ={
    'eur_m15' : eur_m15,
    'eur_m15_dist' : eur_m15_dist,
    'eur_h1' : eur_h1,
    'eur_h1_dist' : eur_h1_dist,
    'eur_h4' : eur_h4,
    'eur_h4_dist' : eur_h4_dist,
    'eur_d1' : eur_d1,
    'eur_d1_dist' : eur_d1_dist 
}

if __name__ == '__main__':
    print(list(eur_d1.find({},projection={'_id':False})))
