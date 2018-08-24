periods = ['M15','M30','H1','H4','D1','W1']

basename='./data/EURUSD_'

for p in periods:
    with open(basename+p+'.csv','r') as f:
        lines = f.readlines()
    for index,line in enumerate(lines):
        lines[index] = lines[index][:10] +'-' + lines[index][11:]
        # print(lines[index])
        # exit()
    with open(basename+p+'.csv','w') as f:
        f.writelines(lines)
