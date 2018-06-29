import csv
res = set()
with open("../data/EvolutionPopUSA_MainData.csv") as f:
    count = 1
    for line in f:
        s = line.split("\n")
        t = s[0].split(",")[1]
        if count==1:
            count +=1
            continue
        if "Featuring" in t:
            continue
        if "ft" in t:
            continue
        if "Feat." in t:
            continue
        if "And" in t:
            continue
        if "&" in t:
            continue
        if '\"' in t:
            t = t.replace('\"','')
        res.add(t)
        count+=1

with open('../data/evolution-artist.csv', 'w') as csvfile:
    for i in res:
        csvfile.write(i+"\n")

l = []
with open('../data/evolution-artist.csv', 'r') as f:
    for i in f:
        if "ft" in i:
            print(i)
            continue
        if "ft" in i:
            continue
        if '\"' in i:
            i = i.replace('\"','')
        l.append(i)

with open('../data/evolution-artist.csv', 'w') as csvfile:
    for i in l:
        csvfile.write(i)

def to_lower(x):
    return x.lower()

def to_upper(x):
    return x.upper()

l1 = []
with open('../data/evolution-artist.csv', 'r') as readf:
    for i in readf:
        l1.append(i)

with open('../data/lower-artist.csv', 'w') as writef:
    for i in l1:
        writef.write(i.lower())

l2 = []
with open('../data/evolution-artist.csv', 'r') as readf:
    for i in readf:
        l2.append(i.upper())

with open('../data/upper-artist.csv', 'w') as writef:
    for i in l2:
        writef.write(i.upper())


