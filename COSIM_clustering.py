import pandas as pd

def sortCluster(cluster_values):
    max_len=len(cluster_values[0])
    for j in range(0,len(cluster_values)-1):
        for i in range(1,len(cluster_values)):
            if len(cluster_values[i])>= max_len:
                temp=cluster_values[i]
                cluster_values[i]=cluster_values[i-1]
                cluster_values[i-1]=temp
    return cluster_values

df = pd.read_csv('/Users/Mru/MSISstudymaterial/GRAdocuments/cosim_test.csv')

cluster= {}
values=[]

bvid1 = df['bvid1']
bvid2 = df['bvid2']
cosim= df['sim']

for (bv1,bv2,co) in zip(bvid1,bvid2,cosim):
    if (float(co) >= 0.9):
        if bv1 in cluster.keys():
            key=bv1
            cluster[key].append(bv2)

        else:
            key = bv1
            value = []
            value.append(bv2)
            cluster[key] = value
keys = cluster.keys()

for key in keys:
     cluster[key].append(key)

values_list= []
for key in keys:
    values_list.append(cluster[key])

print(values_list)

l =values_list
out = []
while len(l)>0:
    first, *rest = l

    first = set(first)
    lf = -1
    while len(first)>lf:
        lf = len(first)
        rest2 = []
        for r in rest:
            if len(first.intersection(set(r)))>0:
                first |= set(r)
            else:
                rest2.append(r)
        rest = rest2

    out.append(first)
    l = rest

out= sortCluster(out)
print(type(out))
sum_=0
i=0
for elem in out:
    print("Length of : " ,i , " cluster is  " , len(elem))
    sum_+= len(elem)
    i+=1

print('Total user ids present in results  : ' , sum_)

for elem in out:
    print (elem)

df_out = pd.DataFrame(out)
df_out= pd.DataFrame.transpose(df_out)
df_out.to_csv('/Users/Mru/MSISstudymaterial/GRAdocuments/cosin_out.csv', index=False)