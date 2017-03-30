import pandas as pd
import datetime as dt
from datetime import datetime
from datetime import time
import csv
import sys


df = pd.read_csv("/Users/Mru/MSISstudymaterial/GRAdocuments/UserBehavior_input.csv")


users = df['bvid']
u = set(users)
users = list(u)
print(len(users))

#fill blank prodid values with 0
df = df.fillna(0)

dict = {}

#To store start time of each user
start_time_arr = {}
prod_id ={}

#find starttime of each user
df_st = pd.DataFrame()
df_st = df[['bvid','dt']]
df_st = df_st.groupby('bvid').first().reset_index()


for rec in df_st.itertuples():
    user= getattr(rec,'bvid')
    start_time_arr[user]= getattr(rec, 'dt')

#logic to find session data

print("Reached looping")
#loop through users and their first session start time
j=0

start = datetime.now()
for user in users:

    if (j%1000 == 0):
        print(j)
    j+=1
    count_ask = 0
    count_ratings =0
    sum_= 0
    count=1
    prod_id={}
    diff=0
    st_time = datetime.strptime(start_time_arr[user], "%m/%d/%y %H:%M")
    n=0
    m=len(df)
    i=0

    #loop through dataframe to findrecords for this user
    for rec in (df[n:m].loc[df['bvid'] == user]).itertuples():
        i+=1

        if user == getattr(rec, 'bvid'):
            end_time = datetime.strptime(getattr(rec, 'dt'), "%m/%d/%y %H:%M")
            diff = end_time - st_time
            diff = diff.total_seconds() / 60
            if diff < 1440 :

                #to find prodcuct ids and count of records for those product ids
                if int(getattr(rec, 'productid')) in prod_id.keys() and int(getattr(rec, 'productid')) != 0:
                    # value_prod.append(int(getattr(rec, 'productid')))
                    prod_id[int(getattr(rec, 'productid'))] += 1
                elif int(getattr(rec, 'productid')) != 0:
                    prod_id[int(getattr(rec, 'productid'))] = 1

                #to find features and number of records of that feature
                if getattr(rec, 'bvproduct') == 'AskAndAnswer':
                    count_ask+=1
                elif getattr(rec, 'bvproduct') == 'RatingsAndReviews':
                    count_ratings+=1

                dict[str(user) + '_' + str(count)] = {}
                dict[str(user) + '_' + str(count)]['productid'] = prod_id
                dict[str(user) + '_' + str(count)]['dt'] = {}
                dict[str(user) + '_' + str(count)]['dt']['starttime'] = datetime.strftime(st_time, "%m/%d/%y %H:%M")
                dict[str(user) + '_' + str(count)]['dt']['endtime'] = getattr(rec, 'dt')
                dict[str(user) + '_' + str(count)]['bvproduct'] = {}
                dict[str(user) + '_' + str(count)]['bvproduct']['RatingsAndReviews'] = count_ratings
                dict[str(user) + '_' + str(count)]['bvproduct']['AskAndAnswer'] = count_ask
                dict[str(user) + '_' + str(count)]['Duration'] = diff
                dict[str(user) + '_' + str(count)]['NextSessionStartsIn'] = {}
                prev_rec = rec
                continue


            else:
                #if sum of diff is greater than 35 , check difference between current record anf its previous record to see if it was inactive
                prev_end_time = datetime.strptime(getattr(prev_rec, 'dt'), "%m/%d/%y %H:%M")
                end_time = datetime.strptime(getattr(rec, 'dt'), "%m/%d/%y %H:%M")
                prev_diff = end_time - prev_end_time
                prev_diff = prev_diff.total_seconds()/60

                #print(" end time is : ", end_time,  "previous record is : ", prev_end_time, "user is : ", user, 'prev difference is : ' , prev_diff)
                if prev_diff < 1440:
                    #inactivity is not greater than 35 , so we consider it as a part of first session
                    if int(getattr(rec, 'productid')) in prod_id.keys() and int(getattr(rec, 'productid')) != 0:
                        #value_prod.append(int(getattr(rec, 'productid')))
                        prod_id[int(getattr(rec, 'productid'))] += 1
                    elif int(getattr(rec, 'productid')) != 0:
                        prod_id[int(getattr(rec, 'productid'))] = 1
                    #value_feat.append(getattr(rec, 'bvproduct'))
                    #value_time.append(getattr(rec, 'dt'))
                    if getattr(rec, 'bvproduct') == 'AskAndAnswer':
                        count_ask += 1
                    elif getattr(rec, 'bvproduct') == 'RatingsAndReviews':
                        count_ratings += 1
                    dict[str(user) + '_' + str(count)] = {}
                    dict[str(user) + '_' + str(count)]['productid'] = prod_id
                    dict[str(user) + '_' + str(count)]['dt'] = {}
                    dict[str(user) + '_' + str(count)]['dt']['starttime'] = datetime.strftime(st_time, "%m/%d/%y %H:%M")
                    dict[str(user) + '_' + str(count)]['dt']['endtime'] = datetime.strftime(end_time, "%m/%d/%y %H:%M")
                    dict[str(user) + '_' + str(count)]['bvproduct'] = {}
                    dict[str(user) + '_' + str(count)]['bvproduct']['RatingsAndReviews'] = count_ratings
                    dict[str(user) + '_' + str(count)]['bvproduct']['AskAndAnswer'] = count_ask
                    dict[str(user) + '_' + str(count)]['Duration'] = diff
                    dict[str(user) + '_' + str(count)]['NextSessionStartsIn'] = {}
                    prev_rec = rec

                else:
                    #print("inside last else :", user)
                    if ((str(user) + '_' + str(count)) in dict.keys()):
                        dict[str(user) + '_' + str(count)]['NextSessionStartsIn'] = prev_diff
                    else:
                        dict[str(user) + '_' + str(count)] = {}
                        dict[str(user) + '_' + str(count)]['NextSessionStartsIn'] = prev_diff

                    count += 1

                    count_ask = 0
                    count_ratings = 0
                    prod_id = {}
                    diff = 0

                    if int(getattr(rec, 'productid')) in prod_id.keys() and int(getattr(rec, 'productid')) != 0:
                        # value_prod.append(int(getattr(rec, 'productid')))
                        prod_id[int(getattr(rec, 'productid'))] += 1
                    elif int(getattr(rec, 'productid')) != 0:
                        prod_id[int(getattr(rec, 'productid'))] = 1
                    # value_feat.append(getattr(rec, 'bvproduct'))
                    # value_time.append(getattr(rec, 'dt'))
                    if getattr(rec, 'bvproduct') == 'AskAndAnswer':
                        count_ask += 1
                    elif getattr(rec, 'bvproduct') == 'RatingsAndReviews':
                        count_ratings += 1
                    dict[str(user) + '_' + str(count)] = {}
                    dict[str(user) + '_' + str(count)]['productid'] = prod_id
                    dict[str(user) + '_' + str(count)]['dt'] = {}
                    dict[str(user) + '_' + str(count)]['dt']['starttime'] = datetime.strftime(st_time, "%m/%d/%y %H:%M")
                    dict[str(user) + '_' + str(count)]['dt']['endtime'] = datetime.strftime(end_time, "%m/%d/%y %H:%M")
                    dict[str(user) + '_' + str(count)]['bvproduct'] = {}
                    dict[str(user) + '_' + str(count)]['bvproduct']['RatingsAndReviews'] = count_ratings
                    dict[str(user) + '_' + str(count)]['bvproduct']['AskAndAnswer'] = count_ask
                    dict[str(user) + '_' + str(count)]['Duration'] = diff
                    dict[str(user) + '_' + str(count)]['NextSessionStartsIn'] = {}

                    count_ask = 0
                    count_ratings = 0
                    prod_id = {}
                    diff = 0

                    st_time = datetime.strptime(getattr(rec, 'dt'), "%m/%d/%y %H:%M")

                    prev_rec = rec
                    continue
    n=i

end= datetime.now()
print(end-start)

keys = sorted(dict)

print("renaming sessions")

df_out = pd.DataFrame(dict)
df_out = df_out.transpose()
df_out.to_csv("/Users/Mru/MSISstudymaterial/GRAdocuments/UserBehavior_output.csv")