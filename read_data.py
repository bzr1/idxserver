# COMP90024 Cluster and Cloud Computing 
# Assignment 1 
# Author: 
# Zhiran Bai    Student ID : 1324780


import json

# Core library used in the program. 
from mpi4py import MPI
import os
import time

# open .json file    
with open("sal.json", "r", encoding = 'utf-8') as f:
    sal = json.load(f)

# MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
start_time = time.time()

## ignore rural infomation and transfer into dictionary format (key:suburb, value:gratecity)
GrateCityDic = {} 
for k,v in sal.items():
    if v["gcc"][1] == "g":
        GrateCityDic[k] = v["gcc"]
    elif v["gcc"] == "8acte" or v["gcc"] == "9oter":
        GrateCityDic[k] = v["gcc"]

TinCity = {'1gsyd':0,'2gmel':0,'3gbri':0,'4gade':0,'5gper':0,'6ghob':0,"7gdar":0,"8acte":0,"9oter":0}

# Most twitter dictionary in format : MostT = {"author_id": number of twitters sent by author}
MostT = {}
DiffCity = {}

# Process one piece of tweet
def process(dic):
    authorid = dic["data"]["author_id"]
    location = dic["includes"]["places"][0]["full_name"]
    suburb = location.split(",")[0].lower()

    # Link the tweet to the author,help for a total of tweets (Task 1)
    if MostT.get(authorid) != None:
            MostT[authorid] += 1
    else:
        MostT[authorid] = 1
    
     # Count the amount of data group by particular grate city (Task 2)
    if  GrateCityDic.get(suburb) != None:
        city=GrateCityDic[suburb]
        TinCity[city] += 1
        
        # Collect the location which tweeted from of each author (Task 3)
        if  DiffCity.get(authorid) == None:
            DiffCity[authorid] = {'1gsyd':0,'2gmel':0,'3gbri':0,'4gade':0,'5gper':0,'6ghob':0,"7gdar":0,"8acte":0,"9oter":0}
        DiffCity[authorid][city] += 1
    
    return 

# Slpit file for multi-thread
total_bytes = os.path.getsize("bigTwitter.json")
single_job = total_bytes // size
begin = rank * single_job
end = (rank + 1) * single_job

# Now, we start to process entire tweets from json file
with open("bigTwitter.json", "r",encoding='utf-8') as t:
    
    tweet = ""
    t.seek(begin)
    
    if rank == 0:
        t.readline()
    line = t.readline()
    
    while line:  
        if line == "  },\n" or line == "  }\n":
            tweet += "  }"
            try: 
                tweet_dic = json.loads(tweet)
                process(tweet_dic)
            except:
                tweet = ""
            if t.tell() >= end:
                break
            tweet = ""
        else:
            tweet += line
            
        # Progress the next line
        line = t.readline()

TinCity_multi_list = comm.gather(TinCity, root = 0)
MostT_multi_list = comm.gather(MostT, root = 0)
DiffCity_multi_list = comm.gather(DiffCity, root = 0)

if rank == 0 :
    for city in TinCity.keys():
        for i in range(1, size):
            TinCity[city] += TinCity_multi_list[i][city]
    # print(TinCity) 
    for i in range(1,size):
        for key in MostT_multi_list[i].keys():
            if key not in MostT:
                MostT[key]=MostT_multi_list[i][key]
            else:
                MostT[key]+=MostT_multi_list[i][key]
    
    for i in range(1,size):
         for key in DiffCity_multi_list[i].keys():
            if key not in DiffCity:
                DiffCity[key]=DiffCity_multi_list[i][key]
            else:
                for city in DiffCity[key]:
                    DiffCity[key][city]+=DiffCity_multi_list[i][key][city]

    
    # Produce output for task 1 (MostT)
    print("Task 1:\n")
    # Sort dictionary items by value and get top 10 entries
    top_10 = sorted(MostT.items(), key=lambda x: x[1], reverse=True)[:10]
    rank = 1
    print("{:<10} {:<25} {:<10}".format("Rank", "Author Id", "Number of Tweets Made"))
    
    for user in top_10:
        print("#{:<9} {:<25} {:=9}".format(rank,user[0],user[1]))
        rank += 1

    # Output print for Task 2 (TinCity)
    print("")
    print("Task 2:\n")

    # Print the header row
    print("{:<30} {:30}".format("Greater Capital City", "Number of Tweets Made"))
    
    # Print each row of the table
    list = ['(Greater Sydney)','(Greater Melbourne)','(Greater Brisbane)','(Greater Adelaide)','(Greater Perth)','(Greater Hobart)','(Greater Darwin)','(Greater Canberra)','(Great Other Territories)']
    idx = 0
    
    for key, value in TinCity.items():
        key += list[idx]
        print("{:<30} {:=9}".format(key, value))
        idx += 1

    # Output for task 3(DiffCity)
    print("")
    print("Task 3:\n")
    user_entry = []

    for user,tweet_list in DiffCity.items():
        unique_counter = 0
        tweet_counter = 0
        for value in tweet_list.values():
            if value != 0:
                unique_counter += 1
            tweet_counter += value
        
        user_entry.append((user, unique_counter, tweet_counter))

    # Define a custom comparison function to sort tuples with the same second value
    # based on the third value
    def my_cmp(x):
        return (-x[1], -x[2])

    # Sort the list based on the second and third elements of each tuple
    top_10_uniqCity_per_author = sorted(user_entry, key=my_cmp)[:10]

    print("{:<10} {:<25} {:<10}".format("Rank","Author Id", "Number of Unique City Locations and #Tweets"))
    rankraw = 0
    while rankraw < 10:
        
        rank_uniq_city = rankraw + 1
        AuthorID = top_10_uniqCity_per_author[rankraw][0]
        uniq_city_num = top_10_uniqCity_per_author[rankraw][1]
        Tnum = top_10_uniqCity_per_author[rankraw][2]
        uniq_city_printout = ""
        long_dic = DiffCity.get(AuthorID)
        for city,value in long_dic.items():
            if value != 0:
                uniq_city_printout += "#"+str(value) + city[1:]+", "
        uniq_city_printout = uniq_city_printout.rstrip(", ")
        
        print("#{:<9} {:<25} {:=1}(#{} tweets - {})".format(rank_uniq_city,AuthorID,uniq_city_num,Tnum,uniq_city_printout))
        rankraw += 1
    
    # # End of task 3.
    print("")
