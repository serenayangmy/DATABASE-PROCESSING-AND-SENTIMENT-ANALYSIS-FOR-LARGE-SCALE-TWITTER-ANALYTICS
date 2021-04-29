#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 20:09:08 2021

@author: serenayang
"""

import urllib
import json
import time
import sqlite3

#question 1-a
file1 = open("dataset1.txt", "wb")
file2 = open("dataset2.txt", "wb")
file3 = open("dataset3.txt", "wb")
tweetData = 'http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt'
webFD = urllib.request.urlopen(tweetData)

#50000 tweets
start = time.time()
count1 = 0
for line in webFD:
    file1.write(line)
    count1 += 1
    print(count1)
    if count1 == 50000:
        break
end = time.time()

#50000 tweets time (289.9412348270416)
time1 = end - start
print(str(time1) + ' seconds')



#200000 tweets
start = time.time()
count2 = 0
for line in webFD:
    file2.write(line)
    count2 += 1
    print(count2)
    if count2 == 200000:
        break 
    
end = time.time()

#200000 tweets time (800.8470940589905）
time2 = end - start
print(str(time2) + ' seconds')


#600000 tweets
start = time.time()
count3 = 0
for line in webFD:
    file3.write(line)
    count3 += 1
    print(count3)
    if count3 == 600000:
        break
    
end = time.time()

#600000 tweets time (2002.7778232097626）
time3 = end - start
print(str(time3) + ' seconds')


#question 1-b
file1b = open("dataset3.txt", 'r')

x, y, z = 0, 0, 0   
for i in range(600000):
    n,m,u = [],[],[]
    allTweets = file1b.readline()
    tweet = json.loads(allTweets)


    replyid = tweet['in_reply_to_user_id']
    replyscreenname = tweet['in_reply_to_screen_name']
    screenname = tweet['user']['screen_name']
    
    if replyid == None:
        a = 0
    else:
        a = len(str(replyid))
    
    if replyscreenname == None:
        b = 0
    else:
        b = len(str(replyscreenname))
    
    if screenname == None:
        c = 0
    else:
        c = len(str(screenname))
    
    if x < a:
        x = a
    if y < b:
        y = b
    if z < c:
        z = c
    print(i)

print(x)
print(y)
print(z)

#question 1-c
conn = sqlite3.connect('ExamQ1c1.db')
cursor = conn.cursor()

dropTweet = """DROP TABLE TWEET"""
cursor.execute(dropTweet)

dropUser = """DROP TABLE USER"""
cursor.execute(dropUser)

dropGeo = """DROP TABLE GEO"""
cursor.execute(dropGeo)


Tweettable = """
CREATE TABLE TWEET(
    created_at               VARCHAR(100),
    id_str                   VARCHAR(30)  primary key UNIQUE,
    user_id                  VARCHAR(30),
    text                     VARCHAR(1000),
    source                   VARCHAR(100),
    in_reply_to_user_id      VARCHAR(20),
    in_reply_to_screen_name  VARCHAR(20),
    in_reply_to_status_id    VARCHAR(50),
    retweet_count            NUMBER,
    contributors             VARCHAR(50)
    
);
"""
cursor.execute(Tweettable)

Usertable = """
CREATE TABLE USER(
    id              VARCHAR(30) primary key UNIQUE,
    name            VARCHAR(50),
    screen_name     VARCHAR(20),
    description     VARCHAR(50),
    friends_count   VARCHAR(50),
    
    FOREIGN KEY(id) 
        REFERENCEs TWEET(id_str)
);
"""
cursor.execute(Usertable)

Geotable = """
CREATE TABLE GEO(
    ID          VARCHAR(30) primary key UNIQUE,
    type        VARCHAR(30),
    longitude   VARCHAR(10),
    latitude    VARCHAR(100),
    
    CONSTRAINT gep_fk
        FOREIGN KEY (ID)
            REFERENCES TWEET (id_str)

);
"""
cursor.execute(Geotable)

tweetData = 'http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt'
webFD = urllib.request.urlopen(tweetData)

#50000 tweets    
start = time.time()   
count = 0
for line in webFD:
    try:
        tempT, tempU, tempG = [],[],[]
        tweet = json.loads(line)
        count += 1
        print(count)
        
        if count == 50001:
            break
        else:
            tempT.append(tweet['created_at'])
            tempT.append(tweet['id_str'])
            tempT.append(tweet['id'])
            tempT.append(tweet['text'])
            tempT.append(tweet['source'])
            tempT.append(tweet['in_reply_to_user_id'])
            tempT.append(tweet['in_reply_to_screen_name'])
            tempT.append(tweet['in_reply_to_status_id'])
            tempT.append(tweet['retweet_count'])
            tempT.append(tweet['contributors'])
            cursor.execute("INSERT INTO TWEET VALUES(?,?,?,?,?,?,?,?,?,?);",tempT)
                
            tempU.append(tweet['id_str'])
            tempU.append(tweet['user']['name'])
            tempU.append(tweet['user']['screen_name'])
            tempU.append(tweet['user']['description'])
            tempU.append(tweet['user']['friends_count'])
            cursor.execute("INSERT INTO USER VALUES(?,?,?,?,?);",tempU)
                
            
            if tweet['coordinates'] == None:
                continue
            else:
                tempG.append(tweet['id_str'])
                tempG.append(tweet['geo']['type'])
                tempG.append(tweet['geo']['coordinates'][0])
                tempG.append(tweet['geo']['coordinates'][1])   
                cursor.execute("INSERT INTO GEO VALUES(?,?,?,?);",tempG)
    except:
        pass

end = time.time()

#50000 tweets time (343.7849521636963)
time1 = end - start
print(str(time1) + ' seconds')


#200000 tweets
conn = sqlite3.connect('ExamQ1c2.db')
cursor = conn.cursor()

dropTweet = """DROP TABLE TWEET"""
cursor.execute(dropTweet)

dropUser = """DROP TABLE USER"""
cursor.execute(dropUser)

dropGeo = """DROP TABLE GEO"""
cursor.execute(dropGeo)


Tweettable = """
CREATE TABLE TWEET(
    created_at               VARCHAR(100),
    id_str                   VARCHAR(30)  primary key UNIQUE,
    user_id                  VARCHAR(30),
    text                     VARCHAR(1000),
    source                   VARCHAR(100),
    in_reply_to_user_id      VARCHAR(20),
    in_reply_to_screen_name  VARCHAR(20),
    in_reply_to_status_id    VARCHAR(50),
    retweet_count            NUMBER,
    contributors             VARCHAR(50)
    
);
"""
cursor.execute(Tweettable)

Usertable = """
CREATE TABLE USER(
    id              VARCHAR(30) primary key UNIQUE,
    name            VARCHAR(50),
    screen_name     VARCHAR(20),
    description     VARCHAR(50),
    friends_count   VARCHAR(50),
    
    FOREIGN KEY(id) 
        REFERENCEs TWEET(id_str)
);
"""
cursor.execute(Usertable)

Geotable = """
CREATE TABLE GEO(
    ID          VARCHAR(30) primary key UNIQUE,
    type        VARCHAR(30),
    longitude   VARCHAR(10),
    latitude    VARCHAR(100),
    
    CONSTRAINT gep_fk
        FOREIGN KEY (ID)
            REFERENCES TWEET (id_str)

);
"""
cursor.execute(Geotable)

tweetData = 'http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt'
webFD = urllib.request.urlopen(tweetData)

#200000 tweets    
start = time.time()   
count = 0
for line in webFD:
    try:
        tempT, tempU, tempG = [],[],[]
        tweet = json.loads(line)
        count += 1
        print(count)
        
        if count == 200001:
            break
        else:
            tempT.append(tweet['created_at'])
            tempT.append(tweet['id_str'])
            tempT.append(tweet['id'])
            tempT.append(tweet['text'])
            tempT.append(tweet['source'])
            tempT.append(tweet['in_reply_to_user_id'])
            tempT.append(tweet['in_reply_to_screen_name'])
            tempT.append(tweet['in_reply_to_status_id'])
            tempT.append(tweet['retweet_count'])
            tempT.append(tweet['contributors'])
            cursor.execute("INSERT INTO TWEET VALUES(?,?,?,?,?,?,?,?,?,?);",tempT)
                
            tempU.append(tweet['id_str'])
            tempU.append(tweet['user']['name'])
            tempU.append(tweet['user']['screen_name'])
            tempU.append(tweet['user']['description'])
            tempU.append(tweet['user']['friends_count'])
            cursor.execute("INSERT INTO USER VALUES(?,?,?,?,?);",tempU)
                
            
            if tweet['coordinates'] == None:
                continue
            else:
                tempG.append(tweet['id_str'])
                tempG.append(tweet['geo']['type'])
                tempG.append(tweet['geo']['coordinates'][0])
                tempG.append(tweet['geo']['coordinates'][1])   
                cursor.execute("INSERT INTO GEO VALUES(?,?,?,?);",tempG)
    except:
        pass

end = time.time()

#200000 tweets time (1558.7821018695831)
time2 = end - start
print(str(time2) + ' seconds')


#600000 tweets 
conn = sqlite3.connect('ExamQ1c3.db')
cursor = conn.cursor()

dropTweet = """DROP TABLE TWEET"""
cursor.execute(dropTweet)

dropUser = """DROP TABLE USER"""
cursor.execute(dropUser)

dropGeo = """DROP TABLE GEO"""
cursor.execute(dropGeo)


Tweettable = """
CREATE TABLE TWEET(
    created_at               VARCHAR(100),
    id_str                   VARCHAR(30)  primary key UNIQUE,
    user_id                  VARCHAR(30),
    text                     VARCHAR(1000),
    source                   VARCHAR(100),
    in_reply_to_user_id      VARCHAR(20),
    in_reply_to_screen_name  VARCHAR(20),
    in_reply_to_status_id    VARCHAR(50),
    retweet_count            NUMBER,
    contributors             VARCHAR(50)
    
);
"""
cursor.execute(Tweettable)

Usertable = """
CREATE TABLE USER(
    id              VARCHAR(30) primary key UNIQUE,
    name            VARCHAR(50),
    screen_name     VARCHAR(20),
    description     VARCHAR(50),
    friends_count   VARCHAR(50),
    
    FOREIGN KEY(id) 
        REFERENCEs TWEET(id_str)
);
"""
cursor.execute(Usertable)

Geotable = """
CREATE TABLE GEO(
    ID          VARCHAR(30) primary key UNIQUE,
    type        VARCHAR(30),
    longitude   VARCHAR(10),
    latitude    VARCHAR(100),
    
    CONSTRAINT gep_fk
        FOREIGN KEY (ID)
            REFERENCES TWEET (id_str)

);
"""
cursor.execute(Geotable)

tweetData = 'http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt'
webFD = urllib.request.urlopen(tweetData)

#50000 tweets    
start = time.time()   
count = 0
for line in webFD:
    try:
        tempT, tempU, tempG = [],[],[]
        tweet = json.loads(line)
        count += 1
        print(count)
        
        if count == 600001:
            break
        else:
            tempT.append(tweet['created_at'])
            tempT.append(tweet['id_str'])
            tempT.append(tweet['id'])
            tempT.append(tweet['text'])
            tempT.append(tweet['source'])
            tempT.append(tweet['in_reply_to_user_id'])
            tempT.append(tweet['in_reply_to_screen_name'])
            tempT.append(tweet['in_reply_to_status_id'])
            tempT.append(tweet['retweet_count'])
            tempT.append(tweet['contributors'])
            cursor.execute("INSERT INTO TWEET VALUES(?,?,?,?,?,?,?,?,?,?);",tempT)
                
            tempU.append(tweet['id_str'])
            tempU.append(tweet['user']['name'])
            tempU.append(tweet['user']['screen_name'])
            tempU.append(tweet['user']['description'])
            tempU.append(tweet['user']['friends_count'])
            cursor.execute("INSERT INTO USER VALUES(?,?,?,?,?);",tempU)
                
            
            if tweet['coordinates'] == None:
                continue
            else:
                tempG.append(tweet['id_str'])
                tempG.append(tweet['geo']['type'])
                tempG.append(tweet['geo']['coordinates'][0])
                tempG.append(tweet['geo']['coordinates'][1])   
                cursor.execute("INSERT INTO GEO VALUES(?,?,?,?);",tempG)
    except:
        pass
    
end = time.time()

#600000 tweets time (3329.1564939022064)
time3 = end - start
print(str(time3) + ' seconds')


#question 1-d
#50000 tweets
conn = sqlite3.connect('ExamQ1d1.db')
cursor = conn.cursor()

dropTweet = """DROP TABLE TWEET"""
cursor.execute(dropTweet)

dropUser = """DROP TABLE USER"""
cursor.execute(dropUser)

dropGeo = """DROP TABLE GEO"""
cursor.execute(dropGeo)


Tweettable = """
CREATE TABLE TWEET(
    created_at               VARCHAR(100),
    id_str                   VARCHAR(30)  primary key UNIQUE,
    user_id                  VARCHAR(30),
    text                     VARCHAR(1000),
    source                   VARCHAR(100),
    in_reply_to_user_id      VARCHAR(20),
    in_reply_to_screen_name  VARCHAR(20),
    in_reply_to_status_id    VARCHAR(50),
    retweet_count            NUMBER,
    contributors             VARCHAR(50)
    
);
"""
cursor.execute(Tweettable)

Usertable = """
CREATE TABLE USER(
    id              VARCHAR(30) primary key UNIQUE,
    name            VARCHAR(50),
    screen_name     VARCHAR(20),
    description     VARCHAR(50),
    friends_count   VARCHAR(50),
    
    FOREIGN KEY(id) 
        REFERENCEs TWEET(id_str)
);
"""
cursor.execute(Usertable)

Geotable = """
CREATE TABLE GEO(
    ID          VARCHAR(30) primary key UNIQUE,
    type        VARCHAR(30),
    longitude   VARCHAR(10),
    latitude    VARCHAR(100),
    
    CONSTRAINT gep_fk
        FOREIGN KEY (ID)
            REFERENCES TWEET (id_str)

);
"""
cursor.execute(Geotable)

file1b = open("dataset1.txt", 'r')

start = time.time()

for i in range(50001):
    try:
        tempT, tempU, tempG = [],[],[]
        allTweets = file1b.readline()
        tweet = json.loads(allTweets)
    
        tempT.append(tweet['created_at'])
        tempT.append(tweet['id_str'])
        tempT.append(tweet['id'])
        tempT.append(tweet['text'])
        tempT.append(tweet['source'])
        tempT.append(tweet['in_reply_to_user_id'])
        tempT.append(tweet['in_reply_to_screen_name'])
        tempT.append(tweet['in_reply_to_status_id'])
        tempT.append(tweet['retweet_count'])
        tempT.append(tweet['contributors'])
        cursor.execute("INSERT INTO TWEET VALUES(?,?,?,?,?,?,?,?,?,?);",tempT)
            
        tempU.append(tweet['id_str'])
        tempU.append(tweet['user']['name'])
        tempU.append(tweet['user']['screen_name'])
        tempU.append(tweet['user']['description'])
        tempU.append(tweet['user']['friends_count'])
        cursor.execute("INSERT INTO USER VALUES(?,?,?,?,?);",tempU)
            
        if tweet['coordinates'] == None:
            continue
        else:
            tempG.append(tweet['id_str'])
            tempG.append(tweet['geo']['type'])
            tempG.append(tweet['geo']['coordinates'][0])
            tempG.append(tweet['geo']['coordinates'][1])   
        cursor.execute("INSERT INTO GEO VALUES(?,?,?,?);",tempG)
        print(i)
    except:
        pass



end = time.time()
timeInsert1 = end - start
# 3.214629888534546 seconds
print(str(timeInsert1) + ' seconds')


#200000 tweets
conn = sqlite3.connect('ExamQ1d2.db')
cursor = conn.cursor()

dropTweet = """DROP TABLE TWEET"""
cursor.execute(dropTweet)

dropUser = """DROP TABLE USER"""
cursor.execute(dropUser)

dropGeo = """DROP TABLE GEO"""
cursor.execute(dropGeo)


Tweettable = """
CREATE TABLE TWEET(
    created_at               VARCHAR(100),
    id_str                   VARCHAR(30)  primary key UNIQUE,
    user_id                  VARCHAR(30),
    text                     VARCHAR(1000),
    source                   VARCHAR(100),
    in_reply_to_user_id      VARCHAR(20),
    in_reply_to_screen_name  VARCHAR(20),
    in_reply_to_status_id    VARCHAR(50),
    retweet_count            NUMBER,
    contributors             VARCHAR(50)
    
);
"""
cursor.execute(Tweettable)

Usertable = """
CREATE TABLE USER(
    id              VARCHAR(30) primary key UNIQUE,
    name            VARCHAR(50),
    screen_name     VARCHAR(20),
    description     VARCHAR(50),
    friends_count   VARCHAR(50),
    
    FOREIGN KEY(id) 
        REFERENCEs TWEET(id_str)
);
"""
cursor.execute(Usertable)

Geotable = """
CREATE TABLE GEO(
    ID          VARCHAR(30) primary key UNIQUE,
    type        VARCHAR(30),
    longitude   VARCHAR(10),
    latitude    VARCHAR(100),
    
    CONSTRAINT gep_fk
        FOREIGN KEY (ID)
            REFERENCES TWEET (id_str)

);
"""
cursor.execute(Geotable)

file1b = open("dataset2.txt", 'r')

start = time.time()

for i in range(200001):
    try:
        tempT, tempU, tempG = [],[],[]
        allTweets = file1b.readline()
        tweet = json.loads(allTweets)
    
        tempT.append(tweet['created_at'])
        tempT.append(tweet['id_str'])
        tempT.append(tweet['id'])
        tempT.append(tweet['text'])
        tempT.append(tweet['source'])
        tempT.append(tweet['in_reply_to_user_id'])
        tempT.append(tweet['in_reply_to_screen_name'])
        tempT.append(tweet['in_reply_to_status_id'])
        tempT.append(tweet['retweet_count'])
        tempT.append(tweet['contributors'])
        cursor.execute("INSERT INTO TWEET VALUES(?,?,?,?,?,?,?,?,?,?);",tempT)
            
        tempU.append(tweet['id_str'])
        tempU.append(tweet['user']['name'])
        tempU.append(tweet['user']['screen_name'])
        tempU.append(tweet['user']['description'])
        tempU.append(tweet['user']['friends_count'])
        cursor.execute("INSERT INTO USER VALUES(?,?,?,?,?);",tempU)
            
        if tweet['coordinates'] == None:
            continue
        else:
            tempG.append(tweet['id_str'])
            tempG.append(tweet['geo']['type'])
            tempG.append(tweet['geo']['coordinates'][0])
            tempG.append(tweet['geo']['coordinates'][1])   
        cursor.execute("INSERT INTO GEO VALUES(?,?,?,?);",tempG)
        print(i)
    except:
        pass



end = time.time()
timeInsert2 = end - start
# 13.039237022399902 seconds
print(timeInsert2)

#600000 tweets
conn = sqlite3.connect('ExamQ1d3.db')
cursor = conn.cursor()

dropTweet = """DROP TABLE TWEET"""
cursor.execute(dropTweet)

dropUser = """DROP TABLE USER"""
cursor.execute(dropUser)

dropGeo = """DROP TABLE GEO"""
cursor.execute(dropGeo)


Tweettable = """
CREATE TABLE TWEET(
    created_at               VARCHAR(100),
    id_str                   VARCHAR(30)  primary key UNIQUE,
    user_id                  VARCHAR(30),
    text                     VARCHAR(1000),
    source                   VARCHAR(100),
    in_reply_to_user_id      VARCHAR(20),
    in_reply_to_screen_name  VARCHAR(20),
    in_reply_to_status_id    VARCHAR(50),
    retweet_count            NUMBER,
    contributors             VARCHAR(50)
    
);
"""
cursor.execute(Tweettable)

Usertable = """
CREATE TABLE USER(
    id              VARCHAR(30) primary key UNIQUE,
    name            VARCHAR(50),
    screen_name     VARCHAR(20),
    description     VARCHAR(50),
    friends_count   VARCHAR(50),
    
    FOREIGN KEY(id) 
        REFERENCEs TWEET(id_str)
);
"""
cursor.execute(Usertable)

Geotable = """
CREATE TABLE GEO(
    ID          VARCHAR(30) primary key UNIQUE,
    type        VARCHAR(30),
    longitude   VARCHAR(10),
    latitude    VARCHAR(100),
    
    CONSTRAINT gep_fk
        FOREIGN KEY (ID)
            REFERENCES TWEET (id_str)

);
"""
cursor.execute(Geotable)

file1b = open("dataset3.txt", 'r')

start = time.time()

for i in range(600001):
    try:
        tempT, tempU, tempG = [],[],[]
        allTweets = file1b.readline()
        tweet = json.loads(allTweets)
    
        tempT.append(tweet['created_at'])
        tempT.append(tweet['id_str'])
        tempT.append(tweet['id'])
        tempT.append(tweet['text'])
        tempT.append(tweet['source'])
        tempT.append(tweet['in_reply_to_user_id'])
        tempT.append(tweet['in_reply_to_screen_name'])
        tempT.append(tweet['in_reply_to_status_id'])
        tempT.append(tweet['retweet_count'])
        tempT.append(tweet['contributors'])
        cursor.execute("INSERT INTO TWEET VALUES(?,?,?,?,?,?,?,?,?,?);",tempT)
            
        tempU.append(tweet['id_str'])
        tempU.append(tweet['user']['name'])
        tempU.append(tweet['user']['screen_name'])
        tempU.append(tweet['user']['description'])
        tempU.append(tweet['user']['friends_count'])
        cursor.execute("INSERT INTO USER VALUES(?,?,?,?,?);",tempU)
            
        
        if tweet['coordinates'] == None:
            continue
        else:
            tempG.append(tweet['id_str'])
            tempG.append(tweet['geo']['type'])
            tempG.append(tweet['geo']['coordinates'][0])
            tempG.append(tweet['geo']['coordinates'][1])   
            cursor.execute("INSERT INTO GEO VALUES(?,?,?,?);",tempG)
    except:
        pass


end = time.time()
timeInsert3 = end - start
# 39.804277181625366 seconds
print(str(timeInsert3) + ' seconds')


#part 1-e
#50000 tweets
conn = sqlite3.connect('ExamQ1e1.db')
cursor = conn.cursor()

dropTweet = """DROP TABLE TWEET"""
cursor.execute(dropTweet)

dropUser = """DROP TABLE USER"""
cursor.execute(dropUser)

dropGeo = """DROP TABLE GEO"""
cursor.execute(dropGeo)


Tweettable = """
CREATE TABLE TWEET(
    created_at               VARCHAR(100),
    id_str                   VARCHAR(30)  primary key UNIQUE,
    user_id                  VARCHAR(30),
    text                     VARCHAR(1000),
    source                   VARCHAR(100),
    in_reply_to_user_id      VARCHAR(20),
    in_reply_to_screen_name  VARCHAR(20),
    in_reply_to_status_id    VARCHAR(50),
    retweet_count            NUMBER,
    contributors             VARCHAR(50)
    
);
"""
cursor.execute(Tweettable)

Usertable = """
CREATE TABLE USER(
    id              VARCHAR(30) primary key UNIQUE,
    name            VARCHAR(50),
    screen_name     VARCHAR(20),
    description     VARCHAR(50),
    friends_count   VARCHAR(50),
    
    FOREIGN KEY(id) 
        REFERENCEs TWEET(id_str)
);
"""
cursor.execute(Usertable)

Geotable = """
CREATE TABLE GEO(
    ID          VARCHAR(30) primary key UNIQUE,
    type        VARCHAR(30),
    longitude   VARCHAR(10),
    latitude    VARCHAR(100),
    
    CONSTRAINT gep_fk
        FOREIGN KEY (ID)
            REFERENCES TWEET (id_str)

);
"""
cursor.execute(Geotable)

file1b = open("dataset1.txt", 'r')

start = time.time()

for i in range(50):
    try: 
        a, b, c = [],[],[]
        for j in range(1000):
            tempT, tempU, tempG = [],[],[]
            allTweets = file1b.readline()
            tweet = json.loads(allTweets)
            
            tempT.append(tweet['created_at'])
            tempT.append(tweet['id_str'])
            tempT.append(tweet['id'])
            tempT.append(tweet['text'])
            tempT.append(tweet['source'])
            tempT.append(tweet['in_reply_to_user_id'])
            tempT.append(tweet['in_reply_to_screen_name'])
            tempT.append(tweet['in_reply_to_status_id'])
            tempT.append(tweet['retweet_count'])
            tempT.append(tweet['contributors'])
            a.append(tempT)
                
            tempU.append(tweet['id_str'])
            tempU.append(tweet['user']['name'])
            tempU.append(tweet['user']['screen_name'])
            tempU.append(tweet['user']['description'])
            tempU.append(tweet['user']['friends_count'])
            b.append(tempU)
            
            if tweet['coordinates'] == None:
                continue
            else:
                tempG.append(tweet['id_str'])
                tempG.append(tweet['geo']['type'])
                tempG.append(tweet['geo']['coordinates'][0])
                tempG.append(tweet['geo']['coordinates'][1])  
            c.append(tempG)
                
    
        cursor.executemany("INSERT INTO TWEET VALUES(?,?,?,?,?,?,?,?,?,?);",a)
        cursor.executemany("INSERT INTO USER VALUES(?,?,?,?,?);",b)
        cursor.executemany("INSERT INTO GEO VALUES(?,?,?,?);",c)
    except:
        pass

end = time.time()
timeInsert1 = end - start
# 9.228225946426392 seconds
print(str(timeInsert1) + ' seconds')


#200000 tweets
conn = sqlite3.connect('ExamQ1e2.db')
cursor = conn.cursor()

dropTweet = """DROP TABLE TWEET"""
cursor.execute(dropTweet)

dropUser = """DROP TABLE USER"""
cursor.execute(dropUser)

dropGeo = """DROP TABLE GEO"""
cursor.execute(dropGeo)


Tweettable = """
CREATE TABLE TWEET(
    created_at               VARCHAR(100),
    id_str                   VARCHAR(30)  primary key UNIQUE,
    user_id                  VARCHAR(30),
    text                     VARCHAR(1000),
    source                   VARCHAR(100),
    in_reply_to_user_id      VARCHAR(20),
    in_reply_to_screen_name  VARCHAR(20),
    in_reply_to_status_id    VARCHAR(50),
    retweet_count            NUMBER,
    contributors             VARCHAR(50)
    
);
"""
cursor.execute(Tweettable)

Usertable = """
CREATE TABLE USER(
    id              VARCHAR(30) primary key UNIQUE,
    name            VARCHAR(50),
    screen_name     VARCHAR(20),
    description     VARCHAR(50),
    friends_count   VARCHAR(50),
    
    FOREIGN KEY(id) 
        REFERENCEs TWEET(id_str)
);
"""
cursor.execute(Usertable)

Geotable = """
CREATE TABLE GEO(
    ID          VARCHAR(30) primary key UNIQUE,
    type        VARCHAR(30),
    longitude   VARCHAR(10),
    latitude    VARCHAR(100),
    
    CONSTRAINT gep_fk
        FOREIGN KEY (ID)
            REFERENCES TWEET (id_str)

);
"""
cursor.execute(Geotable)

file1b = open("dataset2.txt", 'r')

start = time.time()

for i in range(200):
    try: 
        a, b, c = [],[],[]
        for j in range(1000):
            tempT, tempU, tempG = [],[],[]
            allTweets = file1b.readline()
            tweet = json.loads(allTweets)
            m = i*1000 + j
            print(str(m))
            
            tempT.append(tweet['created_at'])
            tempT.append(tweet['id_str'])
            tempT.append(tweet['id'])
            tempT.append(tweet['text'])
            tempT.append(tweet['source'])
            tempT.append(tweet['in_reply_to_user_id'])
            tempT.append(tweet['in_reply_to_screen_name'])
            tempT.append(tweet['in_reply_to_status_id'])
            tempT.append(tweet['retweet_count'])
            tempT.append(tweet['contributors'])
            a.append(tempT)
                
            tempU.append(tweet['id_str'])
            tempU.append(tweet['user']['name'])
            tempU.append(tweet['user']['screen_name'])
            tempU.append(tweet['user']['description'])
            tempU.append(tweet['user']['friends_count'])
            b.append(tempU)
            
            if tweet['coordinates'] == None:
                continue
            else:
                tempG.append(tweet['id_str'])
                tempG.append(tweet['geo']['type'])
                tempG.append(tweet['geo']['coordinates'][0])
                tempG.append(tweet['geo']['coordinates'][1])  
            c.append(tempG)
                
    
        cursor.executemany("INSERT INTO TWEET VALUES(?,?,?,?,?,?,?,?,?,?);",a)
        cursor.executemany("INSERT INTO USER VALUES(?,?,?,?,?);",b)
        cursor.executemany("INSERT INTO GEO VALUES(?,?,?,?);",c)
    except:
        pass

end = time.time()
timeInsert1 = end - start
# 36.993664026260376 seconds
print(str(timeInsert1) + ' seconds')


#600000 tweets
conn = sqlite3.connect('ExamQ1e3.db')
cursor = conn.cursor()

dropTweet = """DROP TABLE TWEET"""
cursor.execute(dropTweet)

dropUser = """DROP TABLE USER"""
cursor.execute(dropUser)

dropGeo = """DROP TABLE GEO"""
cursor.execute(dropGeo)


Tweettable = """
CREATE TABLE TWEET(
    created_at               VARCHAR(100),
    id_str                   VARCHAR(30)  primary key UNIQUE,
    user_id                  VARCHAR(30),
    text                     VARCHAR(1000),
    source                   VARCHAR(100),
    in_reply_to_user_id      VARCHAR(20),
    in_reply_to_screen_name  VARCHAR(20),
    in_reply_to_status_id    VARCHAR(50),
    retweet_count            NUMBER,
    contributors             VARCHAR(50)
    
);
"""
cursor.execute(Tweettable)

Usertable = """
CREATE TABLE USER(
    id              VARCHAR(30) primary key UNIQUE,
    name            VARCHAR(50),
    screen_name     VARCHAR(20),
    description     VARCHAR(50),
    friends_count   VARCHAR(50),
    
    FOREIGN KEY(id) 
        REFERENCEs TWEET(id_str)
);
"""
cursor.execute(Usertable)

Geotable = """
CREATE TABLE GEO(
    ID          VARCHAR(30) primary key UNIQUE,
    type        VARCHAR(30),
    longitude   VARCHAR(10),
    latitude    VARCHAR(100),
    
    CONSTRAINT gep_fk
        FOREIGN KEY (ID)
            REFERENCES TWEET (id_str)

);
"""
cursor.execute(Geotable)

file1b = open("dataset3.txt", 'r')

start = time.time()

for i in range(600):
    try: 
        a, b, c = [],[],[]
        for j in range(1000):
            tempT, tempU, tempG = [],[],[]
            allTweets = file1b.readline()
            tweet = json.loads(allTweets)
            m = i*1000 + j
            print(str(m))
            
            tempT.append(tweet['created_at'])
            tempT.append(tweet['id_str'])
            tempT.append(tweet['id'])
            tempT.append(tweet['text'])
            tempT.append(tweet['source'])
            tempT.append(tweet['in_reply_to_user_id'])
            tempT.append(tweet['in_reply_to_screen_name'])
            tempT.append(tweet['in_reply_to_status_id'])
            tempT.append(tweet['retweet_count'])
            tempT.append(tweet['contributors'])
            a.append(tempT)
                
            tempU.append(tweet['id_str'])
            tempU.append(tweet['user']['name'])
            tempU.append(tweet['user']['screen_name'])
            tempU.append(tweet['user']['description'])
            tempU.append(tweet['user']['friends_count'])
            b.append(tempU)
            
            if tweet['coordinates'] == None:
                continue
            else:
                tempG.append(tweet['id_str'])
                tempG.append(tweet['geo']['type'])
                tempG.append(tweet['geo']['coordinates'][0])
                tempG.append(tweet['geo']['coordinates'][1])  
            c.append(tempG)
                
    
        cursor.executemany("INSERT INTO TWEET VALUES(?,?,?,?,?,?,?,?,?,?);",a)
        cursor.executemany("INSERT INTO USER VALUES(?,?,?,?,?);",b)
        cursor.executemany("INSERT INTO GEO VALUES(?,?,?,?);",c)
    except:
        pass

end = time.time()
timeInsert1 = end - start
# 111.64353394508362 seconds
print(str(timeInsert1) + ' seconds')
 
#part 1-f
ax = [50000, 200000, 600000]
ay = [289.9412348270416,800.8470940589905, 2002.7778232097626]

cx = [50000, 200000, 600000]
cy = [343.7849521636963, 1558.7821018695831, 3329.1564939022064]

dx = [50000, 200000, 600000]
dy = [3.214629888534546, 13.039237022399902, 39.804277181625366]

ex = [50000, 200000, 600000]
ey = [9.228225946426392, 36.993664026260376, 111.64353394508362]

import matplotlib.pyplot as plt

plt.plot(ax, ay, label = "1-a", marker = 'o')
plt.plot(cx, cy, label = "1-c", marker = 'o')
plt.plot(dx, dy, label = "1-d", marker = 'o')
plt.plot(ex, ey, label = "1-e", marker = 'o')

plt.legend(loc='upper left')
plt.xlabel("# of tweets")
plt.ylabel("runtimes")
plt.title("1-f")


#part 2-a
conn = sqlite3.connect('ExamQ1d3.db')
cursor = conn.cursor()

start = time.time()
query = """
SELECT user_id, (SELECT AVG(longitude)
                FROM GEO
                WHERE TWEET.id_str = GEO.ID
                GROUP BY GEO.ID), (SELECT  AVG(latitude)
                                   FROM GEO
                                   WHERE TWEET.id_str = GEO.ID
                                   GROUP BY GEO.ID)
FROM TWEET;
"""
result = cursor.execute(query)
for row in result.fetchall():
    print(row) 

end = time.time()
runtime1 = end - start
#69.02033996582031
print("running time of 1 times is " + str(runtime1) + " seconds")

#part 2-b
import time

#run 10 times
start = time.time()
for i in range(10):
    query = """
    SELECT user_id, (SELECT AVG(longitude)
                    FROM GEO
                    WHERE TWEET.id_str = GEO.ID
                    GROUP BY GEO.ID), (SELECT  AVG(latitude)
                                       FROM GEO
                                       WHERE TWEET.id_str = GEO.ID
                                       GROUP BY GEO.ID)
    FROM TWEET;
    """
    result = cursor.execute(query)
    for row in result.fetchall():
        print(row) 
    
end = time.time()
runtime10 = end - start
#692.867840051651 seconds
print("running time of 10 times is " + str(runtime10) + " seconds")

#run 100 times
start = time.time()
for i in range(10):
    query = """
    SELECT user_id, (SELECT AVG(longitude)
                    FROM GEO
                    WHERE TWEET.id_str = GEO.ID
                    GROUP BY GEO.ID), (SELECT  AVG(latitude)
                                       FROM GEO
                                       WHERE TWEET.id_str = GEO.ID
                                       GROUP BY GEO.ID)
    FROM TWEET;
    """
    result = cursor.execute(query)
    for row in result.fetchall():
        print(row) 
    
end = time.time()
runtime10 = end - start
print("running time of 10 times is " + str(runtime10) + " seconds")


#part 2-c
start = time.time()

temp = {}
file1b = open("dataset3.txt", 'r')
for i in range(600000):
    try:
        allTweets = file1b.readline()
        tweet = json.loads(allTweets)
            
            #print(i)
        if tweet['coordinates'] == None:
            continue
        else:
            if tweet['id'] not in temp:
                longitude = tweet['geo']['coordinates'][0]
                latitude = tweet['geo']['coordinates'][1]
                temp[tweet['id']] = [1, longitude, latitude]
            else:
                count = temp[tweet['id']][0]
                longi = temp[tweet['id']][1]
                lati= temp[tweet['id']][2]
                longitude = tweet['geo']['coordinates'][0]
                latitude = tweet['geo']['coordinates'][1]
                temp[tweet['id']] = [count+1, longi+longitude, lati+latitude]
    except:
        pass

#14472
for key, value in temp.items():
    if value[0] == 1:
        print(key, value[1], value[2])
    else:
        longi = value[1] / value[0]
        lati = value[2] / value[0]
        print(key, longi, lati)

end = time.time()
timeInsert3 = end - start
#26.564638137817383
print("running time of 1 times is " + str(timeInsert3) + " seconds")


#part 2-d
start = time.time()

for i in range(10):
    temp = {}
    file1b = open("dataset3.txt", 'r')
    for i in range(600000):
        try:
            allTweets = file1b.readline()
            tweet = json.loads(allTweets)
            
            #print(i)
            if tweet['coordinates'] == None:
                continue
            else:
                if tweet['id'] not in temp:
                    longitude = tweet['geo']['coordinates'][0]
                    latitude = tweet['geo']['coordinates'][1]
                    temp[tweet['id']] = [1, longitude, latitude]
                else:
                    count = temp[tweet['id']][0]
                    longi = temp[tweet['id']][1]
                    lati= temp[tweet['id']][2]
                    longitude = tweet['geo']['coordinates'][0]
                    latitude = tweet['geo']['coordinates'][1]
                    temp[tweet['id']] = [count+1, longi+longitude, lati+latitude]
        except:
            pass

#14472
    for key, value in temp.items():
        if value[0] == 1:
            print(key, value[1], value[2])
        else:
            longi = value[1] / value[0]
            lati = value[2] / value[0]
            print(key, longi, lati)   

end = time.time()
timeInsert3 = end - start
#273.7922170162201
print("running time of 10 times is " + str(timeInsert3) + " seconds")

#part 2-e
file1b = open("dataset3.txt", 'r')

start = time.time()

temp = {}
for i in range(600000):

    tweet = file1b.readline()
    if '"geo":null,"coordinates":null' in tweet:
        continue
    else:
        indexidStart = tweet.find('id') + 4
        if indexidStart-4 != -1:
            indexidEnd = tweet.find('id', indexidStart) - 2
            id = str(tweet[indexidStart:indexidEnd])
            if id not in temp:
                indexStart = tweet.find('coordinates') + 14
                indexEnd = tweet.find('coordinates', indexStart) - 4
                indexComma = tweet.find(',', indexStart, indexEnd)
                longi = float(tweet[indexStart:indexComma])
                lati = float(tweet[indexComma + 1:indexEnd])
                temp[id] = [1, longi, lati]
            else:
                count = temp[id][0]
                longitude = temp[id][1]
                latitude = temp[id][2]
                indexStart = tweet.find('coordinates') + 14
                indexEnd = tweet.find('coordinates', indexStart) - 4
                indexComma = tweet.find(',', indexStart, indexEnd)
                longi = float(tweet[indexStart:indexComma])
                lati = float(tweet[indexComma + 1:indexEnd])
                temp[id] = [count+1, longi+longitude, lati+latitude]
        else:
            continue

    
#14472
for key, value in temp.items():
    if value[0] == 1:
        print(key, value[1], value[2])
    else:
        longi = value[1] / value[0]
        lati = value[2] / value[0]
        print(key, longi, lati)


end = time.time()
timeInsert3 = end - start
#9.229596853256226
print("running time of 1 times is " + str(timeInsert3) + " seconds")

#part 2-f
start = time.time()

for i in range(100):
    file1b = open("dataset3.txt", 'r')
    temp = {}
    for i in range(600000):
    
        tweet = file1b.readline()
        if '"geo":null,"coordinates":null' in tweet:
            continue
        else:
            indexidStart = tweet.find('id') + 4
            if indexidStart-4 != -1:
                indexidEnd = tweet.find('id', indexidStart) - 2
                id = str(tweet[indexidStart:indexidEnd])
                if id not in temp:
                    indexStart = tweet.find('coordinates') + 14
                    indexEnd = tweet.find('coordinates', indexStart) - 4
                    indexComma = tweet.find(',', indexStart, indexEnd)
                    longi = float(tweet[indexStart:indexComma])
                    lati = float(tweet[indexComma + 1:indexEnd])
                    temp[id] = [1, longi, lati]
                else:
                    count = temp[id][0]
                    longitude = temp[id][1]
                    latitude = temp[id][2]
                    indexStart = tweet.find('coordinates') + 14
                    indexEnd = tweet.find('coordinates', indexStart) - 4
                    indexComma = tweet.find(',', indexStart, indexEnd)
                    longi = float(tweet[indexStart:indexComma])
                    lati = float(tweet[indexComma + 1:indexEnd])
                    temp[id] = [count+1, longi+longitude, lati+latitude]
            else:
                continue
    
        
    #14472
    for key, value in temp.items():
        if value[0] == 1:
            print(key, value[1], value[2])
        else:
            longi = value[1] / value[0]
            lati = value[2] / value[0]
            print(key, longi, lati)
            
end = time.time()
timeInsert3 = end - start
#92.98136281967163
print("running time of 100 times is " + str(timeInsert3) + " seconds")


#part 3-a
conn = sqlite3.connect('ExamQ3a.db')
cursor = conn.cursor()

Tweettable = """
CREATE TABLE TWEET(
    created_at               VARCHAR(100),
    id_str                   VARCHAR(30)  primary key UNIQUE,
    user_id                  VARCHAR(30),
    text                     VARCHAR(1000),
    source                   VARCHAR(100),
    in_reply_to_user_id      VARCHAR(20),
    in_reply_to_screen_name  VARCHAR(20),
    in_reply_to_status_id    VARCHAR(50),
    retweet_count            NUMBER,
    contributors             VARCHAR(50)
    
);
"""
cursor.execute(Tweettable)

Usertable = """
CREATE TABLE USER(
    id              VARCHAR(30) primary key UNIQUE,
    name            VARCHAR(50),
    screen_name     VARCHAR(20),
    description     VARCHAR(50),
    friends_count   VARCHAR(50),
    
    FOREIGN KEY(id) 
        REFERENCEs TWEET(id_str)
);
"""
cursor.execute(Usertable)

Geotable = """
CREATE TABLE GEO(
    ID          VARCHAR(30) primary key UNIQUE,
    type        VARCHAR(30),
    longitude   VARCHAR(10),
    latitude    VARCHAR(100),
    
    CONSTRAINT gep_fk
        FOREIGN KEY (ID)
            REFERENCES TWEET (id_str)

);
"""
cursor.execute(Geotable)

query2 = """
CREATE TABLE TWEETMV AS
SELECT created_at, id_str, user_id, text, source, 
            in_reply_to_user_id, in_reply_to_screen_name, 
            in_reply_to_status_id, retweet_count, contributors,
            name, screen_name, description, friends_count,
            type, longitude, latitude          
FROM (SELECT created_at, id_str, user_id, text, source, 
            in_reply_to_user_id, in_reply_to_screen_name, 
            in_reply_to_status_id, retweet_count, contributors,
            type, longitude, latitude
            FROM TWEET LEFT JOIN GEO ON TWEET.id_str = GEO.ID), USER
WHERE USER.ID = id_str;
"""
result = cursor.execute(query2)

query3 = """
SELECT * 
FROM TWEETMV;
"""

result = cursor.execute(query3)
for row in result.fetchall():
    print(row)  
    break


#question 3-b
import pandas as pd
df = pd.read_csv (r'3-c.csv')
df.to_json(r'3-b.json')


#question 3-c
import csv
file3c = csv.writer(open("3-c.csv", 'w'))
for row in result.fetchall():
    file3c.writerow(row) 





#test
query = """
DROP TABLE TWEETMV;
"""
cursor.execute(query)



deleteTweetData = """
DELETE FROM TWEET;"""
cursor.execute(deleteTweetData)

deleteUserData = """
DELETE FROM USER;"""
cursor.execute(deleteUserData)

deleteGeoData = """
DELETE FROM GEO;"""
cursor.execute(deleteGeoData)

checkTweetData = """
SELECT * FROM TWEET;"""
result = cursor.execute(checkTweetData)
for row in result.fetchall():
    print(row)

checkUserData = """
SELECT * FROM USER;"""
result = cursor.execute(checkUserData)
for row in result.fetchall():
    print(row)  
    
checkGeoData = """
SELECT * FROM GEO;"""
result = cursor.execute(checkGeoData)
for row in result.fetchall():
    print(row)  