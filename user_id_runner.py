# -*- coding: utf-8 -*-

from kafka import KafkaProducer

import sys
import requests
import json
import time
import random
import traceback
import re
import codecs
#import pymysql.cursors

def crawlDetailPage(url,page,producer):
    global ID_get
    global num
    #读取微博网页的JSON信息
    req = requests.get(url)
    jsondata = req.text
    data = json.loads(jsondata)
    #获取每一条页的数据
    try:
        content = data['data']['cards']
        #循环输出每一页的关注者各项信息
        for i in content:
            followingId = i['user']['id']
            ID_get.append(followingId)
            num=num+1
            producer.send('WEIBO_USER_ID',followingId)
    except Exception as e:
        print('Error: ', e)

def get_user_info(user_id):   #containerid和usid不一致，查看用户的关注列表需要他的containerid，usid用于获取用户主页信息
    url = 'http://m.weibo.cn/api/container/getIndex?type=uid&value={user_id}'.format(user_id=user_id)
    resp = requests.get(url)
    jsondata = resp.json()
    jsondata = jsondata['data']
    fans_id=jsondata.get('follow_scheme')
    items = re.findall(r"&lfid=(\w+)*", fans_id, re.M)
    for i in items:
        i=i.decode(encoding='UTF-8',errors='strict')
    return i

user_oid=1005051669879400     #这个是我自己的微博containerid,在个人主页点我的微博得到的链接里的数字，也可以换成你自己的
#containerid，https://weibo.com/p/1005051654024040/home?from=page_100505_profile&wvr=6&mod=data#place
#userid,https://weibo.com/1654024040/profile?topnav=1&wvr=6

producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_servers=['192.168.2.128:9092']
)

for cir in range(1,20):
    print("正在获取第{}位用户的粉丝信息:".format(cir))
    num = 0
    ID_get = []
    for i in range(1, 3):
        print("正在获取第{}页的粉丝列表:".format(i))
        # 微博用户关注列表JSON链接
        url = "https://m.weibo.cn/api/container/getSecond?containerid={user_oid}_-_FANS&page={page}".format(user_oid=user_oid, page=i)  # page=" +   #FOLOWERS关注，FANS粉丝
        crawlDetailPage(url, i,producer)
        # 设置休眠时间
        t1 = random.randint(1, 5)
        t2 = random.randint(2, 6)
        print("休眠时间为:{}s".format(t1+t2))
        time.sleep(t1)
        try:
            for id in ID_get:
                user_id = id
                user_oid = get_user_info(str(user_id))
            time.sleep(t2)
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

