# -*- coding: utf-8 -*-
import leancloud
from leancloud import Object
from leancloud import LeanCloudError
from leancloud import Query
from leancloud import User
import time
import datetime
import operator
import numpy as np

APP_NAME='senz.datasource.timeline'
APP_ID='pin72fr1iaxb7sus6newp250a4pl2n5i36032ubrck4bej81'
APP_KEY='qs4o5iiywp86eznvok4tmhul360jczk7y67qj0ywbcq35iia'

APP_NAME_DEST='test@senz.analyzer.dashboard'
APP_ID_DEST='mqip2evxqhxu8c5essmfavnk44fdm85z12gtkkzzvzwfmzqn'
APP_KEY_DEST='awfcehevaljgo4hqn6c8gp6nhagaqtgzlae1gp5x61l91xtv'

notBinaryData={'age':{"16down":0,"16to35":0.9,"35to55":0.1,"55up":0},'sport':{"jogging":0,"fitness":0,"basketball":0.8,"football":0,"badminton":0,"bicycling":0,"tabel_tennis":0},'field':{"service":0,"commerce":0,"law":0,"humanities":0,"architecture":0,"medical":0,"manufacture":0,"human_resource":0,"financial":0,"natural":0.6,"agriculture":0,"infotech":0,"athlete":0},'consumption':{"5000down":0.1,"5000to10000":0.5,"10000to20000":0.6,"20000up":0.4},'occupation':{"official":0,"teacher":0,"freelancer":0,"supervisor":0,"salesman":0,"engineer":0.8,"others":0,"soldier":0,"student":0}}
binaryData=[u'ACG',u'indoorsman',u'game_show',u'has_car',u'game_news',u'entertainment_news',u'health',u'online_shopping',u'variety_show',u'business_news',u'tvseries_show',u'current_news',u'sports_news',u'tech_news',u'offline_shopping',u'pregnant',u'gender',u'study',u'married',u'sports_show',u'gamer',u'social',u'has_pet']

query_limit=700
leancloud.init(APP_ID_DEST,APP_KEY_DEST)
# table_name='UserBehavior'
table_name_dest = 'UserContext'
field_name='objectId'
currentTime = datetime.datetime.now()

query = Query(User)
query.less_than('updatedAt',currentTime)
query.exists(field_name)
total_count=query.count()
print 'TotalCount  %s' %str(total_count)

query_times=(total_count+query_limit-1)/query_limit

userList=[]

for index in range(query_times):
    print 'querying index: %s' %str(index)
    query = Query(User)
    query.less_than('updatedAt',currentTime)
    query.exists(field_name)
    query.descending('updatedAt')
    query.limit(query_limit)
    query.skip(index*query_limit)
    userList.extend(query.find())


table_name_dest='StaticInfo'
DBTable = Object.extend(table_name_dest)
probList =[]
recordCount = 2000
userLen= len(userList)
print 'userLength is : %s' %str(userLen)
for index ,key in enumerate(range(recordCount)):
    # print 'saving index: %s' %str(index)
    dbTable = DBTable()
    # dbTable.set('userInfoLogSrc','fakeData')
    # dbTable.set('user','fakeData')
    dbTable.set('description','fake data')
    dbTable.set('timestamp',int(time.time()*1000))
    dbTable.set('user',userList[np.random.randint(userLen)])
    for innerIndex1 ,key1 in enumerate(notBinaryData):
        for innerIndex2,key2 in enumerate(notBinaryData[key1]):
            dbTable.set(key1+'__'+key2,2*np.random.random_sample()-1) #用两个下划线是为了防止和table_tennis这样的词语冲突
    for innerIndex1,key1 in enumerate(binaryData):
        dbTable.set(key1,2*np.random.random_sample()-1)
    dbTable.save()
print 'finished all'

# print len(staticInfoDict)[{"behavior":{"prob":-7.314387215370084,"senzList":[{"location":"university","motion":"walking","senzId":[],"sound":"living_room","tenMinScale":133,"timestamp":1436624927842},{"location":"university","motion":"walking","senzId":["55a12f79e4b005382bf2a12d"],"sound":"living_room","tenMinScale":134,"timestamp":1436624927842},{"location":"university","motion":"walking","senzId":["55a12f79e4b06d11d33159df"],"sound":"living_room","tenMinScale":135,"timestamp":1436625467992},{"location":"university","motion":"walking","senzId":[],"sound":"living_room","tenMinScale":136,"timestamp":1436625467992}]},"prediction":{"go_work":-35.07309405386655,"go_to_class":-10849.321329810893,"go_for_concert":-204.71875876974352,"travel_in_scenic":-30.88132952652512,"go_home":-35.112568006678174,"dining_in_restaurant":-20.891283831985966,"movie_in_cinema":-158359.66122732504,"emergency":-40.541956720289896,"go_for_outing":-44.31179790987923,"work_in_office":-207.48950698532494,"exercise_outdoor":-40.47403854943419,"go_for_exhibition":-19.467647123870112,"shopping_in_mall":-17.23000169161957,"exercise_indoor":-31.58784616412453},"algoType":"GMMHMM","modelTag":"randomTrain"},{"behavior":{"prob":-24.720872718756503,"senzList":[{"location":"residence","motion":"running","senzId":[],"sound":"kitchen","tenMinScale":133,"timestamp":1436624927842},{"location":"residence","motion":"running","senzId":["55a12f79e4b005382bf2a12d"],"sound":"kitchen","tenMinScale":134,"timestamp":1436624927842},{"location":"residence","motion":"riding","senzId":["55a12f79e4b06d11d33159df"],"sound":"kitchen","tenMinScale":135,"timestamp":1436625467992},{"location":"residence","motion":"riding","senzId":[],"sound":"kitchen","tenMinScale":136,"timestamp":1436625467992}]},"prediction":{"go_work":-42.70248066655474,"go_to_class":-131259.25808537554,"go_for_concert":-204.71875877247044,"travel_in_scenic":-46.24502466148193,"go_home":-45.82647862847197,"dining_in_restaurant":-45663.5298485668,"movie_in_cinema":-247411.79149599452,"emergency":-41.09323103987009,"go_for_outing":-70.25905548899927,"work_in_office":-207.48950699735667,"exercise_outdoor":-92.42987117430195,"go_for_exhibition":-17615.90093845541,"shopping_in_mall":-207.46359622092666,"exercise_indoor":-262.17567735538523},"algoType":"GMMHMM","modelTag":"randomTrain"},{"behavior":{"prob":-35.80785601725923,"senzList":[{"location":"business_building","motion":"riding","senzId":[],"sound":"bedroom","tenMinScale":133,"timestamp":1436624927842},{"location":"business_building","motion":"riding","senzId":["55a12f79e4b005382bf2a12d"],"sound":"bedroom","tenMinScale":134,"timestamp":1436624927842},{"location":"business_building","motion":"driving","senzId":["55a12f79e4b06d11d33159df"],"sound":"flat","tenMinScale":135,"timestamp":1436625467992},{"location":"business_building","motion":"driving","senzId":[],"sound":"flat","tenMinScale":136,"timestamp":1436625467992}]},"prediction":{"go_work":-39.218977290255914,"go_to_class":-59119.90726173253,"go_for_concert":-204.71875877220455,"travel_in_scenic":-40.10696712558219,"go_home":-43.20662595275348,"dining_in_restaurant":-35159.51776626667,"movie_in_cinema":-440315.5367267065,"emergency":-37.083723574916,"go_for_outing":-38.947557511455585,"work_in_office":-207.4895069988921,"exercise_outdoor":-77.68974880814625,"go_for_exhibition":-11461.832597969584,"shopping_in_mall":-207.46359621980324,"exercise_indoor":-69.99080363090235},"algoType":"GMMHMM","modelTag":"randomTrain"}]
# description ='the value of field data is the set of all the labels in table app_dict_not_binary and the current app_dict_not_binary count is %s and the list of app_dict is %s' %(str(total_count),total_label)
# DB_Tool= Object.extend('DBTool')
# db_tool = DB_Tool()
# db_tool.set('tableName',table_name)
# db_tool.set('fieldName',field_name)
# db_tool.set('data',str(set(total_label)))
# db_tool.set('description',description)
# db_tool.save()

#
# not_binary_label_list=[]
# >>> for innerIndex1 ,key1 in enumerate(notBinaryData):
# ...         for innerIndex2,key2 in enumerate(notBinaryData[key1]):
# ...             not_binary_label_list.append(key1+'__'+key2) # 因为要append的是字符串，所以使用了append而不是extend
# for innerIndex1,key1 in enumerate(binaryData):
# ...     binary_label_list.append(key1)

# 37 total
# not_binary_label_list = ['field__manufacture', 'field__financial', 'field__infotech', 'field__law', 'field__agriculture', 'field__human_resource', 'field__commerce', 'field__natural', 'field__service', 'field__humanities', 'field__medical', 'field__architecture', 'field__athlete', 'age__16to35', 'age__35to55', 'age__55up', 'age__16down', 'sport__basketball', 'sport__bicycling', 'sport__tabel_tennis', 'sport__football', 'sport__jogging', 'sport__badminton', 'sport__fitness', 'consumption__10000to20000', 'consumption__20000up', 'consumption__5000to10000', 'consumption__5000down', 'occupation__freelancer', 'occupation__supervisor', 'occupation__student', 'occupation__others', 'occupation__official', 'occupation__salesman', 'occupation__teacher', 'occupation__soldier', 'occupation__engineer']
# # 23 total
# binary_label_list = [ u'ACG', u'indoorsman', u'game_show', u'has_car', u'game_news', u'entertainment_news', u'health', u'online_shopping', u'variety_show', u'business_news', u'tvseries_show', u'current_news', u'sports_news', u'tech_news', u'offline_shopping', u'pregnant', u'gender', u'study', u'married', u'sports_show', u'gamer', u'social', u'has_pet']

binary_label_list = [ u'ACG', u'indoorsman', u'game_show', u'has_car', u'game_news', u'entertainment_news', u'health', u'online_shopping', u'variety_show', u'business_news', u'tvseries_show', u'current_news', u'sports_news', u'tech_news', u'offline_shopping', u'pregnant', u'gender', u'study', u'married', u'sports_show', u'gamer', u'social', u'has_pet']
not_binary_label_list = ['field__manufacture', 'field__financial', 'field__infotech', 'field__law', 'field__agriculture', 'field__human_resource', 'field__commerce', 'field__natural', 'field__service', 'field__humanities', 'field__medical', 'field__architecture', 'field__athlete', 'age__16to35', 'age__35to55', 'age__55up', 'age__16down', 'sport__basketball', 'sport__bicycling', 'sport__tabel_tennis', 'sport__football', 'sport__jogging', 'sport__badminton', 'sport__fitness', 'consumption__10000to20000', 'consumption__20000up', 'consumption__5000to10000', 'consumption__5000down', 'occupation__freelancer', 'occupation__supervisor', 'occupation__student', 'occupation__others', 'occupation__official', 'occupation__salesman', 'occupation__teacher', 'occupation__soldier', 'occupation__engineer']
