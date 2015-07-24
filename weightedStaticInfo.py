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
from logentries import LogentriesHandler
import logging

log = logging.getLogger('logentries')
log.setLevel(logging.INFO)
log.addHandler(LogentriesHandler('d700103a-44fb-4c35-9cb2-cbe206375060'))


APP_NAME_DEST='test@senz.analyzer.dashboard'
APP_ID_DEST='mqip2evxqhxu8c5essmfavnk44fdm85z12gtkkzzvzwfmzqn'
APP_KEY_DEST='awfcehevaljgo4hqn6c8gp6nhagaqtgzlae1gp5x61l91xtv'


all_label_list = ['field__manufacture', 'field__financial', 'field__infotech', 'field__law', 'field__agriculture', 'field__human_resource', 'field__commerce', 'field__natural', 'field__service', 'field__humanities', 'field__medical', 'field__architecture', 'field__athlete', 'age__16to35', 'age__35to55', 'age__55up', 'age__16down', 'sport__basketball', 'sport__bicycling', 'sport__tabel_tennis', 'sport__football', 'sport__jogging', 'sport__badminton', 'sport__fitness', 'consumption__10000to20000', 'consumption__20000up', 'consumption__5000to10000', 'consumption__5000down', 'occupation__freelancer', 'occupation__supervisor', 'occupation__student', 'occupation__others', 'occupation__official', 'occupation__salesman', 'occupation__teacher', 'occupation__soldier', 'occupation__engineer',u'ACG', u'indoorsman', u'game_show', u'has_car', u'game_news', u'entertainment_news', u'health', u'online_shopping', u'variety_show', u'business_news', u'tvseries_show', u'current_news', u'sports_news', u'tech_news', u'offline_shopping', u'pregnant', u'gender', u'study', u'married', u'sports_show', u'gamer', u'social', u'has_pet']
query_limit=700
leancloud.init(APP_ID_DEST,APP_KEY_DEST)
# table_name='UserBehavior'
field_name='objectId'
currentTime = datetime.datetime.now()

query = Query(User)
query.less_than('createdAt',currentTime)
query.exists(field_name)
total_count=query.count()
print 'TotalCount  %s' %str(total_count)

query_times=(total_count+query_limit-1)/query_limit

userList=[]
#注意这种分页的方法取数据时，如果user数量很大，而且读数据时user还在快速增长的话，是可能取到重复的user的，要怎么Unique呢？
#但是如果采用的是ascending的排序方式的话，貌似倒是不会出现这个情况
for index in range(query_times):
    print 'querying index: %s' %str(index)
    query = Query(User)
    query.less_than('createdAt',currentTime)
    query.exists(field_name)
    query.ascending('createdAt')
    query.limit(query_limit)
    query.skip(index*query_limit)
    userList.extend(query.find())

print 'The length of userList is %s'  %str(len(userList))

table_name = 'StaticInfo'
field_name = 'user'
DBTable = Object.extend(table_name)
userDataDict = {}
for user in userList:
    #这样处理是因为可能一个user的记录超过了一次可以读取的数量（1K条）
    query = Query(DBTable)
    query.equal_to(field_name,user)
    query.less_than('createdAt',currentTime)
    total_count=query.count()
    # print 'TotalCount  %s' %str(total_count)

    query_times=(total_count+query_limit-1)/query_limit
    #如果想在这里按timestamp排序取出每个user的记录是不靠谱的，可能读取时还有插入，而且timestamp介于之间
    for index in range(query_times):
        # print 'querying index: %s' %str(index)
        query = Query(DBTable)
        query.equal_to(field_name,user)
        query.less_than('createdAt',currentTime)
        query.ascending('createdAt')
        query.limit(query_limit)
        query.skip(index*query_limit)
        if userDataDict.get(user):
            userDataDict.get(user).extend(query.find())
        else :
            userDataDict[user]=query.find()
print 'already get the userDataDict'
weightedUserDataDict ={}
table_name_dest='WeightedStaticInfo'
DBTable = Object.extend(table_name_dest)
for user in userDataDict.keys():
    userData = userDataDict[user]
    print 'the length of this userData is %s ' %str(len(userData))
    dbTable = DBTable()
    dbTable.set('user',user)
    dbTable.set('staticInfoCount',len(userData))


    # recordCount = len(userData)
    rawWeight = np.arange(1,len(userData)+1)
    normalizedWeight = rawWeight/float(np.sum(rawWeight)) #将所有的权重归一化
    #接着将所有的label按照labelList的顺序放到一个大的矩阵中
    field_value_matrix = np.empty([len(userData),len(all_label_list)+1])
    print 'the shape of this field_value_matrix is %s' %str(np.shape(field_value_matrix))
    for index1 ,record in enumerate(userData):
        relation = dbTable.relation('static_info')
        relation.add(record)
        for index2,field in enumerate(all_label_list):
            field_value_matrix[index1,index2] = record.get(field)
        field_value_matrix[index1,index2+1] = record.get('timestamp')
        # for index2,field in enumerate(not_binary_label_list):
        #     field_value_matrix[index1,index2] = record.get(field)
        # for index3,field in enumerate(binary_label_list):
        #     field_value_matrix[index1,index2+index3]=record.get(field)
        # field_value_matrix[index1,index2+index3] = record.get('timestamp')
    #接着按照timestamp的顺序将每天记录按升序排序
    sorted_field_value_matrix=np.array(sorted(field_value_matrix,key=lambda record:record[-1]))     # change to a list after sorted
    for index2,field in enumerate(all_label_list):
        log.info('sorted_field_value_matrix:  \n' + str(sorted_field_value_matrix))
        weightedValue = np.dot(sorted_field_value_matrix[:,index2].flatten(),normalizedWeight)
        log.info('normalizedWeight:  \n' + str(normalizedWeight))
        log.info('weightedValue:  \n' + str(weightedValue))
        dbTable.set(field,weightedValue)

    # for index2,field in enumerate(not_binary_label_list):

    #     print sorted_field_value_matrix
    #     weightedValue = np.dot(sorted_field_value_matrix[:,index2].flatten(),normalizedWeight)
    #
    #
    #     dbTable.set(field,weightedValue)
    # for index3,field in enumerate(binary_label_list):
    #     weightedValue = np.dot(sorted_field_value_matrix[:,index2+index3].flatten(),normalizedWeight)
    #     dbTable.set(field,weightedValue)
    dbTable.save()








# probList =[]
# recordCount = 2000
# userLen= len(userList)
# print 'userLength is : %s' %str(userLen)
# for index ,key in enumerate(range(recordCount)):
#     # print 'saving index: %s' %str(index)
#     dbTable = DBTable()
#     # dbTable.set('userInfoLogSrc','fakeData')
#     # dbTable.set('user','fakeData')
#     dbTable.set('description','fake data')
#     dbTable.set('user',userList[np.random.randint(userLen)])
#     for innerIndex1 ,key1 in enumerate(notBinaryData):
#         for innerIndex2,key2 in enumerate(notBinaryData[key1]):
#             dbTable.set(key1+'_'+key2,2*np.random.random_sample()-1)
#     for innerIndex1,key1 in enumerate(binaryData):
#         dbTable.set(key1,2*np.random.random_sample()-1)
#     dbTable.save()
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
