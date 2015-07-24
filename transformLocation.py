# -*- coding: utf-8 -*-
import leancloud
from leancloud import Object
from leancloud import LeanCloudError
from leancloud import Query
import time
import datetime
import operator


APP_NAME='senz.datasource.timeline'
APP_ID='pin72fr1iaxb7sus6newp250a4pl2n5i36032ubrck4bej81'
APP_KEY='qs4o5iiywp86eznvok4tmhul360jczk7y67qj0ywbcq35iia'

APP_NAME_DEST='test@senz.analyzer.dashboard'
APP_ID_DEST='mqip2evxqhxu8c5essmfavnk44fdm85z12gtkkzzvzwfmzqn'
APP_KEY_DEST='awfcehevaljgo4hqn6c8gp6nhagaqtgzlae1gp5x61l91xtv'

query_limit=700
leancloud.init(APP_ID,APP_KEY)
table_name='UserLocation'
table_name_dest = 'UserGeo'
field_name='location'
currentTime = datetime.datetime.now()

DBTable = Object.extend(table_name)
query = Query(DBTable)
query.less_than('updatedAt',currentTime)
query.exists(field_name)
total_count=query.count()
print 'TotalCount  %s' %str(total_count)

query_times=(total_count+query_limit-1)/query_limit

resultDict={}

for index in range(query_times):
    print 'querying index: %s' %str(index)
    query = Query(DBTable)
    query.less_than('updatedAt',currentTime)
    query.exists(field_name)
    query.descending('updatedAt')
    query.limit(query_limit)
    query.skip(index*query_limit)
    result_list=query.find()
    for result in result_list:
        recordWrap ={}
        recordWrap['startTime'] = result.get('startTime')
        recordWrap['endTime'] = result.get('endTime')
        recordWrap['prediction'] = result.get(field_name)
        recordWrap['UserBehavior'] = result   # 这里是为了绑定userBehavior
        resultDict[result.get('user')]=recordWrap  #这里的键user由于是对象，所以即使是同一个user，他们也是不同的

leancloud.init(APP_ID_DEST,APP_KEY_DEST)

DBTable = Object.extend(table_name_dest)
probList =[]
for index ,key in enumerate(resultDict.keys()):
    # print 'saving index: %s' %str(index)
    dbTable = DBTable()
    if resultDict[key]['prediction']:
        mostLikelyPrediction = sorted(resultDict[key]['prediction'],key = lambda behavior: behavior['behavior']['prob'],reverse=True)[0]
        dbTable.set('sound',mostLikelyPrediction['behavior']['senzList'][0]['sound'])
        dbTable.set('motion',mostLikelyPrediction['behavior']['senzList'][0]['motion'])
        dbTable.set('location',mostLikelyPrediction['behavior']['senzList'][0]['location'])
        dbTable.set('prob',mostLikelyPrediction['behavior']['prob'])
        dbTable.set('event',sorted(mostLikelyPrediction['prediction'].items(),key=operator.itemgetter(1),reverse=True)[0])
        dbTable.set('startTime',resultDict[key]['startTime'])
        dbTable.set('endTime',resultDict[key]['endTime'])
        dbTable.set('user',key)
        dbTable.set('userBehavior',resultDict[key]['UserBehavior'])
        dbTable.save()
        # print 'saving successfully  index: %s' %str(index)

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
