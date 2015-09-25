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

#
# APP_NAME_DEST='test@senz.analyzer.dashboard'
# APP_ID_DEST='mqip2evxqhxu8c5essmfavnk44fdm85z12gtkkzzvzwfmzqn'
# APP_KEY_DEST='awfcehevaljgo4hqn6c8gp6nhagaqtgzlae1gp5x61l91xtv'
# leancloud.init(APP_ID_DEST,APP_KEY_DEST)

APP_NAME_DEST='senz.app.dashboard'
APP_ID_DEST='2x27tso41inyau4rkgdqts0mrao1n6rq1wfd6644vdrz2qfo'
APP_KEY_DEST='3fuabth1ar3sott9sgxy4sf8uq31c9x8bykugv3zh7eam5ll'
leancloud.init(APP_ID_DEST,APP_KEY_DEST)

app_event_name_list = ['Event1','Event2']

#motion list length is 6
motion_list = ['walking', 'driving', 'sitting', 'unknown', 'running', 'riding']

#soundlv1 list length is 18

sound_list = ['shop', 'hallway', 'busy_street', 'quiet_street', 'flat', 'unknown', 'train_station', 'bedroom', 'living_room', 'supermarket', 'walk', 'bus_stop', 'study_quiet_office', 'classroom', 'subway', 'in_bus', 'forrest', 'kitchen']

#locationlv2 list length is 107

location_list = ['economy_hotel', 'outdoor', 'bath_sauna', 'technical_school', 'bike_store', 'pet_service', 'clinic', 'motorcycle', 'guest_house', 'ticket_agent', 'chinese_restaurant', 'flea_market', 'resort', 'pet_market', 'digital_store', 'coffee', 'dessert', 'cosmetics_store', 'traffic', 'work_office', 'bank', 'adult_education', 'bar', 'talent_market', 'university', 'cooler', 'convenience_store', 'snack_bar', 'home', 'post_office', 'hostel', 'motel', 'welfare_house', 'farmers_market', 'vegetarian_diet', 'high_school', 'sports_store', 'gas_station', 'training_institutions', 'muslim', 'supermarket', 'insurance_company', 'others', 'auto_sale', 'video_store', 'commodity_market', 'chafing_dish', 'housekeeping', 'residence', 'convention_center', 'atm', 'lottery_station', 'business_building', 'internet_bar', 'mother_store', 'museum', 'night_club', 'antique_store', 'japan_korea_restaurant', 'other_infrastructure', 'car_maintenance', 'odeum', 'unknown', 'hospital', 'primary_school', 'photographic_studio', 'drugstore', 'glass_store', 'bbq', 'auto_repair', 'toll_station', 'hotel', 'newstand', 'stationer', 'public_utilities', 'library', 'security_company', 'comprehensive_market', 'salvage_station', 'ktv', 'exhibition_hall', 'barbershop', 'clothing_store', 'water_supply_office', 'telecom_offices', 'furniture_store', 'gift_store', 'cinema', 'car_wash', 'travel_agency', 'photography_store', 'electricity_office', 'pawnshop', 'game_room', 'kinder_garten', 'emergency_center', 'intermediary', 'jewelry_store', 'parking_plot', 'laundry', 'scenic_spot', 'buffet', 'gallery', 'western_restaurant', 'science_museum', 'seafood', 'cigarette_store']
# length is 14
event_list = ['travel_in_scenic', 'emergency', 'work_in_office', 'go_for_concert', 'dining_in_restaurant', 'exercise_indoor', 'go_for_outing', 'exercise_outdoor', 'go_to_class', 'go_home', 'shopping_in_mall', 'movie_in_cinema', 'go_for_exhibition', 'go_work']


all_label_list = ['field__manufacture', 'field__financial', 'field__infotech', 'field__law', 'field__agriculture', 'field__human_resource', 'field__commerce', 'field__natural', 'field__service', 'field__humanities', 'field__medical', 'field__architecture', 'field__athlete', 'age__16to35', 'age__35to55', 'age__55up', 'age__16down', 'sport__basketball', 'sport__bicycling', 'sport__tabel_tennis', 'sport__football', 'sport__jogging', 'sport__badminton', 'sport__fitness', 'consumption__10000to20000', 'consumption__20000up', 'consumption__5000to10000', 'consumption__5000down', 'occupation__freelancer', 'occupation__supervisor', 'occupation__student', 'occupation__others', 'occupation__official', 'occupation__salesman', 'occupation__teacher', 'occupation__soldier', 'occupation__engineer',u'ACG', u'indoorsman', u'game_show', u'has_car', u'game_news', u'entertainment_news', u'health', u'online_shopping', u'variety_show', u'business_news', u'tvseries_show', u'current_news', u'sports_news', u'tech_news', u'offline_shopping', u'pregnant', u'gender', u'study', u'married', u'sports_show', u'gamer', u'social', u'has_pet']
# table_name='UserBehavior'
field_name='objectId'

query_limit = 1000
current_time = datetime.datetime.now()
currentTime = current_time

query = Query(User)
query.less_than('createdAt',currentTime)
query.exists(field_name)
total_count=query.count()
print 'TotalCount  %s' %str(total_count)

query_times=(total_count+query_limit-1)/query_limit


def get_all_applications(db_name='Application'):
    DbTable = db_name
    query = Query(DbTable)
    query.less_than('createdAt',current_time)
    query.exists('objectId')
    total_count=query.count()
    query_times=(total_count+query_limit-1)/query_limit
    result_list = []
    for index in range(query_times):
        query = Query(DbTable)
        query.exists('objectId')
        query.less_than('createdAt',current_time)
        query.ascending('createdAt')
        query.limit(query_limit)
        query.skip(index*query_limit)
        result_list.extend(query.find())
    return result_list

def get_all_demo_applications(db_name='DemoApplication'):
    DbTable = db_name
    query = Query(DbTable)
    query.less_than('createdAt',current_time)
    query.exists('objectId')
    total_count=query.count()
    query_times=(total_count+query_limit-1)/query_limit
    result_list = []
    for index in range(query_times):
        query = Query(DbTable)
        query.exists('objectId')
        query.less_than('createdAt',current_time)
        query.ascending('createdAt')
        query.limit(query_limit)
        query.skip(index*query_limit)
        result_list.extend(query.find())
    return result_list

def get_all_tracker(table_name=None):
    DbTable = table_name
    query = Query(DbTable)
    query.less_than('createdAt',current_time)
    query.exists('objectId')
    total_count=query.count()
    query_times=(total_count+query_limit-1)/query_limit
    user_list = []
    for index in range(query_times):
        query = Query(DbTable)
        query.exists('objectId')
        query.less_than('createdAt',current_time)
        query.ascending('createdAt')
        query.limit(query_limit)
        query.skip(index*query_limit)
        user_list.extend(query.find())
    return user_list

#注意这种分页的方法取数据时，如果user数量很大，而且读数据时user还在快速增长的话，是可能取到重复的user的，要怎么Unique呢？
#但是如果采用的是ascending的排序方式的话，貌似倒是不会出现这个情况
# for index in range(query_times):
#     print 'querying index: %s' %str(index)
#     query = Query(User)
#     query.less_than('createdAt',currentTime)
#     query.exists(field_name)
#     query.ascending('createdAt')
#     query.limit(query_limit)
#     query.skip(index*query_limit)
#     userList.extend(query.find())


tracker_list = get_all_tracker(table_name='Tracker')

print 'The length of userList is %s'  %str(len(tracker_list))

table_name = 'MergedUserContext'
field_name = 'user'
DBTable = Object.extend(table_name)
userContextDataDict = {}

application_list =get_all_demo_applications()
# application = userList[1]

for index0,tracker in enumerate(tracker_list):
    table_name = 'MergedUserContext'
    field_name = 'user'
    DBTable = Object.extend(table_name)
    tracker_id = tracker.id
    #这样处理是因为可能一个user的记录超过了一次可以读取的数量（1K条）
    query = Query(DBTable)
    query.equal_to(field_name,tracker)
    query.less_than('createdAt',currentTime)
    total_count=query.count()
    if not total_count:
        print 'user not found with user name: %s and index: %s\n' %(str(tracker_id),str(index0))
        continue
    else:
        print 'faking user index : ' +str(index0)

    # print 'TotalCount  %s' %str(total_count)

    # query_times=(total_count+query_limit-1)/query_limit
    total_record_count = total_count
    #这里只是造假数据，假设记录不会新增
    query = Query(DBTable)
    query.equal_to(field_name,tracker)
    query.less_than('createdAt',currentTime)
    query.ascending('startTime')
    query.limit(1)
    earliest_time = query.find()[0].get('startTime')

    query = Query(DBTable)
    query.equal_to(field_name,tracker)
    query.less_than('createdAt',currentTime)
    query.descending('endTime')
    query.limit(1)
    latest_time = query.find()[0].get('endTime')

    fake_timestamp_list = range(earliest_time,latest_time,int((latest_time-earliest_time)/(total_count*6)))
    list_len = len(fake_timestamp_list)
    print 'earliest_time: %s; latest_time: %s; total_count: %s;'  %(str(earliest_time),str(latest_time),str(total_count))
    print 'the length of fake_timestamp_list: %s \n' %str(list_len)



    table_name_dest='FakeAppEvent'
    DBTable = Object.extend(table_name_dest)
    for index in  range(list_len):
        random_timestamp = fake_timestamp_list.pop(np.random.randint(len(fake_timestamp_list)))
        app_event_name = app_event_name_list[np.random.randint(len(app_event_name_list))]
        info = {
            'event':app_event_name,
            'user_id':tracker_id,
            'timestamp':random_timestamp,
            'sth':'something else'
        }
        dbTable = DBTable()
        dbTable.set('application',application_list[np.random.randint(len(application_list))])
        dbTable.set('event_name',app_event_name)
        dbTable.set('info',info)
        dbTable.set('tracker',tracker)
        dbTable.set('timestamp',random_timestamp)
        dbTable.save()



print 'finished all'