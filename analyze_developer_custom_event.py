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

# APP_NAME_DEST='test@senz.analyzer.dashboard'
# APP_ID_DEST='mqip2evxqhxu8c5essmfavnk44fdm85z12gtkkzzvzwfmzqn'
# APP_KEY_DEST='awfcehevaljgo4hqn6c8gp6nhagaqtgzlae1gp5x61l91xtv'
# leancloud.init(APP_ID_DEST,APP_KEY_DEST)


APP_NAME_DEST='senz.app.dashboard'
APP_ID_DEST='2x27tso41inyau4rkgdqts0mrao1n6rq1wfd6644vdrz2qfo'
APP_KEY_DEST='3fuabth1ar3sott9sgxy4sf8uq31c9x8bykugv3zh7eam5ll'
leancloud.init(APP_ID_DEST,APP_KEY_DEST)
# analyze developer's custom event

app_event_name_list = ['Event1','Event2']

#latest motion_type updated at 2015.9.25(database updated at 2015-05-21 17:31:42 ) total length is 6
motion_list = ['walking', 'driving', 'sitting', 'unknown', 'running', 'riding']

#latest sound_level1_type updated at 2015.9.25(database updated at 2015-07-16 19:09:02) total length is 18

sound_list = ['shop', 'hallway', 'busy_street', 'quiet_street', 'flat', 'unknown', 'train_station', 'bedroom', 'living_room', 'supermarket', 'walk', 'bus_stop', 'study_quiet_office', 'classroom', 'subway', 'in_bus', 'forrest', 'kitchen']

#locationlv2 list length is 107 updated at 2015.7.20

# location_list = ['economy_hotel', 'outdoor', 'bath_sauna', 'technical_school', 'bike_store', 'pet_service', 'clinic', 'motorcycle', 'guest_house', 'ticket_agent', 'chinese_restaurant', 'flea_market', 'resort', 'pet_market', 'digital_store', 'coffee', 'dessert', 'cosmetics_store', 'traffic', 'work_office', 'bank', 'adult_education', 'bar', 'talent_market', 'university', 'cooler', 'convenience_store', 'snack_bar', 'home', 'post_office', 'hostel', 'motel', 'welfare_house', 'farmers_market', 'vegetarian_diet', 'high_school', 'sports_store', 'gas_station', 'training_institutions', 'muslim', 'supermarket', 'insurance_company', 'others', 'auto_sale', 'video_store', 'commodity_market', 'chafing_dish', 'housekeeping', 'residence', 'convention_center', 'atm', 'lottery_station', 'business_building', 'internet_bar', 'mother_store', 'museum', 'night_club', 'antique_store', 'japan_korea_restaurant', 'other_infrastructure', 'car_maintenance', 'odeum', 'unknown', 'hospital', 'primary_school', 'photographic_studio', 'drugstore', 'glass_store', 'bbq', 'auto_repair', 'toll_station', 'hotel', 'newstand', 'stationer', 'public_utilities', 'library', 'security_company', 'comprehensive_market', 'salvage_station', 'ktv', 'exhibition_hall', 'barbershop', 'clothing_store', 'water_supply_office', 'telecom_offices', 'furniture_store', 'gift_store', 'cinema', 'car_wash', 'travel_agency', 'photography_store', 'electricity_office', 'pawnshop', 'game_room', 'kinder_garten', 'emergency_center', 'intermediary', 'jewelry_store', 'parking_plot', 'laundry', 'scenic_spot', 'buffet', 'gallery', 'western_restaurant', 'science_museum', 'seafood', 'cigarette_store']

# latest location_level2_type_ key updated at 2015.9.25(database updated at 2015-08-11 20:44:27) total length is 180
locationlv2 = ['economy_hotel','golf','technical_school','welfare_house','factory','outdoor','skiing','sports_venues','ticket_agent_train','public_phone','chinese_restaurant','flea_market','water_supply_office','emergency_center','cosmetics_store','tennis_court','memorial_hall','bar','talent_market','bus_stop','subway','villa','convenience_store','snack_bar','housekeeping_water_deliver','post_office','motorcycle_sell','movie','agriculture_forestry_and_fishing_base','dormitory','vegetarian_diet','railway_station','minor_institutions','sports_store','ticket_agent','supermarket','insurance_company','aquarium','auto_sale','chafing_dish','residence','convention_center','housekeeping_house_moving','other_hotel','airport','public_toilet','coach_station','mother_store','cultural_venues','fishing_garden','church','car_maintenance','odeum','hospital','housekeeping_lock','primary_school','bbq','toll_station','stationer','bath_sauna','comprehensive_market','cooler_store','foreign_institutional','furniture_store','travel_agency','special_hospital','electricity_office','pawnshop','game_room','ticket_agent_plane','dessert','intermediary','jewelry_store','parking_plot','laundry','seafood_restaurant','motorcycle_repair','scenic_spot','chess_room','driving_school','holiday_village','western_restaurant','science_museum','theater','telecom_offices','cigarette_store','ticket_agent_coach','bike_store','pet_service','clinic','telecom_offices_unicom','motorcycle','guest_house','football_field','botanic_garden','resort','housekeeping_hour','pet_market','digital_store','coffee','farm_house','music_hall','traffic','work_office','bank','adult_education','university','japan_restaurant','government_agency','korea_restaurant','japan_korea_restaurant','ticket_agent_bus','telecom_offices_tietong','library','highway_service_area','home','horsemanship','hostel','race_course','farmers_market','playground','high_school','temple','city_square','gas_station','training_institutions','telecom_offices_netcom','housekeeping_nanny','antique_store','video_store','refuge','basketball_court','commodity_market','housekeeping','atm','lottery_station','business_building','internet_bar','museum','night_club','traffic_place','picking_garden','other_infrastructure','apartment_hotel','muslim_dish','tax_authorities','cultural_palace','photographic_studio','drugstore','scientific_research_institution','auto_repair','telecom_offices_mobile','bus_route','hotel','park','newstand','community_center','public_utilities','security_company','salvage_station','ktv','glass_store','barbershop','housekeeping_alliance_repair','clothing_store','industrial_area','gift_store','cinema','car_wash','inn','motorcycle_service','bathing_beach','telecom_offices_telecom','kinder_garten','zoo','shopping_street','exhibition_hall','buffet','gallery','subway_track']

# length is 14
# event_list = ['travel_in_scenic', 'emergency', 'work_in_office', 'go_for_concert', 'dining_in_restaurant', 'exercise_indoor', 'go_for_outing', 'exercise_outdoor', 'go_to_class', 'go_home', 'shopping_in_mall', 'movie_in_cinema', 'go_for_exhibition', 'go_work']

# latest events_type updated at 2015.9.25(database updated at 2015-08-11 16:09:28) total length is 10
event_list = ['go_outing','work_in_office','visit_sights','watch_movie','exercise_outdoor','study_in_class','exercise_indoor','shopping_in_mall','attend_concert','dining_in_restaurant']


# length is 60
all_label_list = ['field__manufacture', 'field__financial', 'field__infotech', 'field__law', 'field__agriculture', 'field__human_resource', 'field__commerce', 'field__natural', 'field__service', 'field__humanities', 'field__medical', 'field__architecture', 'field__athlete', 'age__16to35', 'age__35to55', 'age__55up', 'age__16down', 'sport__basketball', 'sport__bicycling', 'sport__tabel_tennis', 'sport__football', 'sport__jogging', 'sport__badminton', 'sport__fitness', 'consumption__10000to20000', 'consumption__20000up', 'consumption__5000to10000', 'consumption__5000down', 'occupation__freelancer', 'occupation__supervisor', 'occupation__student', 'occupation__others', 'occupation__official', 'occupation__salesman', 'occupation__teacher', 'occupation__soldier', 'occupation__engineer',u'ACG', u'indoorsman', u'game_show', u'has_car', u'game_news', u'entertainment_news', u'health', u'online_shopping', u'variety_show', u'business_news', u'tvseries_show', u'current_news', u'sports_news', u'tech_news', u'offline_shopping', u'pregnant', u'gender', u'study', u'married', u'sports_show', u'gamer', u'social', u'has_pet']
# table_name='UserBehavior'
field_name='objectId'

query_limit = 1000
current_time = datetime.datetime.now()

def get_all_tracker(table_name='Tracker'):
    DbTable = Object.extend(table_name)
    query = Query(DbTable)
    query.less_than('createdAt',current_time)
    query.exists('objectId')
    total_count=query.count()
    query_times=(total_count+query_limit-1)/query_limit
    tracker_list = []
    for index in range(query_times):
        query = Query(DbTable)
        query.exists('objectId')
        query.less_than('createdAt',current_time)
        query.ascending('createdAt')
        query.limit(query_limit)
        query.skip(index*query_limit)
        tracker_list.extend(query.find())
    return tracker_list
# 功能：为当前所有应用造一些开发者自定义的event
# 过程：fake_custom_event
#      =>[get_all_tracker(得到所有存在的tracker)
#         ,[get_all_demo_applications
#             ,get_all_applications]
#         ,(遍历每个tracker,根据每个tracker出现在MergedUserContext中的最早和最晚的时间，在中间时间段造出被触发的用户自定义event）
#         ]
# 输入：<所有的Application>(包括demo)，<根据UserBehavior得到的MergedUserContext的所有tracker的信息>
# 输出：每个开发者应用的自定义event
def fake_custom_event():
    tracker_list = get_all_tracker(table_name='Tracker')
    print 'The length of userList is %s'  %str(len(tracker_list))

    table_name = 'MergedUserContext'
    field_name = 'tracker'
    DBTable = Object.extend(table_name)

    userContextDataDict = {}
    all_application_list = get_all_demo_applications()
    all_application_list.extend(get_all_applications())

    for index,tracker in enumerate(tracker_list):
        tracker_id = tracker.id
        #这样处理是因为可能一个user的记录超过了一次可以读取的数量（1K条）
        query = Query(DBTable)
        query.equal_to(field_name,tracker)
        query.less_than('createdAt',current_time)
        total_count=query.count()
        if not total_count:
            print 'tracker not found with tracker id: %s and index: %s\n' %(str(tracker_id),str(index))
            continue
        else:
            print 'faking tracker index : ' +str(index)

        #这里只是造假数据，假设记录不会新增
        query = Query(DBTable)
        query.equal_to(field_name,tracker)
        query.less_than('createdAt',current_time)
        query.ascending('startTime')
        query.limit(1)
        earliest_time = query.find()[0].get('startTime')

        query = Query(DBTable)
        query.equal_to(field_name,tracker)
        query.less_than('createdAt',current_time)
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
            # 随机选取一个event
            app_event_name = app_event_name_list[np.random.randint(len(app_event_name_list))]
            info = {
                'event':app_event_name,
                'tracker_id':tracker_id,
                'timestamp':random_timestamp,
                'sth':'something else'
            }
            dbTable = DBTable()
            dbTable.set('application',all_application_list[np.random.randint(len(all_application_list))])
            dbTable.set('event_name',app_event_name)
            dbTable.set('info',info)
            dbTable.set('tracker',tracker)
            dbTable.set('timestamp',random_timestamp)
            dbTable.save()


def get_all_applications(table_name='Application'):
    DbTable = Object.extend(table_name)
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

def get_all_demo_applications(table_name='DemoApplication'):
    DbTable = Object.extend(table_name)
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

def get_all_event(table_name='FakeAppEvent'):
    # return the application : event_list dict
    application_list = get_all_demo_applications()
    application_list.extend(get_all_applications())
    print 'already get all application'
    DbTable = Object.extend(table_name)
    application_event_dict = {}
    for index,application in enumerate(application_list):
        print 'getting event in application index : %s' %(str(index))
        query = Query(DbTable)
        query.equal_to('application',application)
        query.less_than('createdAt',current_time)
        total_count=query.count()
        query_times=(total_count+query_limit-1)/query_limit

        event_list = []
        for index in range(query_times):
            query = Query(DbTable)
            query.equal_to('application',application)
            query.less_than('createdAt',current_time)
            query.ascending('createdAt')
            query.limit(query_limit)
            query.skip(index*query_limit)
            event_list.extend(query.find())
        event_dict = {}
        if event_list:
            for event in event_list:
                event_name = event.get('event_name')

                if event_name in event_dict.keys():
                    # print event_dict
                    event_dict[event_name].append(event)
                else:
                    event_dict[event_name] = [event]
            application_event_dict[application] = event_dict
        else:
            pass

    return application_event_dict


# 8.13 保存所有application的event_list

def set_app_event_list(table_name='AllAppEvent'):

    application_event_dict = get_all_event()
    print 'already get the application_event_dict'
    print str(len(application_event_dict.keys()))
    DBTable = Object.extend(table_name)
    for application,event_dict in application_event_dict.items():
        dbTable = DBTable()
        dbTable.set('event_list',event_list)
        dbTable.set('application',application)
        dbTable.save()
# 功能：分析开发者自定义event触发时tracker正在做的事情，目前是统计所有tracker的UserEvent出现的次数，最后输出的是每个event中UserEvent出现次数的字典
# 过程：associate_event_and_activity
#      =>get_all_event(得到每个应用的开发者自定义事件，和触发该事件的记录的字典)
#         =>[get_all_demo_applications
#             ,get_all_applications]
#      =>(遍历每个application的每个event，统计对应timestamp和tracker的UserEvent)
# 输入：<开发者App自定义的event>(目前是根据MergedUserContext出现的时间造的假数据)，<通过对Senz的分析计算出的UserEvent>(目前shixiang那边计算出的UserEvent可用性太低(数量少，时间密度大）)，暂时使用的是yuanzhe算的MergedUserContext中的eventType)
# 输出：每个开发者自定义event和其被触发时对应的UserEvent出现次数的字典
def associate_event_and_activity(db_name='MergedUserContext'):

    application_event_dict = get_all_event()
    print 'already get the application_event_dict'
    print str(len(application_event_dict.keys()))
    DBTable = Object.extend(db_name)

    # print 'event_list: %s' %str(application_event_dict.values())
    for application,event_dict in application_event_dict.items():
        # print event_dict
        if event_dict:
            print 'application_event_dict values first count: %s' %str(event_dict.keys())

        EventActivity = Object.extend('FakeEventActivity')
        for event_name,event_list in event_dict.items():
            total_count = len(event_list)
            print 'event_list total_count: %s with event_name is: %s' %(str(total_count),event_name)
            print 'application id is: %s' %str(application.id)

            event_activity = EventActivity()
            relation = event_activity.relation('event')
            activity_dict = {}
            for index,event in enumerate(event_list):
                relation.add(event)
                query = Query(DBTable)
                query.equal_to('tracker',event.get('tracker'))
                query.less_than_or_equal_to('startTime',event.get('timestamp'))
                query.greater_than_or_equal_to('endTime',event.get('timestamp'))
                activity_list = query.find()
                if len(activity_list) == 1 or len(activity_list) == 2 :
                    # for the convenience of adding the dimension of time to the analyzer
                    event.set('activity',activity_list[0])
                    event.save()
                    # activity = activity_list[0].get('eventType')[0]
                    activity = activity_list[0].get('eventType')[0]

                    if activity in activity_dict.keys():
                        activity_dict[activity]+=1
                    else:
                        activity_dict[activity] =1
                else:
                    event.destroy()
                    print 'length of activity_list: %s' %(str(len(activity_list)))
                    print 'Seems to be an error,index: %s,user: %s; timestamp: %s \n' %(str(index),str(event.get('tracker').id ),str(event.get('timestamp')))

            other_activity_total_count =total_count-sum(activity_dict.values())
            if other_activity_total_count:
                activity_dict['others'] = other_activity_total_count

            # EventActivity = Object.extend('EventActivity')
            # event_activity = EventActivity()
            event_activity.set('application',application)
            event_activity.set('event_name',event_list[0].get('event_name'))
            event_activity.set('activity_dict',activity_dict)
            event_activity.save()


# 8.13 计算所有event被触发时用户的地点分布
def associate_event_and_location(db_name='MergedUserContext'):
    application_event_dict = get_all_event()
    print 'already get the application_event_dict'
    print str(len(application_event_dict.keys()))
    DBTable = Object.extend(db_name)

    # print 'event_list: %s' %str(application_event_dict.values())
    for application,event_dict in application_event_dict.items():
        # print event_dict
        if event_dict:
            print 'application_event_dict values first count: %s' %str(event_dict.keys())
        EventActivity = Object.extend('FakeEventActivity')
        for event_name,event_list in event_dict.items():
            total_count = len(event_list)
            print 'event_list total_count: %s with event_name is: %s' %(str(total_count),event_name)
            print 'application id is: %s' %str(application.id)

            event_activity = EventActivity()
            relation = event_activity.relation('event')
            location_dict = {}
            for index,event in enumerate(event_list):
                relation.add(event)
                query = Query(DBTable)
                query.equal_to('tracker',event.get('tracker'))
                query.less_than_or_equal_to('startTime',event.get('timestamp'))
                query.greater_than_or_equal_to('endTime',event.get('timestamp'))
                activity_list = query.find()
                if len(activity_list) == 1 or len(activity_list) == 2 :
                    # for the convenience of adding the dimension of time to the analyzer
                    event.set('activity',activity_list[0])
                    event.save()
                    # activity = activity_list[0].get('eventType')[0]
                    location = activity_list[0].get('location')

                    if location in location_dict.keys():
                        location_dict[location]+=1
                    else:
                        location_dict[location] =1
                else:
                    # event.destroy()
                    print 'length of activity_list: %s' %(str(len(activity_list)))
                    print 'Seems to be an error,index: %s,user: %s; timestamp: %s \n' %(str(index),str(event.get('tracker').id ),str(event.get('timestamp')))

            other_location_total_count =total_count-sum(location_dict.values())
            if other_location_total_count:
                location_dict['others'] = other_location_total_count

            # EventActivity = Object.extend('EventActivity')
            # event_activity = EventActivity()
            event_activity.set('application',application)
            event_activity.set('event_name',event_list[0].get('event_name'))
            event_activity.set('location_dict',location_dict)
            event_activity.save()


if __name__ == '__main__':
# 功能：分析开发者自定义event触发时tracker正在做的事情，目前是统计所有tracker的UserEvent出现的次数，最后输出的是每个event中UserEvent出现次数的字典
# 过程：associate_event_and_activity
#      =>get_all_event(得到每个应用的开发者自定义事件，和触发该事件的记录的字典)
#         =>[get_all_demo_applications
#             ,get_all_applications]
#      =>(遍历每个application的每个event，统计对应timestamp和tracker的UserEvent)
# 输入：<开发者App自定义的event>(目前是根据UserEvent出现的时间造的假数据)，<通过对Senz的分析计算出的UserEvent>(目前shixiang那边计算出的UserEvent可用性太低(数量少，时间密度大）)，暂时使用的是yuanzhe算的MergedUserContext中的eventType)
# 输出：每个开发者自定义event和其被触发时对应的UserEvent出现次数的字典
#     associate_event_and_activity()
    associate_event_and_location()


