# -*- coding: utf-8 -*-
import leancloud
from leancloud import Object
from leancloud import LeanCloudError
from leancloud import Query
from leancloud import Installation
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

#整个流程是先计算所有tracker的staticInfo，然后根据app和tracker的对应关系，去计算一个app的值
# APP_NAME_DEST='senz.log.tracer'
# APP_ID_DEST='9ra69chz8rbbl77mlplnl4l2pxyaclm612khhytztl8b1f9o'
# APP_KEY_DEST='1zohz2ihxp9dhqamhfpeaer8nh1ewqd9uephe9ztvkka544b'
# leancloud.init(APP_ID_DEST,APP_KEY_DEST)

APP_NAME_DEST='senz.app.dashboard'
APP_ID_DEST='2x27tso41inyau4rkgdqts0mrao1n6rq1wfd6644vdrz2qfo'
APP_KEY_DEST='3fuabth1ar3sott9sgxy4sf8uq31c9x8bykugv3zh7eam5ll'
leancloud.init(APP_ID_DEST,APP_KEY_DEST)

# analyze developer's custom event

query_limit = 1000
current_time = datetime.datetime.now()


not_binary_label_list = [['field__manufacture', 'field__financial', 'field__infotech', 'field__law', 'field__agriculture', 'field__human_resource', 'field__commerce', 'field__natural', 'field__service', 'field__humanities', 'field__medical', 'field__architecture', 'field__athlete'], ['age__16to35', 'age__35to55', 'age__55up', 'age__16down'], ['sport__basketball', 'sport__bicycling', 'sport__tabel_tennis', 'sport__football', 'sport__jogging', 'sport__badminton', 'sport__fitness'], ['consumption__10000to20000', 'consumption__20000up', 'consumption__5000to10000', 'consumption__5000down'], ['occupation__freelancer', 'occupation__supervisor', 'occupation__student', 'occupation__others', 'occupation__official', 'occupation__salesman', 'occupation__teacher', 'occupation__soldier', 'occupation__engineer']]
binary_label_list = [u'ACG', u'indoorsman', u'game_show', u'has_car', u'game_news', u'entertainment_news', u'health', u'online_shopping', u'variety_show', u'business_news', u'tvseries_show', u'current_news', u'sports_news', u'tech_news', u'offline_shopping', u'pregnant', u'gender', u'study', u'married', u'sports_show', u'gamer', u'social', u'has_pet']
all_label_list = ['field__manufacture', 'field__financial', 'field__infotech', 'field__law', 'field__agriculture', 'field__human_resource', 'field__commerce', 'field__natural', 'field__service', 'field__humanities', 'field__medical', 'field__architecture', 'field__athlete', 'age__16to35', 'age__35to55', 'age__55up', 'age__16down', 'sport__basketball', 'sport__bicycling', 'sport__tabel_tennis', 'sport__football', 'sport__jogging', 'sport__badminton', 'sport__fitness', 'consumption__10000to20000', 'consumption__20000up', 'consumption__5000to10000', 'consumption__5000down', 'occupation__freelancer', 'occupation__supervisor', 'occupation__student', 'occupation__others', 'occupation__official', 'occupation__salesman', 'occupation__teacher', 'occupation__soldier', 'occupation__engineer',u'ACG', u'indoorsman', u'game_show', u'has_car', u'game_news', u'entertainment_news', u'health', u'online_shopping', u'variety_show', u'business_news', u'tvseries_show', u'current_news', u'sports_news', u'tech_news', u'offline_shopping', u'pregnant', u'gender', u'study', u'married', u'sports_show', u'gamer', u'social', u'has_pet']

# table_name='UserBehavior'
field_name='objectId'
currentTime = datetime.datetime.now()



def flat_static_info_object():
    pass

def get_all_installation(table_name='_Installation'):
    DbTable = Object.extend(table_name)
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



def connect_tracker_installation(table_name='Application'):
    all_installation = get_all_installation()
    print 'already get all installation ,length is %s' %(str(len(all_installation)))
    for index,installation in enumerate(all_installation):
        application = installation.get('application')
        try:
            application.fetch()
            if application:
                print application.get('name') ,str(index)
            else:
                print 'application not exists in this installation with installation id : %s' %(str(installation.id))
            relation = application.relation('tracker')
            tracker = installation.get('user')
            if tracker:
                tracker.fetch()
                relation.add(tracker)
                application.save()
            else:
                print 'user not exists in this installation with installation id :%s' %(str(installation.id))
        except LeanCloudError,e:
            print e
            print 'error in installation with object id : %s' %(str(installation.id))

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

def get_tracker_data(table_name=None,tracker_list=None,field_name=None):

    DBTable = Object.extend(table_name)
    tracker_data_dict = {}
    for index,tracker in enumerate(tracker_list):
        #这样处理是因为可能一个user的记录超过了一次可以读取的数量（1K条）
        query = Query(DBTable)
        query.equal_to(field_name,tracker)
        query.less_than('createdAt',currentTime)
        total_count=query.count()
        # print 'TotalCount  %s' %str(total_count)

        query_times=(total_count+query_limit-1)/query_limit
        #如果想在这里按timestamp排序取出每个user的记录是不靠谱的，可能读取时还有插入，而且timestamp介于之间
        for index in range(query_times):
            # print 'querying index: %s' %str(index)
            query = Query(DBTable)
            query.equal_to(field_name,tracker)
            query.less_than('createdAt',currentTime)
            query.ascending('createdAt')
            query.limit(query_limit)
            query.skip(index*query_limit)
            if tracker in userDataDict.keys():
                tracker_data_dict.get(tracker).extend(query.find())
            else :
                tracker_data_dict[tracker]=query.find()
    return tracker_data_dict

def weight_tracker_static_info(table_name='WeightedTrackerInfo'):
    all_tracker_list = get_all_tracker(table_name='Tracker')
     # tracker_data_dict的键是tracker，值是一个tracker的所有的staticInfo记录
    tracker_data_dict = get_tracker_data(table_name='StaticInfo',tracker_list=all_tracker_list,field_name='user')
    weightedUserDataDict ={}
    DBTable = Object.extend(table_name)
    for tracker,tracker_record_list in tracker_data_dict.items():

        print 'the length of this tracker_record_list is %s ' %str(len(tracker_record_list))
        dbTable = DBTable()
        dbTable.set('tracker',tracker)
        dbTable.set('staticInfoCount',len(tracker_record_list))
        # recordCount = len(userData)
        raw_weight = np.arange(1,len(tracker_record_list)+1)
        normalizedWeight = raw_weight/float(np.sum(raw_weight)) #将所有的权重归一化
        #接着将所有的label按照labelList的顺序放到一个大的矩阵中
        field_value_matrix = np.empty([len(tracker_record_list),len(all_label_list)+1])
        print 'the shape of this field_value_matrix is %s' %str(np.shape(field_value_matrix))
        for index1 ,record in enumerate(tracker_record_list):
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
        weightedValueDict = {}
        for index2,field in enumerate(all_label_list):
            log.info('sorted_field_value_matrix:  \n' + str(sorted_field_value_matrix))
            weightedValue = np.dot(sorted_field_value_matrix[:,index2].flatten(),normalizedWeight)
            log.info('normalizedWeight:  \n' + str(normalizedWeight))
            log.info('weightedValue:  \n' + str(weightedValue))
            weightedValueDict[field] = weightedValue

        for field_list in not_binary_label_list:
            valueDict = {field:weightedValueDict[field] for field in field_list}
            mostPossibleFieldTuple = sorted(valueDict.items(),key = lambda l:l[1],reverse=True)[0]
            weightedValueDict[field_list[0].split('__')[0]]={mostPossibleFieldTuple[0].split('__')[1]:mostPossibleFieldTuple[1]}
        for field ,value in weightedValueDict.items():
            dbTable.set(field,value)

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


def get_all_trackers(db_name='Tracker'):
    print 'begin get_all_applications'
    all_application_list = get_all_applications()
    print 'length of application_list is %s' %(str(len(all_application_list)))
    print 'end get_all_applications'
    application_tracker_dict = {}
    for app in all_application_list:
        relation = app.relation('tracker')
        query = relation.query()

        #这里的query貌似没有count方法，如果tracker数量超过了1000怎么办呢，这里的query.find()的limit是1K么
        # total_count=query.count()
        # print 'total count of app: %s  is %s ' %(str(app.id),str(total_count))
        # query_times=(total_count+query_limit-1)/query_limit
        # result_list = []
        # for index in range(query_times):
        #     query = relation.query()
        #     query.less_than('createdAt',current_time)
        #     query.ascending('createdAt')
        #     query.limit(query_limit)
        #     query.skip(index*query_limit)
        #     result_list.extend(query.find())
        result_list = query.find()
        if result_list:
            application_tracker_dict[app] = result_list
        print 'this application-tracker_list length is %s' %(str(len(result_list)))
    print 'the length of application_tracker_dict is %s' %(str(len(application_tracker_dict.keys())))
    return application_tracker_dict


def get_age_and_gender_data_dict(table_name='WeightedStaticInfo',tracker_list=None):
    try:

        field_name = 'user'
        DBTable  = Object.extend(table_name)
        gender_type_list =['man','woman']
        age_type_list = ['16down','16to35','35to55','55up']
        dataDict ={gender_type:{age_type:0 for age_type in age_type_list} for gender_type in gender_type_list}
        # new_data_dict = {key:[0 for i in range(4)] for key in dataDict.keys()}
        total_count = len(tracker_list)
        for index, tracker in enumerate(tracker_list):
            query = Query(DBTable)
            query.equal_to(field_name,tracker)
            query.select('age','gender')
            result_list = query.find()
            length = len(result_list)
            if length!=1:
                print 'error: the length of result_list is %s with index: %s with tracker_objectId: %s' %(str(length),str(index),tracker.id)
            if length >=1:
                result = result_list[0]
            else:
                continue

            print 'index: %s  gender: %s  age: %s ' %(str(index),str(result.get('gender')),str(result.get('age')))
            gender = 'man' if result.get('gender') >0 else 'woman'
            age_info_dict= result.get('age')
            dataDict[gender][age_info_dict.keys()[0]] += 1
            # dataDict ={'man' if staticInfo.get('gender') >0 else 'woman':dataDict['man' if staticInfo.get('gender') >0 else 'woman'][staticInfo.get('age').keys()[0]] +=1 for staticInfo in staticInfoList}

            # for index ,age_type in enumerate(age_type_list):
            #     for gender_type in dataDict.keys():
            #         new_data_dict[gender_type][index] = dataDict[gender_type][age_type]
        known_count = sum(dataDict['man'].values())+sum(dataDict['woman'].values())
        dataDict['unknown'] = total_count -known_count
    except LeanCloudError, e:
         raise e
    return dataDict

def get_occupation_data_dict(table_name='WeightedStaticInfo',tracker_list=None):
    try:
        field_name = 'user'
        DBTable  = Object.extend(table_name)
        gender_type_list =['man','woman']
        age_type_list = ['16down','16to35','35to55','55up']
        dataDict ={gender_type:{age_type:0 for age_type in age_type_list} for gender_type in gender_type_list}
        # new_data_dict = {key:[0 for i in range(4)] for key in dataDict.keys()}
        total_count = len(tracker_list)
        for index, tracker in enumerate(tracker_list):
            query = Query(DBTable)
            query.equal_to(field_name,tracker)
            query.select('age','gender')
            result_list = query.find()
            length = len(result_list)
            if length!=1:
                print 'error: the length of result_list is %s with index: %s with tracker_objectId: %s' %(str(length),str(index),tracker.id)
            if length >=1:
                result = result_list[0]
            else:
                continue

            print 'index: %s  gender: %s  age: %s ' %(str(index),str(result.get('gender')),str(result.get('age')))
            gender = 'man' if result.get('gender') >0 else 'woman'
            age_info_dict= result.get('age')
            dataDict[gender][age_info_dict.keys()[0]] += 1
            # dataDict ={'man' if staticInfo.get('gender') >0 else 'woman':dataDict['man' if staticInfo.get('gender') >0 else 'woman'][staticInfo.get('age').keys()[0]] +=1 for staticInfo in staticInfoList}

            # for index ,age_type in enumerate(age_type_list):
            #     for gender_type in dataDict.keys():
            #         new_data_dict[gender_type][index] = dataDict[gender_type][age_type]
        known_count = sum(dataDict['man'].values())+sum(dataDict['woman'].values())
        dataDict['unknown'] = total_count -known_count
    except LeanCloudError, e:
         raise e
    return dataDict

def analyze_tracker_static_info(table_name='AppStaticInfo'):
    DBTable = Object.extend(table_name)

    print 'begin get_all_trackers'
    application_tracker_dict = get_all_trackers(db_name='Tracker')
    print 'end get_all_trackers'
    for app,tracker_list in application_tracker_dict.items():
        dbTable = DBTable()
        print 'begin get_age_and_gender_data_dict'
        age_and_gender_dict = get_age_and_gender_data_dict(table_name='WeightedStaticInfo',tracker_list=tracker_list)
        print 'end get_age_and_gender_data_dict'
        dbTable.set('age_and_gender',age_and_gender_dict)
        dbTable.set('app',app)
        relation = dbTable.relation('tracker')
        for tracker in tracker_list:
            relation.add(tracker)
        dbTable.save()
def get_tracker_context(table_name='WeightedUserContext',tracker_list=None):
    field = 'location'
    DBTable  = Object.extend(table_name)
    k = 5
    unknown = 'unknown'
    # try:
    #     WeightedStaticInfo = Object.extend('WeightedUserContext')
    #     query = Query(WeightedStaticInfo)
    #     query.exists('objectId')
    #     query.select(field)
    #     # 这个地方后面需要做根据applicationid查询
    #     #另外也需要分组查询
    #     resultList = query.find()
    #     seen_location_dict = {}
    #     user_count = len(resultList)
    #
    #     for result in resultList:
    #         location_dict = result.get(field)
    #         for key, value in location_dict.items():
    #             if key in seen_location_dict.keys():
    #                 seen_location_dict[key] += location_dict[key]
    #             else:
    #                 seen_location_dict[key] = location_dict[key]
    #     total_unknown_location_value = seen_location_dict.get(unknown)
    #     #如果seen_location_dict中含有unknown字段的话，就删掉
    #     if total_unknown_location_value:
    #         del seen_location_dict[unknown]
    #
    #     sorted_seen_location = sorted(seen_location_dict.items(), key=lambda l: l[1], reverse=True)
    #     sorted_frequent_location = sorted_seen_location[0:k]
    #     total_known_time = user_count - total_unknown_location_value
    #     sorted_frequent_location_percentage = [(str(kv[0]),(kv[1]/total_known_time)) for kv in sorted_frequent_location]
    #     sorted_frequent_location_percentage.append(('others',1-sum([kv[1] for kv in sorted_frequent_location_percentage])))
    #
    #
    #
    # except LeanCloudError, e:
    #
    #      raise e
    # return sorted_frequent_location_percentage

    try:

        field_name='user'
        tracker_location_list = []
        seen_location_dict = {}
        tracker_count = len(tracker_list)
        print tracker_count

        for index, tracker in enumerate(tracker_list):
            query = Query(DBTable)
            query.equal_to(field_name,tracker)
            query.select('location')
            result_list = query.find()
            length = len(result_list)
            if length!=1:
                print 'error: the length of result_list is %s with index: %s with tracker_objectId: %s' %(str(length),str(index),tracker.id)
            if length >=1:
                tracker_location_list.append(result_list[0])
            else:
                continue
        # print len(tracker_location_list)
        for result in tracker_location_list:
            location_dict = result.get(field)
            for key, value in location_dict.items():
                if key in seen_location_dict.keys():
                    seen_location_dict[key] += location_dict[key]
                else:
                    seen_location_dict[key] = location_dict[key]
        print str(seen_location_dict)
        tracker_context_exists_count = len(tracker_location_list)
        # total_unknown_location_value = seen_location_dict.get(unknown)
        # #如果seen_location_dict中含有unknown字段的话，就删掉
        # if total_unknown_location_value:
        #     del seen_location_dict[unknown]
        #
        # sorted_seen_location = sorted(seen_location_dict.items(), key=lambda l: l[1], reverse=True)
        # sorted_frequent_location = sorted_seen_location[0:k]
        # total_known_time = tracker_count - total_unknown_location_value
        # sorted_frequent_location_percentage = [(str(kv[0]),(kv[1]/total_known_time)) for kv in sorted_frequent_location]
        # sorted_frequent_location_percentage.append(('others',1-sum([kv[1] for kv in sorted_frequent_location_percentage])))


    except LeanCloudError, e:
         raise e
    return seen_location_dict,


def analyze_tracker_context(table_name='WeightedUserContext'):
    DbTable = Object.extend(table_name)

    print 'begin analyze_tracker_context'
    application_tracker_dict = get_all_trackers(db_name='Tracker')
    print 'end get_all_trackers'
    for app,tracker_list in application_tracker_dict.items():
        dbTable = DbTable()
        print 'begin get_tracker_context'
        age_and_gender_dict = get_tracker_context(table_name='WeightedUserContext',tracker_list=tracker_list)
        print age_and_gender_dict
        print 'end get_tracker_context'
        # dbTable.set('age_and_gender',age_and_gender_dict)
        # dbTable.set('app',app)
        # relation = dbTable.relation('tracker')
        # for tracker in tracker_list:
        #     relation.add(tracker)
        # dbTable.save()

# def get_location_distribution_data_dict():
#         field = 'location'
#         k = 5
#         unknown = 'unknown'
#         try:
#             WeightedStaticInfo = Object.extend('WeightedUserContext')
#             query = Query(WeightedStaticInfo)
#             query.exists('objectId')
#             query.select(field)
#             # 这个地方后面需要做根据applicationid查询
#             #另外也需要分组查询
#             resultList = query.find()
#             seen_location_dict = {}
#             user_count = len(resultList)
#
#             for result in resultList:
#                 location_dict = result.get(field)
#                 for key, value in location_dict.items():
#                     if key in seen_location_dict.keys():
#                         seen_location_dict[key] += location_dict[key]
#                     else:
#                         seen_location_dict[key] = location_dict[key]
#             total_unknown_location_value = seen_location_dict.get(unknown)
#             #如果seen_location_dict中含有unknown字段的话，就删掉
#             if total_unknown_location_value:
#                 del seen_location_dict[unknown]
#
#             sorted_seen_location = sorted(seen_location_dict.items(), key=lambda l: l[1], reverse=True)
#             sorted_frequent_location = sorted_seen_location[0:k]
#             total_known_time = user_count - total_unknown_location_value
#             sorted_frequent_location_percentage = [(str(kv[0]),(kv[1]/total_known_time)) for kv in sorted_frequent_location]
#             sorted_frequent_location_percentage.append(('others',1-sum([kv[1] for kv in sorted_frequent_location_percentage])))
#             print str(sorted_frequent_location_percentage)
#
#
#         except LeanCloudError, e:
#
#              raise e
#         return sorted_frequent_location_percentage




def get_all_real_applications(db_name='Application'):
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

def create_demo_application(table_name='DemoApplication'):
    all_real_app = get_all_real_applications()
    DbTable = Object.extend(table_name)

    for index, app in enumerate(all_real_app):
        db_table = DbTable()
        relation = app.relation('tracker')
        # query = Query(DbTable)
        query = relation.query()
        user_list = query.find()
        relation = db_table.relation('tracker')
        for user in user_list:
            ADbTable = Object.extend('Tracker')
            query = Query(ADbTable)
            query.equal_to('objectId',user.id)
            result_list = query.find()
            if result_list:
                tracker = result_list[0]
                relation.add(tracker)
            else:
                print 'tracker not exists and objectId is: %s' %(str(user.id))

        db_table.set('app_name',app.get('app_name'))
        db_table.set('origin_name',app.get('name'))
        db_table.save()

def connect_dev_and_real_app(table_name='Developer'):
    all_real_app = get_all_real_applications()
    DbTable = table_name
    query = Query(DbTable)
    query.less_than('createdAt',current_time)
    query.exists('objectId')
    query.equal_to('username','heamon7')
    dev = query.find()[0]

    # total_count=query.count()
    # query_times=(total_count+query_limit-1)/query_limit
    # user_list = []
    # for index in range(query_times):
    #     query = Query(DbTable)
    #     query.exists('objectId')
    #     query.less_than('createdAt',current_time)
    #     query.ascending('createdAt')
    #     query.limit(query_limit)
    #     query.skip(index*query_limit)
    #     user_list.extend(query.find())
    # return user_list

    # all_installation = get_all_installation()
    # print 'already get all installation ,length is %s' %(str(len(all_installation)))
    for app in all_real_app:
        app.set('developer',dev)
        app.save()

if __name__ == '__main__':
    # print 'begin analyze_tracker_static_info'
    # analyze_tracker_static_info()
    # print 'end analyze_tracker_static_info'


    # analyze_tracker_context()
    # connect_tracker_installation()
    # connect_dev_and_real_app()
    create_demo_application()
    # connect_tracker_installation()

