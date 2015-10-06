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

#整个流程是先计算所有tracker的staticInfo，然后根据app和tracker的对应关系，去计算一个app的值
# APP_NAME_DEST='test@senz.dev'
# APP_ID_DEST='6f7ol0oqdq9wbfj2lxr4ywmxrmdzvddojcubkwuxestq73md'
# APP_KEY_DEST='8o34kecu800bcp2floudoporgxz6xby95jqzrig35cjmvcy8'
# leancloud.init(APP_ID_DEST,APP_KEY_DEST)

APP_NAME_DEST='senz.app.dashboard'
APP_ID_DEST='2x27tso41inyau4rkgdqts0mrao1n6rq1wfd6644vdrz2qfo'
APP_KEY_DEST='3fuabth1ar3sott9sgxy4sf8uq31c9x8bykugv3zh7eam5ll'
MASTER_KEY = 'hpt6805u8a98u8nmp74da55dsfvvnqzrsqakdxgfvrse7mma'
leancloud.init(APP_ID_DEST,master_key=MASTER_KEY)

# analyze developer's custom event

query_limit = 1000
current_time = datetime.datetime.now()

APP_STATIC_INFO_TABLE = 'NewAppStaticInfo'
TRACKER_TABLE = 'BindingInstallation'
APPLICATION_TABLE = 'Application'
INSTALLATION_TABLE = 'BindingInstallation'
APPLICATION_FIELD = 'application'
USER_FIELD = 'user'
TRACKER_FIELD = 'tracker'
FLATTEN_STATIC_INFO_TABLE = 'FlattenStaticInfo'
FLATTEN_USER_BEHAVIOR = 'TrackerContext'
WEIGHTED_STATIC_INFO = 'WeightedTrackerInfo'
WEIGHTED_USER_BEHAVIOR = ''



notBinaryData={'age':{"16down":0,"16to35":0.9,"35to55":0.1,"55up":0},'sport':{"jogging":0,"fitness":0,"basketball":0.8,"football":0,"badminton":0,"bicycling":0,"table_tennis":0},'field':{"service":0,"commerce":0,"law":0,"humanities":0,"architecture":0,"medical":0,"manufacture":0,"human_resource":0,"financial":0,"natural":0.6,"agriculture":0,"infotech":0,"athlete":0},'consumption':{"5000down":0.1,"5000to10000":0.5,"10000to20000":0.6,"20000up":0.4},'occupation':{"official":0,"teacher":0,"freelancer":0,"supervisor":0,"salesman":0,"engineer":0.8,"others":0,"soldier":0,"student":0}}
binaryData=[u'ACG',u'indoorsman',u'game_show',u'has_car',u'game_news',u'entertainment_news',u'health',u'online_shopping',u'variety_show',u'business_news',u'tvseries_show',u'current_news',u'sports_news',u'tech_news',u'offline_shopping',u'pregnant',u'gender',u'study',u'married',u'sports_show',u'gamer',u'social',u'has_pet']


not_binary_label_list = [['field__manufacture', 'field__financial', 'field__infotech', 'field__law', 'field__agriculture', 'field__human_resource', 'field__commerce', 'field__natural', 'field__service', 'field__humanities', 'field__medical', 'field__architecture', 'field__athlete'], ['age__16to35', 'age__35to55', 'age__55up', 'age__16down'], ['sport__basketball', 'sport__bicycling', 'sport__table_tennis', 'sport__football', 'sport__jogging', 'sport__badminton', 'sport__fitness'], ['consumption__10000to20000', 'consumption__20000up', 'consumption__5000to10000', 'consumption__5000down'], ['occupation__freelancer', 'occupation__supervisor', 'occupation__student', 'occupation__others', 'occupation__official', 'occupation__salesman', 'occupation__teacher', 'occupation__soldier', 'occupation__engineer']]
binary_label_list = [u'ACG', u'indoorsman', u'game_show', u'has_car', u'game_news', u'entertainment_news', u'health', u'online_shopping', u'variety_show', u'business_news', u'tvseries_show', u'current_news', u'sports_news', u'tech_news', u'offline_shopping', u'pregnant', u'gender', u'study', u'married', u'sports_show', u'gamer', u'social', u'has_pet']
all_label_list = ['field__manufacture', 'field__financial', 'field__infotech', 'field__law', 'field__agriculture', 'field__human_resource', 'field__commerce', 'field__natural', 'field__service', 'field__humanities', 'field__medical', 'field__architecture', 'field__athlete', 'age__16to35', 'age__35to55', 'age__55up', 'age__16down', 'sport__basketball', 'sport__bicycling', 'sport__table_tennis', 'sport__football', 'sport__jogging', 'sport__badminton', 'sport__fitness', 'consumption__10000to20000', 'consumption__20000up', 'consumption__5000to10000', 'consumption__5000down', 'occupation__freelancer', 'occupation__supervisor', 'occupation__student', 'occupation__others', 'occupation__official', 'occupation__salesman', 'occupation__teacher', 'occupation__soldier', 'occupation__engineer',u'ACG', u'indoorsman', u'game_show', u'has_car', u'game_news', u'entertainment_news', u'health', u'online_shopping', u'variety_show', u'business_news', u'tvseries_show', u'current_news', u'sports_news', u'tech_news', u'offline_shopping', u'pregnant', u'gender', u'study', u'married', u'sports_show', u'gamer', u'social', u'has_pet']

# 这个所有feature的列表要经常根据config的改变而进行更改
all_feature_list=['field-manufacture','field-financial','field-infotech','field-law','field-agriculture','field-human_resource','field-commerce','field-natural','field-service','field-humanities','field-medical','field-architecture','field-athlete','age-16to35','age-35to55','age-55up','age-16down','sport-basketball','sport-bicycling','sport-table_tennis','sport-football','sport-jogging','sport-badminton','sport-fitness','consumption-10000to20000','consumption-20000up','consumption-5000to10000','consumption-5000down','occupation-freelancer','occupation-supervisor','occupation-student','occupation-others','occupation-official','occupation-salesman','occupation-teacher','occupation-soldier','occupation-engineer','ACG','indoorsman','game_show','has_car','game_news','entertainment_news','health','online_shopping','variety_show','business_news','tvseries_show','current_news','sports_news','tech_news','offline_shopping','pregnant','gender','study','married','sports_show','gamer','social','has_pet']

# table_name='UserBehavior'
field_name='objectId'
currentTime = datetime.datetime.now()
# 功能：当前StaticInfo的数据不够完整，字段比较少，所以这里利用随机数造数据使用
# 过程：fake_static_info
#      =>[get_all_tracker(得到所有存在的tracker)]
# 输入：<所有的Application>(包括demo)，<所有的field>(其中包含所有tracker的信息)
# 输出：每个开发者应用的自定义event

# 暂时废弃（9.25）
def fake_static_info(table_name='StaticInfo'):
    table_name='StaticInfo'
    DBTable = Object.extend(table_name)
    record_count = 2000
    tracker_list = get_all_tracker()
    tracker_length = len(tracker_list)
    print 'tracker_list is : %s' %str(tracker_list)
    for index ,key in enumerate(range(tracker_list)):
        # print 'saving index: %s' %str(index)
        dbTable = DBTable()
        # dbTable.set('userInfoLogSrc','fakeData')
        # dbTable.set('user','fakeData')
        dbTable.set('description','fake data')
        dbTable.set('timestamp',int(time.time()*1000))
        dbTable.set('tracker',tracker_list[np.random.randint(tracker_length)])
        for innerIndex1 ,key1 in enumerate(notBinaryData):
            for innerIndex2,key2 in enumerate(notBinaryData[key1]):
                dbTable.set(key1+'__'+key2,2*np.random.random_sample()-1) #用两个下划线是为了防止和table_tennis这样的词语冲突
        for innerIndex1,key1 in enumerate(binaryData):
            dbTable.set(key1,2*np.random.random_sample()-1)
        dbTable.save()
    print 'finished all'

def get_all_static_info(table_name='UserInfoLog',field_name='staticInfo'):
    DBTable = Object.extend(table_name)
    query = Query(DBTable)
    query.exists(field_name)
    query.less_than('updatedAt',current_time)
    total_count=query.count()
    print 'TotalCount %s' %str(total_count)
    query_times=(total_count+query_limit-1)/query_limit
    static_info_list=[]
    for index in range(query_times):
        print 'querying index: %s' %str(index)
        query = Query(DBTable)
        query.exists(field_name)
        query.less_than('updatedAt',current_time)
        query.limit(query_limit)
        query.skip(index*query_limit)
        query.descending('updatedAt')
        result_list=query.find()
        static_info_list.extend(result_list)
    return static_info_list

#功能：将UserInfoLog中的staticInfo对象展开，不存在的字段则将值置为0
#过程：flat_static_info_object
#     => get_all_static_info(取出所有的staticInfo对象）
def flat_static_info_object(table_name='UserInfoLog',field_name='staticInfo'):
    static_info_list = get_all_static_info(table_name=table_name,field_name=field_name)
    print 'Length of static_info_list is %s' %(str(len(static_info_list)))
    StaticInfo = Object.extend(FLATTEN_STATIC_INFO_TABLE)
    for index ,static_info_record in enumerate(static_info_list):
        all_feature_list_tmp = all_feature_list[:]
        print 'saving index: %s' %str(index)
        # print staticInfoDict[key]

        staticInfo = StaticInfo()
        staticInfo.set('tracker',static_info_record.get(USER_FIELD))
        staticInfo.set('userInfoLogSrc',static_info_record)
        #dict comprehension
        # print 'the static_info object is : %s with the objectId is %s' %(str(static_info_record.get('staticInfo')),str(static_info_record.id))
        try:
            for field,value in static_info_record.get(field_name).items():
                split_field = field.split('-')
                if len(split_field) == 2:
                    staticInfo.set('__'.join(split_field),value)

                else:
                    staticInfo.set(field,value)
                del all_feature_list_tmp[all_feature_list_tmp.index(field)]
            for feature in all_feature_list_tmp:
                split_feature= feature.split('-')
                if len(split_feature) == 2:
                    staticInfo.set('__'.join(split_feature),0)
                else:
                    staticInfo.set(feature,0)
            for key1,value1 in notBinaryData.items():
                tmp_dict_data ={}
                for key2,value2 in value1.items():
                    full_key = key1+'-'+key2
                    print 'Full key: %s' %(full_key)
                    static_info = static_info_record.get(field_name)
                    if full_key in  static_info.keys():
                        tmp_dict_data[key2] = static_info[full_key]
                    else:
                        tmp_dict_data[key2] = 0
                largest_value = dict([sorted(tmp_dict_data.items(),key=lambda l:l[1],reverse=True)[1]])
                print 'largest_value in field %s is %s'  %(str(key1),largest_value)
                staticInfo.set(key1,largest_value)
                # return sorted(tmp_dict_data.items(),key=lambda l:l[1],reverse=True)


        except LeanCloudError,e:
            print 'the objectId of the static_info_record is %s' %(str(static_info_record.id))
            print e

        staticInfo.save()


def get_all_behavior_prediction(table_name='UserBehavior',field_name='prediction'):
    DBTable = Object.extend(table_name)
    query = Query(DBTable)
    query.exists(field_name)
    query.less_than('updatedAt',currentTime)
    total_count=query.count()
    print 'TotalCount  %s' %str(total_count)

    query_times=(total_count+query_limit-1)/query_limit

    behavior_prediction_dict={}
    for index in range(query_times):
        print 'querying index: %s' %str(index)
        query = Query(DBTable)
        query.exists(field_name)
        query.less_than('updatedAt',currentTime)
        query.limit(query_limit)
        query.skip(index*query_limit)
        query.descending('updatedAt')
        result_list=query.find()
        for result in result_list:
            record_wrap ={}
            record_wrap['startTime'] = result.get('startTime')
            record_wrap['endTime'] = result.get('endTime')
            record_wrap['prediction'] = result.get(field_name)
            # record_wrap['UserBehavior'] = result   # 这里是为了绑定userBehavior
            behavior_prediction_dict[result]=record_wrap  #这里的键user由于是对象，所以即使是同一个user，他们也是不同的
    return behavior_prediction_dict
#功能：取UserBehavior中的prediction列表中prediction的prob最大的prediction，然后取senzList的第一个senz，sound，location，motion字段，并取prediction中概率最大的event
#过程：flat_tracker_behavior_prediction
#     => get_all_behavior_prediction(取出UserBehavior中所有的record）
def flat_tracker_behavior_prediction(table_name='UserBehavior',field_name='prediction',table_name_dest=FLATTEN_USER_BEHAVIOR):
    behavior_prediction_dict = get_all_behavior_prediction(table_name='UserBehavior',field_name='prediction')
    table_name_dest = table_name_dest
    DBTable = Object.extend(table_name_dest)
    for key,item in behavior_prediction_dict.items():
        # print 'saving index: %s' %str(index)
        dbTable = DBTable()
        if item['prediction']:
            description = 'this table comes from UserBehavior,and choose the most probably prediction in the prediction list then  get and store the first senz in senzList in the prediction and store the most probably event in the prediction'
            most_likely_prediction = sorted(item['prediction'],key = lambda behavior: behavior['behavior']['prob'],reverse=True)[0]
            dbTable.set('sound',most_likely_prediction['behavior']['senzList'][0]['sound'])
            dbTable.set('motion',most_likely_prediction['behavior']['senzList'][0]['motion'])
            dbTable.set('location',most_likely_prediction['behavior']['senzList'][0]['location'])
            dbTable.set('prob',most_likely_prediction['behavior']['prob'])
            dbTable.set('event',sorted(most_likely_prediction['prediction'].items(),key=operator.itemgetter(1),reverse=True)[0])
            dbTable.set('startTime',item['startTime'])
            dbTable.set('endTime',item['endTime'])
            dbTable.set('tracker',key.get('user'))
            dbTable.set('user_behavior',key)
            dbTable.set('description','')

            #这个地方经常报错，数据量有点大耶
            try:
                dbTable.save()
            except (LeanCloudError ,TypeError) as e:
                print e
                try:
                    dbTable.save()
                except (LeanCloudError ,TypeError) as e:
                    print e



            # print 'saving successfully  index: %s' %str(index)

    print 'finished all'


def get_all_tracker(table_name=INSTALLATION_TABLE):
    DbTable = table_name
    query = Query(DbTable)
    query.less_than('createdAt',current_time)
    query.exists('objectId')
    total_count=query.count()
    query_times=(total_count+query_limit-1)/query_limit
    all_installation_list = []
    for index in range(query_times):
        query = Query(DbTable)
        query.exists('objectId')
        query.less_than('createdAt',current_time)
        query.select(USER_FIELD)
        query.ascending('createdAt')
        query.limit(query_limit)
        query.skip(index*query_limit)
        all_installation_list.extend(query.find())

    user_id_set = set()
    user_list = []
    for installation in all_installation_list:
        user = installation.get(USER_FIELD)
        user_id = user.id
        if user_id not in user_id_set:
            user_id_set.add(user_id)
            user_list.append(user)


        else:
            pass
    print "Have gotten all the trackers,total number is: %s" %(str(len(user_list)))
    return user_list

def get_tracker_data(table_name=None,tracker_list=None,field_name=None):

    DBTable = Object.extend(table_name)
    tracker_data_dict = {}
    for index,tracker in enumerate(tracker_list):
        #这样处理是因为可能一个user的记录超过了一次可以读取的数量（1K条）
        print 'Getting tracker data index: %s'  %(str(index))
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
            if tracker in tracker_data_dict.keys():
                tracker_data_dict.get(tracker).extend(query.find())
            else :
                tracker_data_dict[tracker]=query.find()
    print "Have gotton all the tracker data"
    return tracker_data_dict
# 功能：所有展开过的staticInfo中，每个tracker可能有多个staticInfo，这里按照时间的先后顺序，将一个tracker的所有staticInfo加权合并成一个staticInfo，并且将不是binary的feature合并，比如age
# 过程：weight_tracker_static_info
#      => [get_all_tracker
#           ]
def weight_tracker_static_info(table_name=WEIGHTED_STATIC_INFO):
    all_tracker_list = get_all_tracker(table_name=INSTALLATION_TABLE)
     # tracker_data_dict的键是tracker，值是一个tracker的所有的staticInfo记录
    tracker_data_dict = get_tracker_data(table_name=FLATTEN_STATIC_INFO_TABLE,tracker_list=all_tracker_list,field_name=TRACKER_FIELD)
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
            # field_value_matrix[index1,index2+1] = record.get('timestamp')
            field_value_matrix[index1,len(all_label_list)] = record.get('timestamp')

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
        print 'WeightedValueDict: %s' %(str(weightedValueDict))
        for field ,value in weightedValueDict.items():

            dbTable.set(field,value)
        dbTable.save()


# 功能：所有展开过的staticInfo中，每个tracker可能有多个staticInfo，这里按照时间的先后顺序，将一个tracker的所有staticInfo加权合并成一个staticInfo，并且将不是binary的feature合并，比如age
# 过程：weight_tracker_static_info
#      => [get_all_tracker
#          ,get_tracker_data（传入上一步得到的tracker_list，得到一个tracker和其所有tracker_data的字典的列表)
#           ]
def weight_tracker_user_context(table_name='RealWeightedTrackerInfo'):
    all_tracker_list = get_all_tracker(table_name=INSTALLATION_TABLE)
     # tracker_data_dict的键是tracker，值是一个tracker的所有的staticInfo记录
    tracker_data_dict = get_tracker_data(table_name=FLATTEN_STATIC_INFO_TABLE,tracker_list=all_tracker_list,field_name=TRACKER_FIELD)
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
        dbTable.save()

def get_all_applications(db_name=APPLICATION_TABLE):
    '''
    功能：查询并返回所有的应用，这里包括demo应用，返回形式为list
    '''
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

def get_all_trackers(db_name=INSTALLATION_TABLE):
    '''
    这里返回每个应用和Installation表中相应user列表的字典
    '''
    # all_application_list = get_all_demo_applications()
    # all_application_list.extend(get_all_applications())
    # 现在 Demo应用和新建的应用在一个表里面
    print 'begin get_all_applications'
    all_application_list = get_all_applications()

    print 'length of application_list is %s' %(str(len(all_application_list)))
    print 'end get_all_applications'
    application_tracker_dict = {}
    for app in all_application_list:
        Installation = Object.extend(db_name)
        query = Query(Installation)
        query.equal_to(APPLICATION_FIELD,app)
        query.less_than('createdAt',current_time)
        total_count=query.count()
        query_times=(total_count+query_limit-1)/query_limit
        installation_list = []
        for index in range(query_times):
            query = Query(Installation)
            query.equal_to(APPLICATION_FIELD,app)
            query.select(USER_FIELD)
            query.less_than('createdAt',current_time)
            query.ascending('createdAt')
            query.limit(query_limit)
            query.skip(index*query_limit)
            installation_list.extend(query.find())
        # relation = app.relation('tracker')
        # query = relation.query()
        # result_list = query.find()
        #如果这个application没有tracker，则直接忽略掉
        if installation_list:
            application_tracker_dict[app] = [installation.get(USER_FIELD) for installation in installation_list]
        print 'this application-tracker_list length is %s' %(str(len(installation_list)))
    print 'the length of application_tracker_dict is %s' %(str(len(application_tracker_dict.keys())))

    return application_tracker_dict

def get_age_and_gender_data_dict(table_name=WEIGHTED_STATIC_INFO,tracker_list=None):
    try:

        field_name = TRACKER_FIELD
        DBTable  = Object.extend(table_name)
        gender_type_list =['man','woman']
        age_type_list = ['16down','16to35','35to55','55up']
        dataDict ={gender_type:{age_type:0 for age_type in age_type_list} for gender_type in gender_type_list}
        # new_data_dict = {key:[0 for i in range(4)] for key in dataDict.keys()}
        total_count = len(tracker_list)
        i=0
        for index, tracker in enumerate(tracker_list):
            query = Query(DBTable)
            query.equal_to(field_name,tracker)
            query.select('age','gender')
            result_list = query.find()
            length = len(result_list)
            if length!=1:
                pass
                # print 'error: the length of result_list is %s with index: %s with tracker_objectId: %s' %(str(length),str(index),tracker.id)
            if length >=1:
                i=i+1
                print   
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
# 功能：分析所有应用的 age和gender 的统计信息
# 过程：analyze_tracker_static_info
#      => [get_all_trackers[get_all_demo_applications
#          ,get_all_applications](得到每个应用的所有tracker)
#          ,[get_age_and_gender_data_dict(统计一个应用所有的tracker的age和gender信息，age和gender信息已经保存在了一个对象中)]
#           ]
def analyze_tracker_static_info(table_name=APP_STATIC_INFO_TABLE):
    DBTable = Object.extend(table_name)

    print 'begin get_all_trackers'
    application_tracker_dict = get_all_trackers(db_name=INSTALLATION_TABLE)
    print 'end get_all_trackers'
    for app,tracker_list in application_tracker_dict.items():
        dbTable = DBTable()
        print 'begin get_age_and_gender_data_dict'
        age_and_gender_dict = get_age_and_gender_data_dict(table_name=WEIGHTED_STATIC_INFO,tracker_list=tracker_list)
        occupation_dict = get_occupation_data_dict(table_name=WEIGHTED_STATIC_INFO,tracker_list=tracker_list)
        sport_dict = get_sport_data_dict(table_name=WEIGHTED_STATIC_INFO,tracker_list=tracker_list)
        consumption_dict = get_consumption_data_dict(table_name=WEIGHTED_STATIC_INFO,tracker_list=tracker_list)
        field_dict = get_field_data_dict(table_name=WEIGHTED_STATIC_INFO,tracker_list=tracker_list)


        print 'end get_age_and_gender_data_dict'
        dbTable.set('age_and_gender',age_and_gender_dict)
        dbTable.set('occupation',occupation_dict)
        dbTable.set('sport',sport_dict)
        dbTable.set('consumption',consumption_dict)
        dbTable.set('field',field_dict)
        dbTable.set('app',app)
        relation = dbTable.relation('tracker')
        for tracker in tracker_list:
            relation.add(tracker)
        dbTable.save()


def get_tracker_context(table_name='RealWeightedUserContext',tracker_list=None):
    field = 'location'
    DBTable  = Object.extend(table_name)
    k = 5
    unknown = 'unknown'
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
        # print str(seen_location_dict)
        tracker_context_exists_count = len(tracker_location_list)
        location_percentage_dict = {key:value/(tracker_context_exists_count) for key,value in seen_location_dict.items()}

    except LeanCloudError, e:
         raise e
    return location_percentage_dict,tracker_context_exists_count
# 功能：分析所有应用的 所有trackers 经常在的location的时间比例
# 过程：analyze_tracker_context
#      => [get_all_trackers
#          =>[get_all_demo_applications
#             ,get_all_applications](得到每个应用的所有tracker)
#         [get_tracker_context](遍历应用，计算每个应用所有的tracker的location分布，其中每个用户的location字段是一个各个location feature和时间的字典)
def analyze_tracker_context(table_name='WeightedUserContext'):
    DbTable = Object.extend('AppStaticInfo')

    print 'begin analyze_tracker_context'
    application_tracker_dict = get_all_trackers(db_name='Tracker')
    print 'end get_all_trackers'
    for app,tracker_list in application_tracker_dict.items():
        dbTable = DbTable()
        print 'begin get_tracker_context'
        location_percentage_dict,known_count = get_tracker_context(table_name='WeightedUserContext',tracker_list=tracker_list)
        print location_percentage_dict
        print 'end get_tracker_context'
        dbTable.set('location_percentage',location_percentage_dict)
        dbTable.set('known_count',known_count)
        dbTable.set('app',app)
        relation = dbTable.relation('tracker')
        for tracker in tracker_list:
            relation.add(tracker)
        dbTable.save()


# need document
def get_occupation_data_dict(table_name='WeightedStaticInfo',tracker_list=None):
    try:
        field_name = 'user'
        DBTable  = Object.extend(table_name)

        data_dict ={}
        # new_data_dict = {key:[0 for i in range(4)] for key in dataDict.keys()}
        total_count = len(tracker_list)
        for index, tracker in enumerate(tracker_list):
            query = Query(DBTable)
            query.equal_to(field_name,tracker)
            query.select('occupation')
            result_list = query.find()
            length = len(result_list)
            # 以后这个地方的判断还需要修改
            if length!=1:
                print 'error: the length of result_list is %s with index: %s with tracker_objectId: %s' %(str(length),str(index),tracker.id)
            if length >=1:
                result = result_list[0]
            else:
                continue
            print 'index: %s  occupation: %s  ' %(str(index),str(result.get('occupation')))
            occupation= result.get('occupation').keys()[0]
            if occupation in data_dict.keys():
                data_dict[occupation] += 1
            else:
                data_dict[occupation] = 1
        known_count = sum(data_dict.values())
        data_dict['unknown'] = total_count - known_count
    except LeanCloudError, e:
         raise e
    return data_dict

# need document
def get_sport_data_dict(table_name='WeightedStaticInfo',tracker_list=None):
    try:
        field_name = 'sport'
        tracker_field_name = 'user'
        DBTable  = Object.extend(table_name)

        data_dict ={}
        # new_data_dict = {key:[0 for i in range(4)] for key in dataDict.keys()}
        total_count = len(tracker_list)
        for index, tracker in enumerate(tracker_list):
            query = Query(DBTable)
            query.equal_to(tracker_field_name,tracker)
            query.select(field_name)
            result_list = query.find()
            length = len(result_list)
            # 以后这个地方的判断还需要修改
            if length!=1:
                print 'error: the length of result_list is %s with index: %s with tracker_objectId: %s' %(str(length),str(index),tracker.id)
            if length >=1:
                result = result_list[0]
            else:
                continue
            print 'index: %s  field_name: %s: result: %s  ' %(str(index),str(field_name),str(result.get(field_name)))
            field_key= result.get(field_name).keys()[0]
            if field_key in data_dict.keys():
                data_dict[field_key] += 1
            else:
                data_dict[field_key] = 1
        known_count = sum(data_dict.values())
        data_dict['unknown'] = total_count - known_count
    except LeanCloudError, e:
         raise e
    return data_dict

# need document
def get_consumption_data_dict(table_name='WeightedStaticInfo',tracker_list=None):
    try:
        field_name = 'consumption'
        tracker_field_name = 'user'
        DBTable  = Object.extend(table_name)

        data_dict ={}
        # new_data_dict = {key:[0 for i in range(4)] for key in dataDict.keys()}
        total_count = len(tracker_list)
        for index, tracker in enumerate(tracker_list):
            query = Query(DBTable)
            query.equal_to(tracker_field_name,tracker)
            query.select(field_name)
            result_list = query.find()
            length = len(result_list)
            # 以后这个地方的判断还需要修改
            if length!=1:
                print 'error: the length of result_list is %s with index: %s with tracker_objectId: %s' %(str(length),str(index),tracker.id)
            if length >=1:
                result = result_list[0]
            else:
                continue
            print 'index: %s  field_name: %s: result: %s  ' %(str(index),str(field_name),str(result.get(field_name)))
            field_key= result.get(field_name).keys()[0]
            if field_key in data_dict.keys():
                data_dict[field_key] += 1
            else:
                data_dict[field_key] = 1
        known_count = sum(data_dict.values())
        data_dict['unknown'] = total_count - known_count
    except LeanCloudError, e:
         raise e
    return data_dict

# need document
def get_field_data_dict(table_name='WeightedStaticInfo',tracker_list=None):
    try:
        field_name = 'field'
        tracker_field_name = 'user'
        DBTable  = Object.extend(table_name)

        data_dict ={}
        # new_data_dict = {key:[0 for i in range(4)] for key in dataDict.keys()}
        total_count = len(tracker_list)
        for index, tracker in enumerate(tracker_list):
            query = Query(DBTable)
            query.equal_to(tracker_field_name,tracker)
            query.select(field_name)
            result_list = query.find()
            length = len(result_list)
            # 以后这个地方的判断还需要修改
            if length!=1:
                print 'error: the length of result_list is %s with index: %s with tracker_objectId: %s' %(str(length),str(index),tracker.id)
            if length >=1:
                result = result_list[0]
            else:
                continue
            print 'index: %s  field_name: %s: result: %s  ' %(str(index),str(field_name),str(result.get(field_name)))
            field_key= result.get(field_name).keys()[0]
            if field_key in data_dict.keys():
                data_dict[field_key] += 1
            else:
                data_dict[field_key] = 1
        known_count = sum(data_dict.values())
        data_dict['unknown'] = total_count - known_count
    except LeanCloudError, e:
         raise e
    return data_dict


if __name__ == '__main__':
#功能：将UserInfoLog中的staticInfo对象展开，不存在的字段则将值置为0
#过程：flat_static_info_object
#     =>get_all_static_info(取出所有的staticInfo对象）
#     flat_static_info_object()

#功能：取UserBehavior中的prediction列表中prediction的prob最大的prediction，然后取senzList的第一个senz，sound，location，motion字段，并取prediction中概率最大的event
#过程：flat_tracker_behavior_prediction
#     =>get_all_behavior_prediction(取出UserBehavior中所有的record）
#     flat_tracker_behavior_prediction()

# 功能：所有展开过的staticInfo中，每个tracker可能有多个staticInfo，这里按照时间的先后顺序，将一个tracker的所有staticInfo加权合并成一个staticInfo，并且将不是binary的feature合并，比如age
# 过程：weight_tracker_static_info
#      =>[get_all_tracker
#           ]
#     weight_tracker_static_info()

# 功能：所有展开过的staticInfo中，每个tracker可能有多个staticInfo，这里按照时间的先后顺序，将一个tracker的所有staticInfo加权合并成一个staticInfo，并且将不是binary的feature合并，比如age
# 过程：weight_tracker_static_info
#      =>[get_all_tracker
#          ,get_tracker_data（传入上一步得到的tracker_list，得到一个tracker和其所有tracker_data的字典的列表)
#           ]
#     weight_tracker_user_context()

# 功能：分析所有应用的 所有trackers 经常在的location的时间比例
# 过程：analyze_tracker_context
#      =>[get_all_trackers
#          =>[get_all_demo_applications
#             ,get_all_applications](得到每个应用的所有tracker)
#         [get_tracker_context](遍历应用，计算每个应用所有的tracker的location分布，其中每个用户的location字段是一个各个location feature和时间的字典)
    analyze_tracker_static_info()

# 功能：分析所有应用的 所有trackers 经常在的location的时间比例
# 过程：analyze_tracker_context
#      =>[get_all_trackers
#          =>[get_all_demo_applications
#             ,get_all_applications](得到每个应用的所有tracker)
#         [get_tracker_context](遍历应用，计算每个应用所有的tracker的location分布，其中每个用户的location字段是一个各个location feature和时间的字典)
#     analyze_tracker_context()









# def get_all_demo_applications(db_name='DemoApplication'):
#     DbTable = db_name
#     query = Query(DbTable)
#     query.less_than('createdAt',current_time)
#     query.exists('objectId')
#     total_count=query.count()
#     query_times=(total_count+query_limit-1)/query_limit
#     result_list = []
#     for index in range(query_times):
#         query = Query(DbTable)
#         query.exists('objectId')
#         query.less_than('createdAt',current_time)
#         query.ascending('createdAt')
#         query.limit(query_limit)
#         query.skip(index*query_limit)
#         result_list.extend(query.find())
#     return result_list

