# for innerIndex1 ,key1 in enumerate(notBinaryData):
# ...     for innerIndex2,key2 in enumerate(notBinaryData[key1]):
# ...             labelList.append(key1+'_'+key2)
#


import leancloud
from leancloud import Object
from leancloud import LeanCloudError
from leancloud import Query

APP_NAME_DEST='test@senz.analyzer.dashboard'
APP_ID_DEST='mqip2evxqhxu8c5essmfavnk44fdm85z12gtkkzzvzwfmzqn'
APP_KEY_DEST='awfcehevaljgo4hqn6c8gp6nhagaqtgzlae1gp5x61l91xtv'
query_limit=700
leancloud.init(APP_ID,APP_KEY)
table_name='app_dict'
field_name='app'

APP_DICT = Object.extend(table_name)
query = Query(APP_DICT)
query.exists(field_name)
total_count=query.count()
print 'TotalCount %s' %str(total_count)

query_times=(total_count+query_limit-1)/query_limit

total_label=[]
for index in range(query_times):
    query = Query(APP_DICT)
    query.limit(query_limit)
    query.skip(index*query_limit)
    query.descending('updatedAt')
    query.select('label')
    result_list=query.find()
    for result in result_list:
        total_label.append(result.get('label'))

print len(total_label)
description ='the value of field data is the set of all the labels in table app_dict and the current app_dict count is %s and the list of app_dict is %s' %(str(total_count),total_label)
DB_Tool= Object.extend('DBTool')
db_tool = DB_Tool()
db_tool.set('tableName',table_name)
db_tool.set('fieldName',field_name)
db_tool.set('data',set(total_label))
db_tool.set('description',description)
db_tool.save()































# import leancloud
# from leancloud import Object
# from leancloud import LeanCloudError
# from leancloud import Query
# APP_NAME='senz.analyzer.user.staticinfo.degree'
# APP_ID='pelj09whtpy6ipcob33o4zw4jl6850et2be2f1g331lcn7vr'
# APP_KEY='2t9uzscrnpt2ls23jj0c8vwba3sys0rxyjfu5np5qxi5v7y2'
# query_limit=700
# leancloud.init(APP_ID,APP_KEY)
# table_name='app_dict_not_binary'
# field_name='label'
#
# APP_DICT = Object.extend(table_name)
# query = Query(APP_DICT)
# query.exists(field_name)
# total_count=query.count()
# print 'TotalCount %s' %str(total_count)
#
# query_times=(total_count+query_limit-1)/query_limit
#
# total_label=[]
# for index in range(query_times):
#     query = Query(APP_DICT)
#     query.limit(query_limit)
#     query.skip(index*query_limit)
#     query.descending('updatedAt')
#     query.select(field_name)
#     result_list=query.find()
#     for result in result_list:
#         total_label.append(result.get(field_name))
#
# print len(total_label)
# description ='the value of field data is the set of all the labels in table app_dict_not_binary and the current app_dict_not_binary count is %s and the list of app_dict is %s' %(str(total_count),total_label)
# DB_Tool= Object.extend('DBTool')
# db_tool = DB_Tool()
# db_tool.set('tableName',table_name)
# db_tool.set('fieldName',field_name)
# db_tool.set('data',str(set(total_label)))
# db_tool.set('description',description)
# db_tool.save()



































#
#
#
#
# import leancloud
# from leancloud import Object
# from leancloud import LeanCloudError
# from leancloud import Query
# APP_NAME='senz.analyzer.user.staticinfo.degree'
# APP_ID='pelj09whtpy6ipcob33o4zw4jl6850et2be2f1g331lcn7vr'
# APP_KEY='2t9uzscrnpt2ls23jj0c8vwba3sys0rxyjfu5np5qxi5v7y2'
# query_limit=700
# leancloud.init(APP_ID,APP_KEY)
# table_name='app_dict'
# field_name='app'
#
# APP_DICT = Object.extend(table_name)
# query = Query(APP_DICT)
# query.exists(field_name)
# total_count=query.count()
# print 'TotalCount %s' %str(total_count)
#
# query_times=(total_count+query_limit-1)/query_limit
#
# total_label=[]
# for index in range(query_times):
#     query = Query(APP_DICT)
#     query.limit(query_limit)
#     query.skip(index*query_limit)
#     query.descending('updatedAt')
#     query.select('label')
#     result_list=query.find()
#     for result in result_list:
#         total_label.append(result.get('label'))
#
# print len(total_label)
# description ='the value of field data is the set of all the labels in table app_dict and the current app_dict count is %s and the list of app_dict is %s' %(str(total_count),total_label)
# DB_Tool= Object.extend('DBTool')
# db_tool = DB_Tool()
# db_tool.set('tableName',table_name)
# db_tool.set('fieldName',field_name)
# db_tool.set('data',set(total_label))
# db_tool.set('description',description)
# db_tool.save()