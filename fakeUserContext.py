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

contextData = {
    'motion':{"sitting":{"isActive":True},"walking":{"isActive":True},"running":{"isActive":True},"riding":{"isActive":True},"driving":{"isActive":True},"unknown":{"isActive":True}}
    ,
    #soundlv1
    'sound':{"study_quiet_office":{"isActive":True},"hallway":{"isActive":True},"walk":{"isActive":True},"quiet_street":{"isActive":True},"kitchen":{"isActive":True},"unknown":{"isActive":True},"forrest":{"isActive":True},"shop":{"isActive":True},"busy_street":{"isActive":True},"living_room":{"isActive":True},"flat":{"isActive":True},"bus_stop":{"isActive":True},"in_bus":{"isActive":True},"subway":{"isActive":True},"bedroom":{"isActive":True},"supermarket":{"isActive":True},"classroom":{"isActive":True},"train_station":{"isActive":True}}
    ,
    #locationlv2
    'location':{"hospital":{"isActive":True},"bath_sauna":{"isActive":True},"drugstore":{"isActive":True},"vegetarian_diet":{"isActive":True},"talent_market":{"isActive":True},"business_building":{"isActive":True},"comprehensive_market":{"isActive":True},"motel":{"isActive":True},"stationer":{"isActive":True},"high_school":{"isActive":True},"insurance_company":{"isActive":True},"home":{"isActive":True},"resort":{"isActive":True},"digital_store":{"isActive":True},"cigarette_store":{"isActive":True},"pawnshop":{"isActive":True},"auto_sale":{"isActive":True},"japan_korea_restaurant":{"isActive":True},"toll_station":{"isActive":True},"salvage_station":{"isActive":True},"newstand":{"isActive":True},"western_restaurant":{"isActive":True},"car_maintenance":{"isActive":True},"scenic_spot":{"isActive":True},"barbershop":{"isActive":True},"chafing_dish":{"isActive":True},"buffet":{"isActive":True},"convenience_store":{"isActive":True},"odeum":{"isActive":True},"pet_service":{"isActive":True},"traffic":{"isActive":True},"unknown":{"isActive":True},"cinema":{"isActive":True},"coffee":{"isActive":True},"auto_repair":{"isActive":True},"bar":{"isActive":True},"hostel":{"isActive":True},"video_store":{"isActive":True},"game_room":{"isActive":True},"laundry":{"isActive":True},"photographic_studio":{"isActive":True},"ktv":{"isActive":True},"exhibition_hall":{"isActive":True},"bank":{"isActive":True},"night_club":{"isActive":True},"bike_store":{"isActive":True},"furniture_store":{"isActive":True},"travel_agency":{"isActive":True},"technical_school":{"isActive":True},"welfare_house":{"isActive":True},"intermediary":{"isActive":True},"security_company":{"isActive":True},"gift_store":{"isActive":True},"muslim":{"isActive":True},"lottery_station":{"isActive":True},"photography_store":{"isActive":True},"science_museum":{"isActive":True},"sports_store":{"isActive":True},"gas_station":{"isActive":True},"university":{"isActive":True},"primary_school":{"isActive":True},"outdoor":{"isActive":True},"motorcycle":{"isActive":True},"electricity_office":{"isActive":True},"library":{"isActive":True},"convention_center":{"isActive":True},"kinder_garten":{"isActive":True},"ticket_agent":{"isActive":True},"snack_bar":{"isActive":True},"hotel":{"isActive":True},"cosmetics_store":{"isActive":True},"adult_education":{"isActive":True},"telecom_offices":{"isActive":True},"pet_market":{"isActive":True},"housekeeping":{"isActive":True},"antique_store":{"isActive":True},"work_office":{"isActive":True},"seafood":{"isActive":True},"gallery":{"isActive":True},"bbq":{"isActive":True},"water_supply_office":{"isActive":True},"other_infrastructure":{"isActive":True},"residence":{"isActive":True},"clinic":{"isActive":True},"internet_bar":{"isActive":True},"commodity_market":{"isActive":True},"guest_house":{"isActive":True},"clothing_store":{"isActive":True},"farmers_market":{"isActive":True},"others":{"isActive":True},"flea_market":{"isActive":True},"jewelry_store":{"isActive":True},"training_institutions":{"isActive":True},"post_office":{"isActive":True},"mother_store":{"isActive":True},"supermarket":{"isActive":True},"economy_hotel":{"isActive":True},"glass_store":{"isActive":True},"public_utilities":{"isActive":True},"dessert":{"isActive":True},"cooler":{"isActive":True},"emergency_center":{"isActive":True},"car_wash":{"isActive":True},"parking_plot":{"isActive":True},"chinese_restaurant":{"isActive":True},"atm":{"isActive":True},"museum":{"isActive":True}}
    ,
    'event':{"go_work":{"motion":[{"running":0.1},{"riding":0.1},{"walking":0.3},{"sitting":0.2},{"driving":0.3}],"sound":[{"walk":0.1},{"bus_stop":0.1},{"quiet_street":0.1},{"subway":0.2},{"in_bus":0.2},{"busy_street":0.3}],"location":[{"traffic":0.26},{"Others":0.1},{"residence":0.1},{"home":0.05},{"cigarette_store":0.01},{"newstand":0.05},{"coffee":0.05},{"gas_station":0.05},{"university":0.01},{"primary_school":0.01},{"motorcycle":0.05},{"outdoor":0.1},{"post_office":0.01},{"convenience_store":0.04},{"parking_plot":0.1},{"toll_station":0.01}]},"go_to_class":{"motion":[{"walking":0.1},{"sitting":0.9}],"sound":[{"walk":0.1},{"unknown":0.1},{"classroom":0.8}],"location":[{"high_school":0.1},{"university":0.23},{"primary_school":0.05},{"adult_education":0.1},{"technical_school":0.3},{"science_museum":0.05},{"library":0.05},{"kinder_garten":0.01},{"emergency_center":0.01},{"museum":0.05},{"training_institutions":0.05}]},"go_for_concert":{"motion":[{"walking":0.1},{"sitting":0.9}],"sound":[{"Others":1}],"location":[{"gallery":0.2},{"exhibition_hall":0.2},{"odeum":0.2},{"motel":0.05},{"scenic_spot":0.05},{"bar":0.05},{"night_club":0.05},{"university":0.1},{"outdoor":0.05},{"ticket_agent":0.05}]},"travel_in_scenic":{"motion":[{"running":0.1},{"walking":0.3},{"riding":0.1},{"sitting":0.2},{"driving":0.3}],"sound":[{"walk":0.1},{"quiet_street":0.1},{"busy_street":0.2},{"shop":0.2},{"forrest":0.3},{"unknown":0.1}],"location":[{"traffic":0.2},{"Others":0.1},{"residence":0.1},{"scenic_spot":0.2},{"museum":0.1},{"atm":0.01},{"parking_plot":0.02},{"emergency_center":0.01},{"cooler":0.01},{"dessert":0.01},{"economy_hotel":0.01},{"jewelry_store":0.01},{"gallery":0.01},{"snack_bar":0.01},{"outdoor":0.05},{"gas_station":0.01},{"muslim":0.01},{"travel_agency":0.01},{"bank":0.01},{"bar":0.01},{"coffee":0.01},{"hostel":0.05},{"toll_station":0.01},{"cigarette_store":0.01},{"motel":0.01},{"drugstore":0.01}]},"go_home":{"motion":[{"running":0.1},{"walking":0.3},{"sitting":0.2},{"driving":0.3},{"riding":0.1}],"sound":[{"walk":0.1},{"quiet_street":0.1},{"subway":0.2},{"in_bus":0.2},{"busy_street":0.3},{"bus_stop":0.1}],"location":[{"traffic":0.26},{"Others":0.1},{"residence":0.1},{"home":0.05},{"cigarette_store":0.01},{"newstand":0.05},{"coffee":0.05},{"gas_station":0.05},{"university":0.01},{"primary_school":0.01},{"motorcycle":0.05},{"outdoor":0.1},{"post_office":0.01},{"convenience_store":0.04},{"parking_plot":0.1},{"toll_station":0.01}]},"dining_in_restaurant":{"motion":[{"walking":0.2},{"sitting":0.8}],"sound":[{"walk":0.3},{"kitchen":0.4},{"living_room":0.1},{"shop":0.2}],"location":[{"vegetarian_diet":0.05},{"western_restaurant":0.2},{"chafing_dish":0.05},{"buffet":0.1},{"muslim":0.05},{"seafood":0.05},{"bbq":0.1},{"chinese_restaurant":0.2},{"japan_korea_restaurant":0.1},{"coffee":0.04},{"dessert":0.03},{"scenic_spot":0.01},{"bar":0.01},{"hotel":0.01},{"hostel":0.01}]},"movie_in_cinema":{"motion":[{"walking":0.1},{"sitting":0.9}],"sound":[{"Others":1}],"location":[{"cinema":0.9},{"Others":0.1}]},"emergency":{"motion":[{"running":0.5},{"walking":0.2},{"sitting":0.1},{"driving":0.2}],"sound":[{"subway":0.2},{"in_bus":0.3},{"busy_street":0.5}],"location":[{"traffic":0.3},{"emergency_center":0.2},{"residence":0.1},{"hospital":0.2},{"drugstore":0.1},{"salvage_station":0.1}]},"go_for_outing":{"motion":[{"running":0.1},{"walking":0.2},{"sitting":0.2},{"driving":0.4},{"riding":0.1}],"sound":[{"walk":0.1},{"quiet_street":0.1},{"busy_street":0.2},{"shop":0.1},{"forrest":0.4},{"bus_stop":0.1}],"location":[{"traffic":0.2},{"Others":0.1},{"residence":0.1},{"scenic_spot":0.2},{"museum":0.05},{"atm":0.01},{"parking_plot":0.02},{"emergency_center":0.01},{"cooler":0.01},{"dessert":0.01},{"economy_hotel":0.01},{"jewelry_store":0.01},{"gallery":0.01},{"snack_bar":0.01},{"outdoor":0.05},{"gas_station":0.01},{"muslim":0.01},{"travel_agency":0.01},{"bank":0.01},{"bar":0.01},{"coffee":0.01},{"hostel":0.05},{"toll_station":0.01},{"cigarette_store":0.01},{"motel":0.01},{"drugstore":0.01},{"resort":0.05}]},"work_in_office":{"motion":[{"walking":0.1},{"sitting":0.9}],"sound":[{"walk":0.1},{"living_room":0.2},{"study_quiet_office":0.7}],"location":[{"business_building":0.2},{"university":0.1},{"work_office":0.3},{"museum":0.01},{"post_office":0.01},{"training_institutions":0.01},{"water_supply_office":0.01},{"telecom_offices":0.05},{"adult_education":0.05},{"hotel":0.01},{"ticket_agent":0.01},{"library":0.01},{"electricity_office":0.05},{"primary_school":0.01},{"science_museum":0.04},{"technical_school":0.03},{"travel_agency":0.01},{"bank":0.04},{"high_school":0.03},{"insurance_company":0.02}]},"exercise_outdoor":{"motion":[{"running":0.4},{"walking":0.3},{"sitting":0.1},{"riding":0.2}],"sound":[{"walk":0.3},{"quiet_street":0.2},{"busy_street":0.5}],"location":[{"traffic":0.3},{"outdoor":0.58},{"university":0.1},{"cooler":0.01},{"dessert":0.01}]},"go_for_exhibition":{"motion":[{"walking":0.7},{"sitting":0.3}],"sound":[{"walk":0.3},{"hallway":0.3},{"living_room":0.3},{"study_quiet_office":0.1}],"location":[{"museum":0.3},{"gallery":0.2},{"muslim":0.1},{"exhibition_hall":0.3},{"university":0.1}]},"shopping_in_mall":{"motion":[{"walking":0.8},{"sitting":0.2}],"sound":[{"walk":0.4},{"quiet_street":0.2},{"busy_street":0.2},{"shop":0.2}],"location":[{"clothing_store":0.14},{"sports_store":0.1},{"comprehensive_market":0.1},{"digital_store":0.1},{"cigarette_store":0.05},{"video_store":0.05},{"dessert":0.05},{"pawnshop":0.01},{"coffee":0.05},{"ktv":0.01},{"furniture_store":0.02},{"gift_store":0.05},{"photography_store":0.03},{"cosmetics_store":0.05},{"pet_market":0.05},{"antique_store":0.01},{"commodity_market":0.01},{"jewelry_store":0.1},{"mother_store":0.02},{"supermarket":0.05},{"cooler":0.02},{"parking_plot":0.02},{"atm":0.01}]},"exercise_indoor":{"motion":[{"running":0.1},{"walking":0.2},{"sitting":0.7}],"sound":[{"walk":0.2},{"living_room":0.8}],"location":[{"home":0.5},{"residence":0.5}]}}

}
#motion list length is 6
motion_list = ['walking', 'driving', 'sitting', 'unknown', 'running', 'riding']

#soundlv1 list length is 18

soundlv1_list = ['shop', 'hallway', 'busy_street', 'quiet_street', 'flat', 'unknown', 'train_station', 'bedroom', 'living_room', 'supermarket', 'walk', 'bus_stop', 'study_quiet_office', 'classroom', 'subway', 'in_bus', 'forrest', 'kitchen']

#locationlv2 list length is 107

locationlv2 = ['economy_hotel', 'outdoor', 'bath_sauna', 'technical_school', 'bike_store', 'pet_service', 'clinic', 'motorcycle', 'guest_house', 'ticket_agent', 'chinese_restaurant', 'flea_market', 'resort', 'pet_market', 'digital_store', 'coffee', 'dessert', 'cosmetics_store', 'traffic', 'work_office', 'bank', 'adult_education', 'bar', 'talent_market', 'university', 'cooler', 'convenience_store', 'snack_bar', 'home', 'post_office', 'hostel', 'motel', 'welfare_house', 'farmers_market', 'vegetarian_diet', 'high_school', 'sports_store', 'gas_station', 'training_institutions', 'muslim', 'supermarket', 'insurance_company', 'others', 'auto_sale', 'video_store', 'commodity_market', 'chafing_dish', 'housekeeping', 'residence', 'convention_center', 'atm', 'lottery_station', 'business_building', 'internet_bar', 'mother_store', 'museum', 'night_club', 'antique_store', 'japan_korea_restaurant', 'other_infrastructure', 'car_maintenance', 'odeum', 'unknown', 'hospital', 'primary_school', 'photographic_studio', 'drugstore', 'glass_store', 'bbq', 'auto_repair', 'toll_station', 'hotel', 'newstand', 'stationer', 'public_utilities', 'library', 'security_company', 'comprehensive_market', 'salvage_station', 'ktv', 'exhibition_hall', 'barbershop', 'clothing_store', 'water_supply_office', 'telecom_offices', 'furniture_store', 'gift_store', 'cinema', 'car_wash', 'travel_agency', 'photography_store', 'electricity_office', 'pawnshop', 'game_room', 'kinder_garten', 'emergency_center', 'intermediary', 'jewelry_store', 'parking_plot', 'laundry', 'scenic_spot', 'buffet', 'gallery', 'western_restaurant', 'science_museum', 'seafood', 'cigarette_store']
# length is 14
event_list = ['travel_in_scenic', 'emergency', 'work_in_office', 'go_for_concert', 'dining_in_restaurant', 'exercise_indoor', 'go_for_outing', 'exercise_outdoor', 'go_to_class', 'go_home', 'shopping_in_mall', 'movie_in_cinema', 'go_for_exhibition', 'go_work']

contextDict={}
for index ,key in enumerate(contextData):
    contextDict[key]= contextData[key].keys()
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

DBTable = Object.extend(table_name_dest)
probList =[]
recordCount = 10
userLen= len(userList)
print 'userLength is : %s' %str(userLen)
for index ,key in enumerate(range(recordCount)):
    # print 'saving index: %s' %str(index)
    dbTable = DBTable()
    # dbTable.set('userInfoLogSrc','fakeData')
    # dbTable.set('user','fakeData')
    start = time.time()*0.9+ index*1800
    dbTable.set('description','fake data')
    dbTable.set('user',userList[np.random.randint(userLen)])
    dbTable.set('prob',40*np.random.random_sample()-20)
    for innerIndex ,key in enumerate(contextDict):
        if key=='event':
            dbTable.set(key,[contextDict[key][np.random.randint(len(contextDict[key]))],100*np.random.random_sample()-50])
        else:
            dbTable.set(key,contextDict[key][np.random.randint(len(contextDict[key]))])

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
