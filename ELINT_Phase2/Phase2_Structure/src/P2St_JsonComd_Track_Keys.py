
import os
import numpy as np 
import pandas as pd
import json
import collections as col

#################################################################################################

# Phase1 Part0 Universal Variables' Values
variables_dict = col.defaultdict()

####################################################################

# RAM table size
variables_dict['N_GB'] = 4

# ELINT data base, so far for Phase1
MySQL_key_ELINT = {'host':'localhost','user':'','password':'','db':'','charset':'utf8'}
variables_dict['MySQL_key_ELINT'] = MySQL_key_ELINT

#################################################################################################

#################################################################################################

target2id_dict = dict()
target2id_dict['POTUS'] = '822215679726100480'
target2id_dict['POTUS44'] = '1536791610'
target2id_dict['realDonaldTrump'] = '25073877'
target2id_dict['BarackObama'] = '813286'
target2id_dict['HillaryClinton'] = '1339835893'
target2id_dict['JohnKasich'] = '18020081'
target2id_dict['tedcruz'] = '23022687'
target2id_dict['BernieSanders'] = '216776631'
target2id_dict['marcorubio'] = '15745368'
target2id_dict['RealBenCarson'] = '1180379185'

target2id_dict['cnnbrk'] = '428333'
target2id_dict['WSJ'] = '3108351'
target2id_dict['nytimes'] = '807095'
target2id_dict['BBCWorld'] = '742143'
target2id_dict['FoxNews'] = '1367531'
target2id_dict['BBCBreaking'] = '5402612'
target2id_dict['CNNPolitics'] = '13850422'
target2id_dict['BBCBreaking'] = '5402612'
target2id_dict['NBCNews'] = '14173315'
target2id_dict['Reuters'] = '1652541'
target2id_dict['cnni'] = '2097571'
target2id_dict['washingtonpost'] = '2467791'
target2id_dict['Forbes'] = '91478624'
target2id_dict['business'] = '34713362'
target2id_dict['FortuneMagazine'] = '25053299'
target2id_dict['ReutersBiz'] = '15110357'
target2id_dict['FT'] = '18949452'
target2id_dict['BBCBusiness'] = '621523'
target2id_dict['WSJbusiness'] = '28140646'
target2id_dict['ReutersWorld'] = '335455570'
target2id_dict['ReutersPolitics'] = '25562002'

target2id_dict['TheEconomist'] = '5988062'
target2id_dict['ForeignAffairs'] = '21114659'
target2id_dict['ForeignPolicy'] = '26792275'
target2id_dict['CFR_org'] = '17469492'
target2id_dict['ReutersOpinion'] = '382275399'

id2target_dict = dict()
for key in target2id_dict:
	id2target_dict[ target2id_dict[key] ] = key

variables_dict['target2id_dict'] = target2id_dict
variables_dict['id2target_dict'] = id2target_dict

#################################################################################################

#################################################################################################

# tracked keywords <--- for early data set, before mid-March, 2017
variables_dict['tracked_keywords'] = ['china','taiwan','russia','trade']

# geo-political related <--- post mid-March, 2017
variables_dict['key_china'] = ['china','chinese','beijing','xi jinping','shanghai',\
								'guangzhou','hong kong','tibet','xin jiang']
variables_dict['key_taiwan'] = ['taipei','taiwan','taiwanese']
variables_dict['key_japan'] = ['tokyo','japan','japanese']
variables_dict['key_russia'] = ['russia','moscow','kremlin','russian','putin','ukraine','crimea','NATO']
variables_dict['key_mid_east'] = ['syria','turkey','iran','isreal','isis','saudi','iraq','india','pakistan']
variables_dict['key_europe'] = ['brexit','frexit','euro','european union','french','france','le pen','national front','fillon']

# macro-economy related <--- post mid-March, 2017
variables_dict['key_Economy'] = ['trade','debt','inflation','gdp','fiscal','FDI','investment','tariff','currency',\
								'exchange rate','outbound investment','employment','unemployment','subsidiary',\
								'philanthropy','environment',\
								'tax','TPP','NAFTA','WTO','IMF','AIIB','world bank','wall street','Dow Jones']

# Chinese companies related <--- post mid-March, 2017
variables_dict['key_CNcompany'] = ['Wanda','Shuanghui','Lenovo','Haier','Fosun','Sinopec','China National Offshore Oil Corporation',\
									'Anbang','China Investment Corporation','Aviation Industry Corporation of China',\
									'Hua Capital consortium','Yantai Xinchao','Zhang Xin family','Summitview Capital consortium',\
									'Huaneng','Wanxiang','HNA','Tencent','Cinda','Ningbo Joyson']

#################################################################################################

#################################################################################################

# stream data type
# tableName = variables_dict['load_type']+"TweetStack"+pin_time.strftime('_%Y_%m_%d_%H')
variables_dict['load_type'] = 'Selected_Keyword_'

variables_dict['extract_type'] = 'Track_Keyword_'
variables_dict['extract_method'] = ['china']





#################################################################################################

#################################################################################################

# start time; 
# match the date with your first file, one day earlier
Start_Time = '2017-05-14 02:00:00'
Start_Time = pd.to_datetime(Start_Time)
End_Time = '2017-05-14 04:00:00'
End_Time = pd.to_datetime(End_Time)
# go to next time point
#Time_Period = np.timedelta64(3600*6,'s') # 2 hours, save RAM

variables_dict['Start_Time'] = Start_Time
variables_dict['End_Time'] = End_Time
variables_dict['N_Time_Period'] = 2 # 2 hours, save RAM

#################################################################################################

#################################################################################################

variables_dict['Start_Time'] = str( Start_Time )
variables_dict['End_Time'] = str( End_Time )
#variables_dict['N_Time_Period'] = str( variables_dict['Time_Period'] )

# saving model_options to .json file
#json_name = 'P1P2_FollowMedias_Comds_3.json'
json_name = 'P2St_.json'
with open(json_name, 'w') as fp:
	json.dump(variables_dict, fp, sort_keys=True, indent=4)	
	#json.dump(variables_dict, fp)






