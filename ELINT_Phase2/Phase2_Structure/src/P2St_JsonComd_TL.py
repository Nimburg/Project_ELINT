
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
#target2id_dict['POTUS'] = '822215679726100480'
target2id_dict['POTUS44'] = '1536791610'
#target2id_dict['realDonaldTrump'] = '25073877'
#target2id_dict['BarackObama'] = '813286'
#target2id_dict['HillaryClinton'] = '1339835893'
target2id_dict['JohnKasich'] = '18020081'
target2id_dict['tedcruz'] = '23022687'
target2id_dict['BernieSanders'] = '216776631'
target2id_dict['marcorubio'] = '15745368'
#target2id_dict['RealBenCarson'] = '1180379185'

#target2id_dict['cnnbrk'] = '428333'
#target2id_dict['WSJ'] = '3108351'
#target2id_dict['nytimes'] = '807095'
#target2id_dict['BBCWorld'] = '742143'
#target2id_dict['FoxNews'] = '1367531'
target2id_dict['BBCBreaking'] = '5402612'
target2id_dict['CNNPolitics'] = '13850422'
target2id_dict['BBCBreaking'] = '5402612'
#target2id_dict['NBCNews'] = '14173315'
target2id_dict['Reuters'] = '1652541'
#target2id_dict['cnni'] = '2097571'
target2id_dict['washingtonpost'] = '2467791'
#target2id_dict['Forbes'] = '91478624'
target2id_dict['business'] = '34713362'
target2id_dict['FortuneMagazine'] = '25053299'
target2id_dict['ReutersBiz'] = '15110357'
#target2id_dict['FT'] = '18949452'
target2id_dict['BBCBusiness'] = '621523'
#target2id_dict['WSJbusiness'] = '28140646'
#target2id_dict['ReutersWorld'] = '335455570'
#target2id_dict['ReutersPolitics'] = '25562002'

target2id_dict['TheEconomist'] = '5988062'
target2id_dict['ForeignAffairs'] = '21114659'
#target2id_dict['ForeignPolicy'] = '26792275'
#target2id_dict['CFR_org'] = '17469492'
target2id_dict['ReutersOpinion'] = '382275399'

target2id_dict['TIME'] = '14293310'
#target2id_dict['voguemagazine'] = '136361303'
#target2id_dict['ABC'] = '28785486'
target2id_dict['Metro_TV'] = '57261519'
target2id_dict['timesofindia'] = '134758540'
#target2id_dict['ndtv'] = '37034483'
target2id_dict['XHNews'] = '487118986'
#target2id_dict['TimesNow'] = '240649814'
target2id_dict['lemondefr'] = '24744541'
target2id_dict['WIRED'] = '1344951'
target2id_dict['ntv'] = '15016209'
target2id_dict['guardian'] = '87818409'
target2id_dict['la_patilla'] = '124172948'
target2id_dict['CBSNews'] = '15012486'
#target2id_dict['FinancialTimes'] = '4898091'
#target2id_dict['IndiaToday'] = '19897138'
#target2id_dict['CNNnews18'] = '6509832'
#target2id_dict['politico'] = '9300262'
target2id_dict['RT_com'] = '64643056'
target2id_dict['RT_America'] = '115754870'
#target2id_dict['IndianExpress'] = '38647512'
#target2id_dict['detikcom'] = '69183155'
#target2id_dict['UberFacts'] = '95023423'
target2id_dict['AP'] = '51241574'
#target2id_dict['globovision'] = '17485551'
#target2id_dict['the_hindu'] = '20751449'
#target2id_dict['USATODAY'] = '15754281'
#target2id_dict['DolarToday'] = '145459615'

id2target_dict = dict()
for key in target2id_dict:
	id2target_dict[ target2id_dict[key] ] = key

variables_dict['target2id_dict'] = target2id_dict
variables_dict['id2target_dict'] = id2target_dict

#################################################################################################

#################################################################################################

# geo-political related <--- post mid-March, 2017
variables_dict['key_china'] = ['china','chinese','beijing','xi jinping','shanghai',\
							   'guangzhou','hong kong','tibet','xin jiang','rmb','yuan']

variables_dict['key_korea'] = ['korea','pyongyang','seoul']

variables_dict['key_taiwan'] = ['taipei','taiwan','taiwanese']

variables_dict['key_japan'] = ['tokyo','japan','japanese','yen']

variables_dict['key_russia'] = ['russia','moscow','kremlin','russian','putin','ukraine','crimea','NATO']

variables_dict['key_mid_east'] = ['syria','turkey','iran','isreal','isis','saudi','iraq','pakistan','afghanistan',\
								  'egypt','jordan','kuwait','lebanon','palestine','qatar','arabia','yemen']

variables_dict['key_india'] = ['india','new delhi','delhi','indian']

variables_dict['key_europe'] = ['brexit','britain','british','uk','london','england','scotland',\
								'europe','euro','european union',\
								'french','france','le pen','frexit','national front','fillon','paris',\
								'german','germany','berlin','merkel']

# macro-economy related <--- post mid-March, 2017
variables_dict['key_Economy'] = ['trade','debt','inflation','gdp','fiscal','FDI','investment','tariff','currency',\
								'exchange rate','outbound investment','employment','unemployment','subsidiary',\
								'philanthropy','environment',\
								'rmb','dollar','euro','yuan','yen',\
								'tax','TPP','NAFTA','WTO','IMF','AIIB','world bank','wall street','Dow Jones']

# Chinese companies related <--- post mid-March, 2017
variables_dict['key_CNcompany'] = ['Wanda','Shuanghui','Lenovo','Haier','Fosun','Sinopec','China National Offshore Oil Corporation',\
									'Anbang','China Investment Corporation','Aviation Industry Corporation of China',\
									'Hua Capital consortium','Yantai Xinchao','Zhang Xin family','Summitview Capital consortium',\
									'Huaneng','Wanxiang','HNA','Tencent','Cinda','Ningbo Joyson']

#################################################################################################

#################################################################################################

# stream data type
# tableName = TweetStack_TL_userName
variables_dict['extract_type'] = 'TweetStack_TL_'

# tableName = TweetStack_TL_St_userName
variables_dict['load_type'] = 'TweetStack_St_TL_'

#################################################################################################

#################################################################################################

# saving model_options to .json file
json_name = 'P2_TL_St_.json'
with open(json_name, 'w') as fp:
	json.dump(variables_dict, fp, sort_keys=True, indent=4)	
	#json.dump(variables_dict, fp)






