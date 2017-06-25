

import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors


"""
###########################################################################################################

# main data structure, contains information from each tweet
Tweet_OBJ = col.defaultdict()
# tweet
Tweet_OBJ['tweet_time'] = None # pd.timestamp
Tweet_OBJ['tweet_id'] = None # all id are id_str
Tweet_OBJ['text'] = "" # make sure text is onto a single line.....
Tweet_OBJ['lang'] = ""
Tweet_OBJ['coordinates'] = "" # Null, or {"coordinates":[-75.14310264,40.05701649],"type":"Point"}

Tweet_OBJ['reply_to_userID'] = "-1" # set of tuple(userID, userName)
Tweet_OBJ['quoted_status_id'] = "-1"
Tweet_OBJ['in_reply_to_status_id'] = "-1" # set of tweetID (set of strings)

Tweet_OBJ['retweet_count'] = 0
Tweet_OBJ['favorite_count'] = 0	

Tweet_OBJ['Discussion_userName'] = ""
Tweet_OBJ['Discussion_tweetID'] = "-1" 

Tweet_OBJ['Concern_userName'] = set()

# user
Tweet_OBJ['user_id'] = "-1" # all id are id_str
Tweet_OBJ['user_name'] = "" 
Tweet_OBJ['user_verified'] = 0 # default

Tweet_OBJ['user_followers'] = 0 # default 0
Tweet_OBJ['user_friends'] = 0
Tweet_OBJ['user_favourites'] = 0
Tweet_OBJ['user_listed'] = 0
Tweet_OBJ['user_statuses'] = 0

# hash tag
Tweet_OBJ['Tag'] = set() # set of strings
# mentions
Tweet_OBJ['mentioned_userID'] = set() # set of tuple(userID, userName)

###########################################################################################################
"""

###########################################################################################################

def taglist_targetslist_filter(Tweet_OBJ, tag_list, userID_list):
	'''
	for geo_streamed data file from Ultra data set
	using Ultra_taglist and targets_list to filter

	Tweet_OBJ:
	tag_list: list of tag_str
	userID_list: list of targets' userID_str

	returns: flag_final, Tweet_OBJ
	'''
	flag_final = False

	###############################
	# check against Ultra taglist #
	###############################
	# adjust flag_final against hash tags
	for tag in Tweet_OBJ['Tag']:
		if tag in tag_list: 
			flag_final = True

	#############################
	# check against lv0_targets #
	#############################
	# adjust flag_final against mentioned_userID
	for userID in Tweet_OBJ['mentioned_userID']:
		if userID in userID_list:
			flag_final = True
			Tweet_OBJ['Concern_userName'].add(variables_dict['id2target_dict'][userID])

	# adjust flag_final against in_reply_to_user_id_str
	if Tweet_OBJ['reply_to_userID'] in userID_list:
		flag_final = True
		Tweet_OBJ['Concern_userName'].add(variables_dict['id2target_dict'][Tweet_OBJ['reply_to_userID']])

	return flag_final, Tweet_OBJ

###########################################################################################################

def track_keywords_filter(Tweet_OBJ, tag_list, userID_list, variables_dict):
	'''
	put class tokens of keywords into Tweet_OBJ['Concern_userName']
	update Tweet_OBJ['Concern_userName'] as USUAL

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

	Tweet_OBJ:
	tag_list: list of tag_str
	userID_list: list of targets' userID_str
	variables_dict:

	returns: flag_final, Tweet_OBJ
	'''
	flag_final = False

	##########################
	# check against keywords #
	##########################

	# geo-political related <--- post mid-March, 2017
	for keyword in variables_dict['key_china']:
		if keyword in Tweet_OBJ['text'].lower(): 
			flag_final = True
			Tweet_OBJ['Concern_userName'].add('china')
			break

	for keyword in variables_dict['key_taiwan']:
		if keyword in Tweet_OBJ['text'].lower(): 
			flag_final = True
			Tweet_OBJ['Concern_userName'].add('taiwan')
			break

	for keyword in variables_dict['key_japan']:
		if keyword in Tweet_OBJ['text'].lower(): 
			flag_final = True
			Tweet_OBJ['Concern_userName'].add('japan')
			break

	for keyword in variables_dict['key_russia']:
		if keyword in Tweet_OBJ['text'].lower(): 
			flag_final = True
			Tweet_OBJ['Concern_userName'].add('russia')
			break

	for keyword in variables_dict['key_mid_east']:
		if keyword in Tweet_OBJ['text'].lower(): 
			flag_final = True
			Tweet_OBJ['Concern_userName'].add('mid_east')
			break

	for keyword in variables_dict['key_europe']:
		if keyword in Tweet_OBJ['text'].lower(): 
			flag_final = True
			Tweet_OBJ['Concern_userName'].add('europe')
			break

	# macro-economy related <--- post mid-March, 2017
	for keyword in variables_dict['key_Economy']:
		if keyword in Tweet_OBJ['text'].lower(): 
			flag_final = True
			Tweet_OBJ['Concern_userName'].add('Economy')
			break

	# Chinese companies related <--- post mid-March, 2017
	for keyword in variables_dict['key_CNcompany']:
		if keyword in Tweet_OBJ['text'].lower(): 
			flag_final = True
			Tweet_OBJ['Concern_userName'].add('CNcompany')
			break

	#############################
	# check against lv0_targets #
	#############################
	# adjust flag_final against mentioned_userID
	for userID in Tweet_OBJ['mentioned_userID']:
		if userID in userID_list:
			flag_final = True
			Tweet_OBJ['Concern_userName'].add(variables_dict['id2target_dict'][userID])

	# adjust flag_final against in_reply_to_user_id_str
	if Tweet_OBJ['reply_to_userID'] in userID_list:
		flag_final = True
		Tweet_OBJ['Concern_userName'].add(variables_dict['id2target_dict'][Tweet_OBJ['reply_to_userID']])

	# adjust flag_final against user_id; quite Unlikely
	if Tweet_OBJ['user_id'] in userID_list:
		flag_final = True
		Tweet_OBJ['Concern_userName'].add(variables_dict['id2target_dict'][Tweet_OBJ['user_id']])

	return flag_final, Tweet_OBJ

###########################################################################################################

def follow_medias_filter(Tweet_OBJ, tag_list, userID_list, variables_dict):
	'''
	put keywords (variables_dict['tracked_keywords']=['china','taiwan','russia','trade']) 
	into Tweet_OBJ['Concern_userName']
	update Tweet_OBJ['Concern_userName'] as USUAL

	Extactly the same as track_keywords_filter()

	Tweet_OBJ:
	tag_list: list of tag_str
	userID_list: list of targets' userID_str
	variables_dict:

	returns: flag_final, Tweet_OBJ
	'''
	flag_final = False

	##########################
	# check against keywords #
	##########################
	for keyword in variables_dict['tracked_keywords']:
		if keyword in Tweet_OBJ['text'].lower(): 
			flag_final = True
			Tweet_OBJ['Concern_userName'].add(keyword)

	#############################
	# check against lv0_targets #
	#############################
	# adjust flag_final against mentioned_userID
	for userID in Tweet_OBJ['mentioned_userID']:
		if userID in userID_list:
			flag_final = True
			Tweet_OBJ['Concern_userName'].add(variables_dict['id2target_dict'][userID])

	# adjust flag_final against in_reply_to_user_id_str
	if Tweet_OBJ['reply_to_userID'] in userID_list:
		flag_final = True
		Tweet_OBJ['Concern_userName'].add(variables_dict['id2target_dict'][Tweet_OBJ['reply_to_userID']])

	# adjust flag_final against user_id; quite Unlikely
	if Tweet_OBJ['user_id'] in userID_list:
		flag_final = True
		Tweet_OBJ['Concern_userName'].add(variables_dict['id2target_dict'][Tweet_OBJ['user_id']])

	return flag_final, Tweet_OBJ

###########################################################################################################








