

import os
import numpy as np 
import pandas as pd
import json
import collections as col


#################################################################################################

# Phase1 Part0 Universal Variables' Values
variables_dict = col.defaultdict()

#################################################################################################

#################################################################################################

# @POTUS as 822215679726100480 <- back track to Jan 20th, 2017
# @POTUS44 as 1536791610 <- back track to May 18th, 2015
# @realDonaldTrump as 25073877 <- back track to March 4th, 2016
# @BarackObama as 813286 <- back track to July 31st, 2014
# @HillaryClinton as 1339835893 <- back track to July 19th, 2016
# @JohnKasich as 18020081 <- back track to Jan 5th, 2016
# @tedcruz as 23022687 <- back track to March 14th, 2016
# @BernieSanders as 216776631 <- back track to March 1st, 2016
# @marcorubio as 15745368 <- back track to July 17th, 2015
# @RealBenCarson as 1180379185 <- back track to Feb 15th, 2013

# New Medias
######################################################
# cnnbrk(CNN breaking news) as 428333
# WSJ as 3108351
# nytimes as 807095
# BBCWorld as 742143
# FoxNews as 1367531
# BBCBreaking as 5402612
# CNNPolitics as 13850422
# NBCNews as 14173315
# Reuters as 1652541
# cnni(CNN International) as 2097571
# washingtonpost as 2467791
# Forbes as 91478624
# business (Bloomberg) as 34713362
# FortuneMagazine as 25053299
# ReutersBiz (Reuters Business) as 15110357
# FT (Financial Times) as 18949452
# BBCBusiness (BBC Business) as 621523
# WSJbusiness as 28140646
# ReutersWorld as 335455570
# ReutersPolitics as 25562002

# Opinion Medias
######################################################
# TheEconomist as 5988062
# ForeignAffairs as 21114659
# ForeignPolicy as 26792275
# CFR_org (Council on Forign Relations) as 17469492
# ReutersOpinion as 382275399

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

media2id_dict = dict()
media2id_dict['cnnbrk'] = '428333'
media2id_dict['WSJ'] = '3108351'
media2id_dict['nytimes'] = '807095'
media2id_dict['BBCWorld'] = '742143'
media2id_dict['FoxNews'] = '1367531'
media2id_dict['BBCBreaking'] = '5402612'
media2id_dict['CNNPolitics'] = '13850422'
media2id_dict['BBCBreaking'] = '5402612'
media2id_dict['NBCNews'] = '14173315'
media2id_dict['Reuters'] = '1652541'
media2id_dict['cnni'] = '2097571'
media2id_dict['washingtonpost'] = '2467791'
media2id_dict['Forbes'] = '91478624'
media2id_dict['business'] = '34713362'
media2id_dict['FortuneMagazine'] = '25053299'
media2id_dict['ReutersBiz'] = '15110357'
media2id_dict['FT'] = '18949452'
media2id_dict['BBCBusiness'] = '621523'
media2id_dict['WSJbusiness'] = '28140646'
media2id_dict['ReutersWorld'] = '335455570'
media2id_dict['ReutersPolitics'] = '25562002'

media2id_dict['TheEconomist'] = '5988062'
media2id_dict['ForeignAffairs'] = '21114659'
media2id_dict['ForeignPolicy'] = '26792275'
media2id_dict['CFR_org'] = '17469492'
media2id_dict['ReutersOpinion'] = '382275399'

id2media_dict = dict()
for key in media2id_dict:
	id2media_dict[ media2id_dict[key] ] = key

variables_dict['media2id_dict'] = media2id_dict
variables_dict['id2media_dict'] = id2media_dict

#################################################################################################

#################################################################################################

# tracked keywords
variables_dict['tracked_keywords'] = ['china','taiwan','russia','trade']

#################################################################################################

#################################################################################################

# keys of variables_dict; for list of keywords (list of strings)
variables_dict['News_filters_key'] = ['filter_China_keywords','filter_Russia_keywords',\
									  'filter_Trade_keywords']

# ELINT_Phase2 table header
# tableName: header_'China'_'timeStamp'
variables_dict['filtered_newsTable_header'] = 'flteredNews_'

# MySQL tableName timestamp
# DO NOT change this one....
variables_dict['tableName_timestamp'] = '_stamp_Feb05_2017'

# MySQL tableName timestamp in query
# latest MySQL UTL timestamp at: 2017-03-18 00:00:00
variables_dict['query_timestamp'] = '2017-02-08 00:00:00'

# MySQL UTL table header, at MySQL_key_ELINT
# tweetstack_tl_'Medias_name'
variables_dict['UTL_tableName_header'] = 'tweetstack_tl_'

######################################################
# Filter Keywords for China 
# (including Taiwan, North Korea, HK)
# _Xi, space is important
variables_dict['filter_China_keywords'] = ['China','Beijing','Chinese',' Xi',\
				'Shanghai','Guangzhou','Hong Kong','Taiwan','Taipei','Xinjiang','Tibet',\
				'TPP','North Korea']

######################################################
# Filter Keywords for Russia 
# (includng Ukraine, Crimea, NATO)
variables_dict['filter_Russia_keywords'] = ['Russia','Moscow','Kremlin','Russian','Putin',\
				'Ukraine','Crimea','NATO']

######################################################
# Filter Keywords for Japan
variables_dict['filter_Japan_keywords'] = ['Japan','Japanese','Tokyo',' Abe']

######################################################

######################################################
# Filter Keywords for Trade
# (very broad scope)
variables_dict['filter_Trade_keywords'] = ['trade','debt','inflation','growth rate','GDP','fiscal',\
										   'FDI','investment','tariff','currency','exchange rate','tax',\
										   'TPP','NAFTA','WTO','IMF','World Bank',\
										   'AIIB','Asian Infrastructure Investment Bank',\
										   'Wall Street','bank','Dow Jones','S%P 500']

#################################################################################################

#################################################################################################

# RAM table size
variables_dict['N_GB'] = 4

# data base with selected Ultra tables transfered
MySQL_key_transUltra = {'host':'localhost','user':'','password':'','db':'','charset':'utf8'}
# ELINT data base, intended as the root data base for ELINT
MySQL_key_ELINT = {'host':'localhost','user':'','password':'','db':'','charset':'utf8'}
# ELINT data base, for Phase2 specifics
MySQL_key_ELINT_P2 = {'host':'localhost','user':'','password':'','db':'','charset':'utf8'}

variables_dict['MySQL_key_transUltra'] = MySQL_key_transUltra
variables_dict['MySQL_key_ELINT'] = MySQL_key_ELINT
variables_dict['MySQL_key_ELINT_P2'] = MySQL_key_ELINT_P2

#################################################################################################

#################################################################################################

# folder addr where to save .JSON (article information) and .txt (article full text)
variables_dict['Articles_save_addr'] = '../Data/'

#################################################################################################

#################################################################################################

# saving model_options to .json file
json_name = 'JsonComd_Extract_NewsArticles_Operations.json'
with open(json_name, 'w') as fp:
	json.dump(variables_dict, fp, sort_keys=True, indent=4)	
	#json.dump(variables_dict, fp)




