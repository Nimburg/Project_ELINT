

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

# Number of Entity Types Encountered: 28
'''
u'City', u'Movie', u'Crime', u'Holiday', u'OperatingSystem', u'Degree', u'Company', u'Country', u'Region', 
u'Drug', u'Person', u'ProfessionalDegree', u'HealthCondition', u'Organization', u'Quantity', u'FieldTerminology', 
u'EntertainmentAward', u'TelevisionShow', u'Facility', u'JobTitle', u'GeographicFeature', u'TelevisionStation', 
u'StateOrCounty', u'NaturalDisaster', u'PrintMedia', u'Sport', u'Technology', u'Continent'
'''

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

# tracked keywords
variables_dict['tracked_keywords'] = ['china','taiwan','russia','trade']

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

# list of .json files for Entities extracted from Speeches/Statements
json_Entities_list = ['HSp_entities_to_fileName.json','HSt_entities_to_fileName.json',\
					  'TSp_entities_to_fileName.json','TSt_entities_to_fileName.json']

# address of data files
json_Entities_addr = '../Results/'

variables_dict['json_ENtities_list'] = json_Entities_list
variables_dict['json_Entities_addr'] = json_Entities_addr

# table name for concatenated Entities, all types, full text of speech/statement
variables_dict['tableName_cedEnti_fullText'] = 'Entities_Full_SpeechStatement'
# table name for concatenated Entities, all types, filtered text of speech/statement
variables_dict['tableName_cedEnti_filteredText'] = 'Entities_Filtered_SpeechStatement'

######################################################
# controls whether to load all .json files under addr
variables_dict['flag_fullText'] = True

#################################################################################################

#################################################################################################

# saving model_options to .json file
json_name = 'P2_Extracted_Entities_Operations.json'
with open(json_name, 'w') as fp:
	json.dump(variables_dict, fp, sort_keys=True, indent=4)	
	#json.dump(variables_dict, fp)




