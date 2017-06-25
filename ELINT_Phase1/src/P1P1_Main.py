

import os
from os import listdir
from os.path import isfile, join
import numpy as np 
import pandas as pd
import json
import collections as col

from twarc import Twarc
import codecs
import pymysql.cursors

from P1P1_MySQL_Operations import TweetStack_TL_Init, TweetStack_TL_convert2InnoDB, \
								  TweetStack_TL_convert2MEMORY, TweetStack_TL_load
from P1P1_JsonLoad import UserTimeLine_JsonLoad


"""
########################################################################################################
"""

# variable type check
def check_args(*types):
	def real_decorator(func):
		def wrapper(*args, **kwargs):
			for val, typ in zip(args, types):
				assert isinstance(val, typ), "Value {} is not of expected type {}".format(val, typ)
			return func(*args, **kwargs)
		return wrapper
	return real_decorator

# check each line from readline(), check length, start/end character
@check_args(str, int)
def Json_format_check(input_str, index_line):
	# check tweet_line, which should at least be "{}"
	fflag_line = True
	if len(input_str) <= 2:
		fflag_line = False
	# check if tweet_line is complete
	fflag_json = False
	if fflag_line and input_str[0:1] == '{':
		if input_str[-2:-1] == '}' or input_str[-1:] == '}': # last line has no '\n'
			fflag_json = True
	else:
		print "Line: {}, Incomplete or Empty Line".format(index_line)
	return fflag_line, fflag_json

# single input pd.timestamp
@check_args(pd.tslib.Timestamp)
def pd_timestamp_check(input):
	return True

"""
########################################################################################################

Utilities

########################################################################################################
"""

def Set_TempTable_Variables(MySQL_DBkey, N_GB=4):
	""" 
	"""	
	####################################################################
	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey['host'],
								 user=MySQL_DBkey['user'],
								 password=MySQL_DBkey['password'],
								 db=MySQL_DBkey['db'],
								 charset=MySQL_DBkey['charset'],
								 cursorclass=pymysql.cursors.DictCursor)

	####################################################################
	comd_set_temptable = """
SET GLOBAL tmp_table_size = 1024 * 1024 * 1024 * %i;
SET GLOBAL max_heap_table_size = 1024 * 1024 * 1024 * %i;
"""
	# execute Initialize Table commands
	try:
		with connection.cursor() as cursor:
			cursor.execute( comd_set_temptable % (N_GB, N_GB) )
		# commit commands
		print "Temp table size set for 1024 * 1024 * 1024 * %i" % N_GB
		connection.commit()
	finally:
		pass
	connection.close()
	return None

"""
########################################################################################################

User Time Line Extract

########################################################################################################
"""

def UserTimeLine_Extract(variables_dict, target):
	'''
	'''
	# This creates an instance of Twarc.
	credentials = variables_dict['credentials'] 
	t = Twarc(consumer_key=credentials['consumer_key'],
			  consumer_secret=credentials['consumer_secret'],
			  access_token=credentials['access_token'],
			  access_token_secret=credentials['access_token_secret']
			  )
	tweet_list = []
	# go through user timeline
	#for tweet in t.timeline(user_id='1339835893'):
	for tweet in t.timeline(screen_name=target):
		tweet_json = json.dumps(tweet)
		tweet_list.append(tweet_json)
		# tweet infor
		print "{} is created at {} with the following text: ".format(tweet['id_str'], tweet['created_at'])
		print "{}".format(tweet['text'].encode('utf-8'))
		print "by {}. \n".format(tweet['user']['screen_name'])

	return tweet_list

"""
########################################################################################################

Main Function of Phase1 Part1

########################################################################################################
"""

def Phase1_Part1_Main(variables_dict):
	'''
	variables_dict: dictionary of variables' values
					Universal through out Phase1_Part0
	'''
	# check against sys.arg from command line

	####################################################################

	target_list = variables_dict['target_list']

	MySQL_DBkey = variables_dict['MySQL_key_ELINT']
	N_GB = variables_dict['N_GB']
	# set in-RAM table size
	Set_TempTable_Variables(MySQL_DBkey = MySQL_DBkey, N_GB = N_GB)

	####################################################################

	##############################
	# go through Folder_FileList #
	##############################
	for target in target_list:
		
		print "\n\nStart working on %s\n\n" % target
		# extract user time line for target
		# and create tweet_list
		tweet_list = UserTimeLine_Extract(variables_dict=variables_dict, target=target)

		# create table TweetStack; CREATE TABLE IF NOT EXISTS
		tableName = TweetStack_TL_Init(MySQL_DBkey=MySQL_DBkey, userName=target)

		####################################################################

		# Connect to the database
		connection = pymysql.connect(host=MySQL_DBkey['host'],
									 user=MySQL_DBkey['user'],
									 password=MySQL_DBkey['password'],
									 db=MySQL_DBkey['db'],
									 charset=MySQL_DBkey['charset'],
									 cursorclass=pymysql.cursors.DictCursor)

		####################################################################

		# convert TweetStack_TL from InnoDB to Memory
		TweetStack_TL_convert2MEMORY(connection=connection, tableName=tableName)
		print "%s converted to MEMORY" % tableName

		####################################################################
		
		# go through tweet_list
		count_line = 0
		for tweet_line in tweet_list:
			count_line += 1 

			# json format check and file end check and data type check
			flag_line = False
			flag_json = False
			try:
				flag_line, flag_json = Json_format_check(tweet_line, count_line)
			except AssertionError:
				print "Line: {}, Json_format_check() data type check failed".format(index_line)
				pass 
			else:
				pass

			# read JSON tweet
			flag_TweetStack = False
			if flag_json == True:		
				# load JSON, extract information
				flag_TweetStack, Tweet_OBJ = UserTimeLine_JsonLoad(input_str=tweet_line, index_line=count_line)

			# upload TweetStack
			if flag_TweetStack:
				TweetStack_TL_load(connection=connection, Tweet_OBJ=Tweet_OBJ, tableName=tableName)
		# end of while
		####################################################################

		# convert TweetStack_TL from InnoDB to Memory
		TweetStack_TL_convert2InnoDB(connection=connection, tableName=tableName)
		print "%s converted to InnoDB" % tableName		
		
		connection.close()
	####################################################################
	# End of Folder_FileList

	####################################################################
	# saving model_options to .json file
	json_name = 'P1P1_ExecutedComds.json'
	with open(json_name, 'w') as fp:
		json.dump(variables_dict, fp, sort_keys=True, indent=4)	
	return None

"""
########################################################################################################

# Execution of Phase1_Part1

########################################################################################################
"""

if __name__ == "__main__":

	####################################################################

	# Phase1 Part0 Universal Variables' Values
	variables_dict = col.defaultdict()

	####################################################################
	
	# RAM table size
	variables_dict['N_GB'] = 4

	# data base with selected Ultra tables transfered
	MySQL_key_transUltra = {'host':'localhost','user':'','password':'','db':'','charset':'utf8'}
	# ELINT data base, so far for Phase1
	MySQL_key_ELINT = {'host':'localhost','user':'','password':'','db':'','charset':'utf8'}

	variables_dict['MySQL_key_transUltra'] = MySQL_key_transUltra
	variables_dict['MySQL_key_ELINT'] = MySQL_key_ELINT

	####################################################################

	# twitter API credentials
	credentials = dict({})

	variables_dict['credentials'] = credentials

	####################################################################

	target_list = ['POTUS','POTUS44','realDonaldTrump','BarackObama','HillaryClinton','JohnKasich','tedcruz',\
				   'BernieSanders','marcorubio','RealBenCarson','cnnbrk','WSJ','nytimes','BBCWorld','FoxNews',\
				   'BBCBreaking','CNNPolitics','NBCNews','Reuters','cnni','washingtonpost','Forbes','business',\
				   'FortuneMagazine','ReutersBiz','FT','BBCBusiness','WSJbusiness','ReutersWorld','ReutersPolitics',\
				   'TheEconomist','ForeignAffairs','ForeignPolicy','CFR_org','ReutersOpinion',\
				   'TIME','voguemagazine','ABC','Metro_TV','timesofindia','ndtv','XHNews','TimesNow','lemondefr',\
				   'WIRED','ntv','guardian','la_patilla','CBSNews','FinancialTimes','IndiaToday','CNNnews18','politico',\
				   'RT_com','RT_America','IndianExpress','detikcom','UberFacts','AP','globovision','the_hindu','USATODAY','DolarToday']

# @POTUS as 822215679726100480 <- back track to Jan 20th, 2017
# @realDonaldTrump as 25073877 <- back track to March 4th, 2016
# @BarackObama as 813286 <- back track to July 31st, 2014
# @HillaryClinton as 1339835893 <- back track to July 19th, 2016
# @JohnKasich as 18020081 <- back track to Jan 5th, 2016
# @tedcruz as 23022687 <- back track to March 14th, 2016
# @BernieSanders as 216776631 <- back track to March 1st, 2016
# @marcorubio as 15745368 <- back track to July 17th, 2015
# @RealBenCarson as 1180379185 <- back track to Feb 15th, 2013

# <--- Institutions
# cnnbrk(CNN breaking news) as 428333 <- back track to 2016-07-22 17:16:25
# WSJ as 3108351 <- back track to 2016-12-25 01:47:43
# nytimes as 807095 <- back track to 2016-12-29 07:51:28
# BBCWorld as 742143 <- back track to 2016-12-13 01:57:51
# FoxNews as 1367531 <- back track to 2017-01-07 15:43:56
# BBCBreaking as 5402612 <- back track to  <- back track to 2016-03-24 09:24:41
# CNNPolitics as 13850422 <- back track to 2016-12-17 13:02:01
# NBCNews as 14173315 <- back track to 2016-12-17 07:00:14
# Reuters as 1652541 <- back track to 2016-12-27 20:14:54
# cnni(CNN International) as 2097571 <- back track to 2016-12-16 03:50:01
# washingtonpost as 2467791 <- back track to 2016-12-30 15:23:50
# Forbes as 91478624
# business (Bloomberg) as 34713362 <--- VERY ACTIVE
# FortuneMagazine as 25053299
# ReutersBiz (Reuters Business) as 15110357
# FT (Financial Times) as 18949452
# BBCBusiness (BBC Business) as 621523
# WSJbusiness as 28140646
# ReutersWorld as 335455570
# ReutersPolitics as 25562002

# TheEconomist as 5988062
# ForeignAffairs as 21114659
# ForeignPolicy as 26792275 <--- Rather active
# CFR_org (Council on Forign Relations) as 17469492
# ReutersOpinion as 382275399

# <--- post May24th
# <--- added because of high followers
# TIME as 14293310 <--- rather active
# voguemagazine as 136361303
# ABC as 28785486
# Metro_TV as 57261519 <--- Very active
# timesofindia as 134758540 <--- rather active
# ndtv as 37034483 <--- rather active
# XHNews as 487118986
# TimesNow as 240649814 <--- rather active
# lemondefr as 24744541 
# WIRED as 1344951
# ntv as 15016209
# guardian as 87818409
# la_patilla as 124172948 <--- not sure what is this <--- rather active
# CBSNews as 15012486 
# FinancialTimes as 4898091 
# IndiaToday as 19897138 <--- Very Active
# CNNnews18 as 6509832
# politico as 9300262 <--- rather active
# RT_com as 64643056
# RT_America as 115754870
# IndianExpress as 38647512
# detikcom as 69183155
# UberFacts as 95023423
# AP as 51241574
# globovision as 17485551
# the_hindu as 20751449
# USATODAY as 15754281
# DolarToday as 145459615

	variables_dict['target_list'] = target_list

	####################################################################
	
	Phase1_Part1_Main(variables_dict)

	####################################################################














