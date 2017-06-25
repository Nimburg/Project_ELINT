#!/usr/bin/env python


import sys
import os
from os import listdir
from os.path import isfile, join
import json
import numpy as np 
import pandas as pd
import time
import collections as col

import pymysql.cursors

from P2_IR2_EntitiesOperations import Extract_Concatenate_Entities, Load_Entities
from P2_IR7_NewsArticles_Operations import filteredNews_init, Filter_Extract_MediasTweets, Load_MediasTweets


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
	'''
	'''
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

Main Function for: Concatenate Entities (by Alchemy) from .JSON to MySQL table

########################################################################################################
"""

def Phase2_IR2_Concatenate_Extracted_Entities(comd_json=None):
	'''
	Operations on Extracted Entities from Alchemy 
	load form json files, concatenate properties (Ncall, relevance, type and casing)
	load into ELINT_Phase1 database, with Ncall, relevance and type as columns
	'''
	# check against sys.arg from command line
	if len(sys.argv)==1:
		#raise ValueError('Must specify input Commands.')
		if comd_json is None: 
			return None # end of execution
		else:
			f = open(comd_json,'r')
			text = f.read()
			f.close()
			# convert to json
			variables_dict = json.loads(text)
	# comd_json from command line 
	else:
		f = open(sys.argv[1],'r')
		text = f.read()
		f.close()
		# convert to json
		variables_dict = json.loads(text)

	####################################################################

	MySQL_DBkey = variables_dict['MySQL_key_ELINT']
	N_GB = variables_dict['N_GB']
	# set in-RAM table size
	Set_TempTable_Variables(MySQL_DBkey = MySQL_DBkey, N_GB = N_GB)

	####################################################################
	
	#####################################
	# Concatenate Entities from Alchemy #
	#####################################
	# check if there is specified json file list
	json_list = None
	if len( variables_dict['json_ENtities_list'] ) > 0:
		json_list = variables_dict['json_ENtities_list']
	# extract Entities from json files
	entity_types_set, Entities_Concatenated = Extract_Concatenate_Entities(variables_dict=variables_dict, 
																		   json_list=json_list)

	##########################
	# load into ELINT_Phase1 #
	##########################

	Load_Entities(variables_dict=variables_dict, 
				  Entities_Concatenated=Entities_Concatenated, 
				  flag_fullText=variables_dict['flag_fullText'])

	####################################################################
	return None

"""
########################################################################################################

Main Function for: Filter UTL tweets of Medias, Extract Full Articles and Save

########################################################################################################
"""

def Phase2_IR7_Extract_NewsArticles(comd_json=None):
	'''
	filter UTL of Medias at db:ELINT_Phase1, using filters
	Extract valid url, Extract and save articles
	Create filteredNews_ table at ELINT_Phase2
	'''
	# check against sys.arg from command line
	if len(sys.argv)==1:
		#raise ValueError('Must specify input Commands.')
		if comd_json is None: 
			return None # end of execution
		else:
			f = open(comd_json,'r')
			text = f.read()
			f.close()
			# convert to json
			variables_dict = json.loads(text)
	# comd_json from command line 
	else:
		f = open(sys.argv[1],'r')
		text = f.read()
		f.close()
		# convert to json
		variables_dict = json.loads(text)

	####################################################################

	MySQL_DBkey = variables_dict['MySQL_key_ELINT']
	N_GB = variables_dict['N_GB']
	# set in-RAM table size
	Set_TempTable_Variables(MySQL_DBkey = MySQL_DBkey, N_GB = N_GB)

	MySQL_DBkey = variables_dict['MySQL_key_ELINT_P2']
	N_GB = variables_dict['N_GB']
	# set in-RAM table size
	Set_TempTable_Variables(MySQL_DBkey = MySQL_DBkey, N_GB = N_GB)

	####################################################################

	# generate the over-all filteredNews_table
	# at ELINT_Phase2 db

	filteredNews_tableName = filteredNews_init(variables_dict=variables_dict, 
											   MySQL_DBkey=variables_dict['MySQL_key_ELINT_P2'])

	##############################################################################################

	# over-all data structre
	# 
	# has key: filter_name
	filteredNews_tweetStack = col.defaultdict()

	#################################
	# go through all the filter_key #
	#################################
	for item in variables_dict['News_filters_key']:
		filter_key = item
		filter_name = item[7:-9]

		###############################################
		# create flteredNews_table, extract/save text #
		###############################################
		# Article_Infor_dictionary is on per-filter level
		# Article_Infor_dictionary has key: 
		#
		# medias_tweet information: tweetID, tweetTime, userID, tweetText, MenUserList_text, TagList_text
		# article information: (title, author, root_url)
		# saved_file information: infor_json, txt,
		# post_Alchemy_json, post_coreNLP_json <--- not generated here
		filteredNews_tweetStack[filter_name] = Filter_Extract_MediasTweets(variables_dict=variables_dict, 
																		   filter_key=filter_key)

	# end of for item in variables_dict['News_filters_key']:
	# Done extracting all relevant tweets for all filters
	
	####################################################
	# load filteredNews tweets into filteredNews_table #
	####################################################

	Load_MediasTweets(variables_dict=variables_dict, filteredNews_tweetStack=filteredNews_tweetStack)

	##############################################################################################
	return None

"""
########################################################################################################

Execution of Phase2 IR2 IR7

########################################################################################################
"""

if __name__ == "__main__":

	#########################################################################
	'''
	Operations on Extracted Entities from Alchemy 
	load form json files, concatenate properties (Ncall, relevance, type and casing)
	load into ELINT_Phase1 database, with Ncall, relevance and type as columns
	'''
	Phase2_IR2_Concatenate_Extracted_Entities(comd_json=None)

	#########################################################################

	'''
	filter UTL of Medias at db:ELINT_Phase1, using filters
	Extract valid url, Extract and save articles
	Create filteredNews_ table at ELINT_Phase2
	'''
	Phase2_IR7_Extract_NewsArticles(comd_json='.json')

	#########################################################################










