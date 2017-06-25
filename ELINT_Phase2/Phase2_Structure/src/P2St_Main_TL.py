#!/usr/bin/env python

import sys
import os
from os import listdir
from os.path import isfile, join
import numpy as np 
import pandas as pd
import json
import collections as col

import pymysql.cursors

from P2St_MySQL_Operations_TL import *
from P2St_coreNLP import P2St_NP_Structure
from P2St_TextFilter import Remove_http_tag_user, Negative_Parsing, track_keywords_filter


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
		Parameters
		----------
		Returns
		-------
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

########################################################################################################
def convert2InnoDB(connection, tableName):

	# Comds for Converting in-RAM tables into InnoDB tables
	comd_convert = """
ALTER TABLE %s ENGINE=InnoDB;"""
	# Convert tables
	try:
		with connection.cursor() as cursor:
			cursor.execute( comd_convert % tableName )
		# commit commands
		connection.commit()
	finally:
		pass
	return None

########################################################################################################
def convert2MEMORY(connection, tableName):

	# Comds for Converting in-RAM tables into InnoDB tables
	comd_convert = """
ALTER TABLE %s ENGINE=MEMORY;"""
	# Convert tables
	try:
		with connection.cursor() as cursor:
			cursor.execute( comd_convert % tableName )
		# commit commands
		connection.commit()
	finally:
		pass
	return None

"""
########################################################################################################

Main Function of Phase2 NP Structure

########################################################################################################
"""

def Phase2_St_TL(comd_json):
	'''
	variables_dict: dictionary of variables' values
					Universal through out Phase1_Part0
	'''
	# check against sys.arg from command line
	if len(sys.argv)==1:
		#raise ValueError('Must specify input Commands.')
		
		f = open(comd_json,'r')
		text = f.read()
		f.close()
		# convert to json
		variables_dict = json.loads(text)
	
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

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey['host'],
								 user=MySQL_DBkey['user'],
								 password=MySQL_DBkey['password'],
								 db=MySQL_DBkey['db'],
								 charset=MySQL_DBkey['charset'],
								 cursorclass=pymysql.cursors.DictCursor)

	####################################################################

	########################################################################################################

	# Main control loop
	for userName in variables_dict['target2id_dict']:

		################################
		# create Selected Tweets Table #
		################################
		# create/open table TweetStack; CREATE TABLE IF NOT EXISTS
		tbN_Selected_Stack = Selected_TweetStack_Init(connection=connection, 
													  MySQL_DBkey=MySQL_DBkey, 
													  key_userName=userName, 
													  variables_dict=variables_dict
													  )
		# convert tbN_Selected_Stack from InnoDB to Memory
		convert2MEMORY(connection=connection, tableName=tbN_Selected_Stack)
		print "%s converted to MEMORY" % tbN_Selected_Stack

		###########################
		# Extract Selected Tweets #
		###########################
		# list of dicts; could be None
		Selected_Tweets_list = Selected_Tweets_Extract(connection=connection, 
													   MySQL_DBkey=MySQL_DBkey, 
													   key_userName=userName, 
													   variables_dict=variables_dict
													   )
		
		#################################
		# Text filter and NP extraction #
		#################################
		if Selected_Tweets_list is not None:
			for Tweet_dict in Selected_Tweets_list:
				# remove http, #, @ expression
				Tweet_dict['ProcessedText'] = Remove_http_tag_user(input_str=Tweet_dict['OriginalText'])
				# parse negative expression
				Tweet_dict['ProcessedText'] = Negative_Parsing(input_str=Tweet_dict['ProcessedText'])
				# parse NP list, returns list of str
				Tweet_dict['NP_list'] = P2St_NP_Structure(sentence_input=Tweet_dict['ProcessedText'])
				# update Concerned_Keywords
				Tweet_dict = track_keywords_filter(Tweet_Input=Tweet_dict, variables_dict=variables_dict)

		##############################
		# Load Selected Tweets Table #
		##############################
		# insert Selected_Tweets_list
		Selected_TweetStack_load(connection=connection, 
								 Selected_Tweets_list=Selected_Tweets_list, 
								 tableName=tbN_Selected_Stack
								 )
		# convert tbN_Selected_Stack from InnoDB to Memory
		convert2InnoDB(connection=connection, tableName=tbN_Selected_Stack)
		print "%s converted to InnoDB" % tbN_Selected_Stack

		# end of userName

	# end of time-control
	return None

"""
########################################################################################################

# Execution of Phase2_Structure

########################################################################################################
"""

if __name__ == "__main__":

	Phase2_St_TL(comd_json='P2_TL_St_.json')








