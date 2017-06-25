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

from P1P2_MySQL_Operations import TweetStack_Init, TweetStack_load
from P1P2_MySQL_Operations import convert2InnoDB, convert2MEMORY
from P1P2_MySQL_Operations import UserUnique_Init, UserUnique_load
from P1P2_MySQL_Operations import ExtractFilter_Tags, ExtractFilter_tweetIDs, ExtractFilter_userIDs
from P1P2_JsonLoad import Ultra_JsonLoad


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

def DataFile_Iter(file_name_list, file_idex, file_addr):
	'''
	go through file_name_list
	returns file_input = open(Inputfilename,'r') of each data file
	'''
	# read tweets.txt file data
	InputfileDir = os.path.dirname(os.path.realpath('__file__'))
	print InputfileDir
	
	data_file_name = file_addr + file_name_list[file_idex]
	
	Inputfilename = os.path.join(InputfileDir, data_file_name) # ../ get back to upper level
	Inputfilename = os.path.abspath(os.path.realpath(Inputfilename))
	print Inputfilename
	file_input = open(Inputfilename,'r')

	return file_input

"""
########################################################################################################

Construct Discussion_Tree

########################################################################################################
"""

def DiscussionTree_Init(variables_dict):
	'''
	data_addr: variables_dict['DiscussionTree_save']
	'''
	# search if there is previously constructed discussion tree
	data_addr = variables_dict['DiscussionTree_save']
	src_addr = os.path.dirname(os.path.realpath('__file__'))
	search_addr = os.path.join(src_addr, data_addr)
	fileName = 'active_DiscussionTree.json'
	# get file names under search_addr
	fileName_list = [ f for f in listdir(search_addr) if isfile(join(search_addr, f)) ]
	
	# check if file exists
	if fileName in fileName_list:
		# old Discussion Tree exists
		fileName = os.path.join(search_addr, fileName)
		f = open(fileName,'r')
		text = f.read()
		f.close()
		# convert to json
		DiscussionTree = json.loads(text)
		return DiscussionTree		
	
	##############################
	# create new Discussion Tree #
	##############################	

	if fileName not in fileName_list:
		print "\nconstructing DiscussionTree\n"
		DiscussionTree = col.defaultdict()
		DiscussionTree['targetName_list'] = []
		DiscussionTree['targetID_list'] = []
		
		####################################################################
		# extract tweetIDs from list of targets
		for key in variables_dict['target2id_dict']:
			targetName = key
			targetID = variables_dict['target2id_dict'][key]
			DiscussionTree['targetName_list'].append(targetName)
			DiscussionTree['targetID_list'].append(targetID)
			
			####################################################################
			# tweetID list under target
			tweetID_list = ExtractFilter_tweetIDs(MySQL_DBkey=variables_dict['MySQL_key_ELINT'], 
												  targetName=targetName)
			
			####################################################################
			# load data of this target into DiscussionTree
			DiscussionTree[targetName] = col.defaultdict()
			# tweetID list by targetName
			DiscussionTree[targetName]['lv0_tweetID_list'] = dict()
			
			for tweetID in tweetID_list:
				DiscussionTree[targetName]['lv0_tweetID_list'][tweetID]=0
				# tweetID list associated with specific lv0_tweetID
				key_name = 'sub_'+tweetID
				DiscussionTree[targetName][key_name] = col.defaultdict()
				DiscussionTree[targetName][key_name]['lv1_tweetID_list'] = dict()
				DiscussionTree[targetName][key_name]['lv1_userID_list'] = dict()

			####################################################################
			# summary
			print "candidate %s has %i tweetIDs on record" % tuple( [targetName] + 
																	[len(DiscussionTree[targetName]['lv0_tweetID_list'])] 
																   )
		####################################################################
		# end of for key in variables_dict['target2id_dict']:
		# save Initial DiscussionTree
		json_name = search_addr+'clean_DiscussionTree.json'
		with open(json_name, 'w') as fp:
			json.dump(DiscussionTree, fp, sort_keys=False, indent=4)	
		return DiscussionTree

def DiscussionTree_save(variables_dict, DiscussionTree):
	'''
	'''
	data_addr = variables_dict['DiscussionTree_save']
	src_addr = os.path.dirname(os.path.realpath('__file__'))
	search_addr = os.path.join(src_addr, data_addr)
	json_name = search_addr+'active_DiscussionTree.json'
	with open(json_name, 'w') as fp:
		json.dump(DiscussionTree, fp, sort_keys=False, indent=4)	
	return None

"""
########################################################################################################

Main Function of Phase1 Part0

########################################################################################################
"""

def Phase1_Part1_Main(comd_json):
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

	# data file variables
	file_name_list = variables_dict['file_name_list']
	file_addr = variables_dict['file_addr']
	# time variables
	Start_Time = pd.to_datetime( variables_dict['Start_Time'] )
	# need to fix this
	Time_Period = np.timedelta64(3600*variables_dict['N_Time_Period'],'s')

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

	#####################
	# tables operations #
	#####################
	# create/open table TweetStack; CREATE TABLE IF NOT EXISTS
	tableName_TweetStack = TweetStack_Init(connection=connection, 
										   MySQL_DBkey=MySQL_DBkey, 
										   pin_time=Start_Time, 
										   variables_dict=variables_dict
										   )
	# convert TweetStack_TL from InnoDB to Memory
	convert2MEMORY(connection=connection, tableName=tableName_TweetStack)
	print "%s converted to MEMORY" % tableName_TweetStack

	# create/open table UserUnique; CREATE TABLE IF NOT EXISTS
	tableName_UserUnique = UserUnique_Init(connection=connection, 
										   MySQL_DBkey=MySQL_DBkey, 
										   pin_time=Start_Time, 
										   variables_dict=variables_dict
										   )
	# convert TweetStack_TL from InnoDB to Memory
	convert2MEMORY(connection=connection, tableName=tableName_UserUnique)
	print "%s converted to MEMORY" % tableName_UserUnique

	######################################
	# extract filters on userID and Tags #
	######################################

	# extract tags for set time from ELINT_transUltra
	tag_list = ExtractFilter_Tags(connection=connection, 
								  MySQL_DBkey=variables_dict['MySQL_key_transUltra'], 
								  pin_time=Start_Time)
	# list of userID_str
	userID_list = ExtractFilter_userIDs(variables_dict=variables_dict)

	#####################################
	# load or construct discussion tree #
	#####################################

	DiscussionTree = DiscussionTree_Init(variables_dict=variables_dict)

	########################################################################################################
	
	# main logic structure, controled by readline() returns, exist at end of file
	flag_fileEnd = True # to kick start the 1st file
	count_emptyline = 0
	count_line = 0
	count_tweet = 0
	pin_time = Start_Time

	#########################################
	# this is the logic loop for EACH tweet #
	#########################################
	file_index = 0
	file_Ntotal = len(file_name_list)
	while ( file_index < file_Ntotal ):
		
		##############################
		# for the start of each file #
		##############################
		if flag_fileEnd == True:
			flag_fileEnd = False
			file_input = DataFile_Iter(file_name_list=file_name_list, file_idex=file_index, file_addr=file_addr)
			count_line = 0
		
		count_line += 1 
		tweet_line = file_input.readline()

		if count_line % 1000 == 0:
			print "Tweets Analyzed: %i, Tweets Recorded: %i" % (count_line, count_tweet)
	
		############################################################
		# json format check and file end check and data type check #
		############################################################
		flag_line = False
		flag_json = False
		try:
			flag_line, flag_json = Json_format_check(tweet_line, count_line)
		except AssertionError:
			print "Line: {}, Json_format_check() data type check failed".format(index_line)
			pass 
		else:
			pass
		# count adjacent empty lines
		if flag_line == True:
			count_emptyline = 0
		else:
			count_emptyline += 1
		# flag end of file
		if count_emptyline > 4:
			flag_fileEnd = True
			file_index += 1

		########################################################################################################
		# Stage0
		# read JSON tweet, check conditions, (if) extract all infor into Tweet_OBJ, update pin_time
		# input/output RollingScoreBank; output Tweet_OBJ
		flag_TweetStack = False
		if flag_json == True:		
			##################################
			# load JSON, extract information #
			##################################
			flag_TweetStack, Tweet_OBJ, DiscussionTree = Ultra_JsonLoad(input_str=tweet_line, 
																		index_line=count_line, 
																		DiscussionTree=DiscussionTree,
																		tag_list=tag_list,
																		userID_list=userID_list, 
																		variables_dict=variables_dict)
			# if JSON extraction successful, check pin_time, check sliding_window
			flag_timeStamp = False
			if flag_TweetStack:
				# DataTypeCheck for pd.timestamp; compare and update pin_time
				try:
					flag_timeStamp = pd_timestamp_check(Tweet_OBJ['tweet_time'])
				except AssertionError:
					print "pin_time datatype failed"
					pass 
				else:
					pass
			
			##################
			# check pin_time #
			##################
			Delta_time = 0
			flag_new_window = False
			if flag_TweetStack and flag_timeStamp:
				Delta_time = (Tweet_OBJ['tweet_time'] - pin_time)/np.timedelta64(1,'s')
			# one Time_Period has past compared with pin_time
			N_Time_Period = Time_Period/np.timedelta64(1,'s')
			# the USUAL case
			if Delta_time >= N_Time_Period:
				pin_time = pin_time + Time_Period
				flag_new_window = True
			# if there is mismatch between time
			# if Delta_time <= -N_Time_Period:
			# 	pin_time = pin_time - Time_Period
			# 	flag_new_window = True				
		# end of if flag_json == True:
		########################################################################################################		
		# Stage1
		# check flag_new_window
		if flag_new_window == True:
			
			#####################
			# tables operations #
			#####################
			# convert old tweetStack
			convert2InnoDB(connection=connection, tableName=tableName_TweetStack)
			print "%s converted to InnoDB" % tableName_TweetStack
			# convert old UserUnique
			convert2InnoDB(connection=connection, tableName=tableName_UserUnique)
			print "%s converted to InnoDB" % tableName_UserUnique
			# new table Names
			# create/open/convert new tweetStack
			tableName_TweetStack = TweetStack_Init(connection=connection, 
												   MySQL_DBkey=MySQL_DBkey, 
												   pin_time=pin_time, 
												   variables_dict=variables_dict
												   )
			convert2MEMORY(connection=connection, tableName=tableName_TweetStack)
			print "%s converted to MEMORY" % tableName_TweetStack
			# create/open/convert new UserUnique
			tableName_UserUnique = UserUnique_Init(connection=connection, 
												   MySQL_DBkey=MySQL_DBkey, 
												   pin_time=pin_time, 
												   variables_dict=variables_dict
												   )
			convert2MEMORY(connection=connection, tableName=tableName_UserUnique)
			print "%s converted to MEMORY" % tableName_UserUnique

			###############################
			# extract new filters on tags #
			###############################

			# extract tags for set time from ELINT_transUltra
			tag_list = ExtractFilter_Tags(connection=connection, 
										  MySQL_DBkey=variables_dict['MySQL_key_transUltra'], 
										  pin_time=pin_time)

		########################################################################################################
		# Stage2
		if flag_TweetStack:
			count_tweet += 1
			# upload TweetStack
			TweetStack_load(connection=connection, Tweet_OBJ=Tweet_OBJ, tableName=tableName_TweetStack)
			# upload UserUnique
			UserUnique_load(connection=connection, Tweet_OBJ=Tweet_OBJ, tableName=tableName_UserUnique)

	# end of while ( file_index < file_Ntotal )
	########################################################################################################

	# convert whatever in the RAM to InnoDB
	convert2InnoDB(connection=connection, tableName=tableName_TweetStack)
	print "%s converted to InnoDB" % tableName_TweetStack
	# convert old UserUnique
	convert2InnoDB(connection=connection, tableName=tableName_UserUnique)
	print "%s converted to InnoDB" % tableName_UserUnique

	DiscussionTree_save(variables_dict=variables_dict, DiscussionTree=DiscussionTree)
	connection.close()
	
	####################################################################
	# convert Timestamp to string
	variables_dict['Start_Time'] = str( Start_Time )
	# saving model_options to .json file
	json_name = 'P1P2_ExecutedComds.json'
	with open(json_name, 'w') as fp:
		json.dump(variables_dict, fp, sort_keys=True, indent=4)	
	return None

"""
########################################################################################################

# Execution of Phase1_Part1

########################################################################################################
"""

if __name__ == "__main__":

	Phase1_Part1_Main(comd_json='P1P2_TrackKeywords_DF_May20.json')


