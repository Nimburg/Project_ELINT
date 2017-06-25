

import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors


####################################################################
# variable type check
def check_args(*types):
	def real_decorator(func):
		def wrapper(*args, **kwargs):
			for val, typ in zip(args, types):
				assert isinstance(val, typ), "Value {} is not of expected type {}".format(val, typ)
			return func(*args, **kwargs)
		return wrapper
	return real_decorator

# single input pd.timestamp
@check_args(pd.tslib.Timestamp)
def pd_timestamp_check(input):
	return True

"""
###########################################################################################################

Selected Tweets Table

###########################################################################################################
"""

def Selected_TweetStack_Init(connection, MySQL_DBkey, key_userName, variables_dict):
	'''
	key_userName: media userName
	return: tableName

	Check if exists, else create new TweetStack table for Selected Tweets
	'''
	# tableName = TweetStack_TL_St_userName
	tableName = variables_dict['load_type']+key_userName
	db_name = MySQL_DBkey['db']
	
	##########################################
	# check if TweetStack_TL_ already exists #
	##########################################
	# comd
	comd_table_check = """
SELECT IF( 
(SELECT count(*) FROM information_schema.tables
WHERE table_schema = '%s' AND table_name = '%s'), 1, 0);"""
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute( comd_table_check % tuple( [db_name] + [tableName] )
						  )
			result = cursor.fetchall()
			#print result
			for key in result[0]:
				pin = result[0][key]
			#print pin
	finally:
		pass
	# load results
	if pin == 1:
		print "%s exists." % tableName 
		return tableName
	################
	# create table #
	################	
	else:
		#Comd
		Comd_TweetStack_Init = """
DROP TABLE IF EXISTS %s;
CREATE TABLE IF NOT EXISTS %s
(
	tweetID BIGINT PRIMARY KEY NOT NULL,
	tweetTime TIMESTAMP NOT NULL,
	userID BIGINT NOT NULL,
	userName varchar(300),
	Origin_userID BIGINT NOT NULL,
	Origin_userName varchar(300),
	retweet_count BIGINT,
	favorite_count BIGINT,
	OriginalText varchar(1000) COLLATE utf8_bin,
	ProcessedText varchar(1000) COLLATE utf8_bin,
	Concerned_Keywords varchar(1000),
	NP_list varchar(3000) COLLATE utf8_bin,
	MenUserList_text varchar(1000),
	TagList_text varchar(1000),
	lang varchar(100),
	coordinates varchar(300)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"""
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute( Comd_TweetStack_Init % tuple( [tableName]+[tableName] ) )
			# commit commands
			print tableName+" Initialized"
			connection.commit()
		finally:
			pass
		return tableName

###########################################################################################################

def Extract_Original_userName(sentence_input):
	# extract original userName from RT
	word_list = sentence_input.split(' ')
	if (word_list[0] == 'rt') and ('@' in word_list[1]):
		return word_list[1]
	else:
		return None

###########################################################################################################

def Selected_Tweets_Extract(connection, MySQL_DBkey, key_userName, variables_dict):
	'''
	key_userName: media userName

	return: list of dicts, each dict as a tweet
	could return None is table doesn't exist. 
	'''
	# tableName = TweetStack_TL_userName
	tableName = variables_dict['extract_type']+key_userName
	db_name = MySQL_DBkey['db']
	
	##############################################
	# check if TweetStack to be extracted exists #
	##############################################
	# comd
	comd_table_check = """
SELECT IF( 
(SELECT count(*) FROM information_schema.tables
WHERE table_schema = '%s' AND table_name = '%s'), 1, 0);"""
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute( comd_table_check % tuple( [db_name] + [tableName] )
						  )
			result = cursor.fetchall()
			#print result
			for key in result[0]:
				pin = result[0][key]
			#print pin
	finally:
		pass
	# load results
	if pin == 0:
		print "%s does NOT exists." % tableName 
		return None

	##################
	# Extract Tweets #
	##################
	# command for Selecting Tweets
	comd_Select_Tweets = """
SELECT tweetID,tweetTime,userID,tweetText,retweet_count,favorite_count,MenUserList_text,TagList_text,lang,coordinates
FROM %s;"""

	# Results Data Structure
	Selected_Tweets_list = [] # list of dicts
	# Execute Comds
	print "extracting Selected Tweets from %s" % tableName
	try:
		with connection.cursor() as cursor:
			cursor.execute( comd_Select_Tweets % tableName )
			result = cursor.fetchall()
			# loop through all rows of this table
			for entry in result:
				# dict of all information
				Tweet_Infor = dict()
				# direct str()
				Tweet_Infor['tweetID'] = str(entry['tweetID'])
				Tweet_Infor['tweetTime'] = str(entry['tweetTime'])
				Tweet_Infor['userID'] = str(entry['userID'])
				# take as int
				Tweet_Infor['retweet_count'] = entry['retweet_count']
				Tweet_Infor['favorite_count'] = entry['favorite_count']
				# direct str()
				Tweet_Infor['MenUserList_text'] = str(entry['MenUserList_text'])
				Tweet_Infor['TagList_text'] = str(entry['TagList_text'])
				Tweet_Infor['lang'] = str(entry['lang'])
				Tweet_Infor['coordinates'] = str(entry['coordinates'])
				
				# add userName
				Tweet_Infor['userName'] = ''
				if Tweet_Infor['userID'] in variables_dict['id2target_dict']:
					Tweet_Infor['userName'] = variables_dict['id2target_dict'][Tweet_Infor['userID']]

				# tweetText
				Tweet_Infor['OriginalText'] = str(entry['tweetText']).lower().decode("utf-8").encode('ascii','ignore')	
				
				# Origin_userID, Origin_userName
				Tweet_Infor['Origin_userID'] = 0
				Tweet_Infor['Origin_userName'] = ''				
				# extract original userName and userID from RT
				temp_userName = Extract_Original_userName(sentence_input=Tweet_Infor['OriginalText'])
				if temp_userName is not None:
					Tweet_Infor['Origin_userName'] = temp_userName[1:] # rid of @
					if Tweet_Infor['Origin_userName'] in variables_dict['target2id_dict']:
						Tweet_Infor['Origin_userID'] = variables_dict['target2id_dict'][ Tweet_Infor['Origin_userName'] ]

				# ProcessedText
				Tweet_Infor['ProcessedText'] = '' # default
				# NP_list
				Tweet_Infor['NP_list'] = [] # default

				# append to Selected_Tweets_list
				Selected_Tweets_list.append(Tweet_Infor)
			# end of table result
	finally:
		pass
	# end of extraction
	return Selected_Tweets_list

###########################################################################################################

def Selected_TweetStack_load(connection, Selected_Tweets_list, tableName):
	'''
	Selected_Tweets_list: list of dicts. 

	target table has been opened. tableName given. Directly Insert.

	'''
	# command for Tweet_Stack
	comd_TweetStack_Insert = """
INSERT INTO %s (tweetID, tweetTime, userID, userName, Origin_userID, Origin_userName, \
retweet_count, favorite_count, \
OriginalText, ProcessedText, Concerned_Keywords, \
NP_list, MenUserList_text, TagList_text, lang, coordinates)
VALUES ( %s, '%s', %s, '%s', %s, '%s', \
%i, %i, \
'%s', '%s', '%s', \
'%s', '%s', '%s', '%s', '%s')
ON DUPLICATE KEY UPDATE userID = %s;"""	
	
	# go through Selected_Tweets_list and Insert into table
	for Tweet_Infor in Selected_Tweets_list:
		# convert Tweet_Infor['NP_list']
		NP_list_str = ''
		if len(Tweet_Infor['NP_list']) > 0:
			NP_list_str = ",NP,".join(Tweet_Infor['NP_list'])
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute( comd_TweetStack_Insert % tuple( [tableName] +
																[Tweet_Infor['tweetID']] + 
																[Tweet_Infor['tweetTime']] + 
																[Tweet_Infor['userID']] +
																[Tweet_Infor['userName']] +
																[Tweet_Infor['Origin_userID']] +
																[Tweet_Infor['Origin_userName']] +
																[Tweet_Infor['retweet_count']] +
																[Tweet_Infor['favorite_count']] +
																[Tweet_Infor['OriginalText']] +
																[Tweet_Infor['ProcessedText']] +
																[Tweet_Infor['Concerned_Keywords']] + 
																[NP_list_str] + 
																[Tweet_Infor['MenUserList_text']] + 
																[Tweet_Infor['TagList_text']] +
																[Tweet_Infor['lang']] +
																[Tweet_Infor['coordinates']] +
																[Tweet_Infor['userID']]
															   )
							  )
			# commit commands 
			connection.commit()
		except pymysql.err.InternalError:
			pass
		finally:
			pass
		# end of each Tweet_Infor
	# end of Selected_Tweets_list

	return None


















