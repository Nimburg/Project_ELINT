

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

TweetStack tables

###########################################################################################################
"""

def TweetStack_TL_Init(MySQL_DBkey, userName):
	'''
	pin_time: pd.timestamp
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

	# table Name
	tableName = "TweetStack_TL_"+userName
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
CREATE TABLE IF NOT EXISTS %s
(
	tweetID BIGINT PRIMARY KEY NOT NULL,
	tweetTime TIMESTAMP NOT NULL,
	userID BIGINT NOT NULL,
	tweetText varchar(3000) COLLATE utf8_bin,
	reply_user_id BIGINT,
	in_reply_to_status_id BIGINT,
	quoted_status_id BIGINT,
	retweet_count BIGINT,
	favorite_count BIGINT, 
	MenUserList_text varchar(3000),
	TagList_text varchar(3000),
	lang varchar(300),
	coordinates varchar(3000)	
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"""
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute( Comd_TweetStack_Init % tableName )
			# commit commands
			print tableName+" Initialized"
			connection.commit()
		finally:
			pass
		return tableName

###########################################################################################################

def TweetStack_TL_convert2InnoDB(connection, tableName):

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

def TweetStack_TL_convert2MEMORY(connection, tableName):

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

###########################################################################################################

def TweetStack_TL_load(connection, Tweet_OBJ, tableName):

	# DataTypeCheck for pd.timestamp; compare and update pin_time
	flag_timeStamp = False	
	try:
		flag_timeStamp = pd_timestamp_check( Tweet_OBJ['tweet_time'] )
	except AssertionError:
		print "pin_time datatype failed"
		pass 
	else:
		pass
	
	# id_str check
	flag_id_str = False
	if Tweet_OBJ['tweet_id'].isdigit() and Tweet_OBJ['user_id'].isdigit():
		flag_id_str = True
	
	# mentioned_userID check and parse
	mentioned_userID_str = ""
	for men_user in Tweet_OBJ['mentioned_userID']:
		if men_user.isdigit():
			mentioned_userID_str += men_user + ','
	if len(mentioned_userID_str) > 1:
		mentioned_userID_str = mentioned_userID_str[:-1] # for the last ','
		
	# parse tagList
	tagList_str = ""
	for tag_key in Tweet_OBJ['Tag']:
		tagList_str += tag_key + ','
	if len(tagList_str) > 1:
		tagList_str = tagList_str[:-1]

	####################################################################
	if flag_timeStamp and flag_id_str:
		# command for Tweet_Stack
		comd_TweetStack_Insert = """
INSERT INTO %s (tweetID, tweetTime, userID, tweetText, reply_user_id, in_reply_to_status_id, \
quoted_status_id, retweet_count, favorite_count, MenUserList_text, TagList_text, lang, coordinates)
VALUES ( %s, '%s', %s, '%s', %s, %s, \
%s, %i, %i, '%s', '%s', '%s', '%s')
ON DUPLICATE KEY UPDATE userID = %s;"""
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute( comd_TweetStack_Insert % tuple( [tableName] +
																[Tweet_OBJ['tweet_id']] + 
																[str(Tweet_OBJ['tweet_time'])] + 
																[Tweet_OBJ['user_id']] + 
																[Tweet_OBJ['text']] + 
																[Tweet_OBJ['reply_to_userID']] + 
																[Tweet_OBJ['in_reply_to_status_id']] +
																[Tweet_OBJ['quoted_status_id']] +
																[Tweet_OBJ['retweet_count']] +
																[Tweet_OBJ['favorite_count']] + 
																[mentioned_userID_str] + 
																[tagList_str] + 
																[Tweet_OBJ['lang']] + 
																[Tweet_OBJ['coordinates']] +
																[Tweet_OBJ['user_id']]
															   )
							  )
			# commit commands 
			connection.commit()
		except pymysql.err.InternalError:
			pass
		finally:
			pass

	return None

###########################################################################################################
