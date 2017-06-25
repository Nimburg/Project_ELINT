

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

###########################################################################################################

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
###########################################################################################################

TweetStack tables

###########################################################################################################
"""

def TweetStack_Init(connection, MySQL_DBkey, pin_time, variables_dict):
	'''
	pin_time: pd.timestamp
	'''
	# table Name
	tableName = variables_dict['load_type']+"TweetStack"+pin_time.strftime('_%Y_%m_%d_%H')
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
	tweetText varchar(1000) COLLATE utf8_bin,
	reply_user_id BIGINT,
	in_reply_to_status_id BIGINT,
	quoted_status_id BIGINT,
	retweet_count int,
	favorite_count int, 
	MenUserList_text varchar(1000),
	TagList_text varchar(1000),
	lang varchar(300),
	coordinates varchar(300),
	Discussion_userName varchar(300),
	Discussion_tweetID BIGINT,
	Concern_userName varchar(1000)
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

def TweetStack_load(connection, Tweet_OBJ, tableName):

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
	if len( Tweet_OBJ['mentioned_userID'] ) > 0: 
		for men_user in Tweet_OBJ['mentioned_userID']:
			if men_user is None:
				continue
			try:
				if men_user.isdigit():
					mentioned_userID_str += men_user + ','
			except AttributeError:
				pass
		if len(mentioned_userID_str) > 1:
			mentioned_userID_str = mentioned_userID_str[:-1] # for the last ','
		
	# parse tagList
	tagList_str = ""
	if len( Tweet_OBJ['Tag'] ) > 0:
		for tag_key in Tweet_OBJ['Tag']:
			tagList_str += tag_key + ','
		if len(tagList_str) > 1:
			tagList_str = tagList_str[:-1]

	# parse  Concern_userName
	Concern_userName_str = ""
	if len( Tweet_OBJ['Concern_userName'] ) > 0:
		for userName in Tweet_OBJ['Concern_userName']:
			Concern_userName_str += userName + ','
		if len(Concern_userName_str) > 1:
			Concern_userName_str = Concern_userName_str[:-1]

	####################################################################
	if flag_timeStamp and flag_id_str:
		# command for Tweet_Stack
		comd_TweetStack_Insert = """
INSERT INTO %s (tweetID, tweetTime, userID, tweetText, reply_user_id, in_reply_to_status_id, \
quoted_status_id, retweet_count, favorite_count, MenUserList_text, TagList_text, lang, coordinates,\
Discussion_userName, Discussion_tweetID, Concern_userName)
VALUES ( %s, '%s', %s, '%s', %s, %s, \
%s, %i, %i, '%s', '%s', '%s', '%s',\
'%s', %s, '%s')
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
																[Tweet_OBJ['Discussion_userName']] + 
																[Tweet_OBJ['Discussion_tweetID']] + 
																[Concern_userName_str] +
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

"""
###########################################################################################################

UserUnique tables

###########################################################################################################
"""

def UserUnique_Init(connection, MySQL_DBkey, pin_time, variables_dict):
	'''
	'''

	# table Name
	tableName = variables_dict['load_type']+"UserUnique"+pin_time.strftime('_%Y_%m_%d_%H')
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
	else:
	################
	# create table #
	################		
		#Comd
		Comd_UserUnique_Init = """
CREATE TABLE IF NOT EXISTS %s
(
	userID bigint PRIMARY KEY NOT NULL,
	userName varchar(255),
	user_verified int,
	followers int,
	friends int,
	favourites int,
	listed int,
	statuses int
)ENGINE=MEMORY;"""
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute( Comd_UserUnique_Init % tableName )
			# commit commands
			print tableName+" Initialized"
			connection.commit()
		finally:
			pass
	# end
	return tableName

###########################################################################################################

def UserUnique_load(connection, Tweet_OBJ, tableName):

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

	####################################################################
	if flag_timeStamp and flag_id_str:
		# command for Tweet_Stack
		comd_UserUnique_Insert = """
INSERT INTO %s (userID, userName, user_verified, followers, friends, favourites, listed, statuses)
VALUES (%s, '%s', %i, %i, %i, %i, %i, %i)
ON DUPLICATE KEY UPDATE statuses = %i;"""
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute( comd_UserUnique_Insert % tuple( [tableName] +
																[Tweet_OBJ['user_id']] + 
																[Tweet_OBJ['user_name']] + 
																[Tweet_OBJ['user_verified']] + 
																[Tweet_OBJ['user_followers']] + 
																[Tweet_OBJ['user_friends']] + 
																[Tweet_OBJ['user_favourites']] +
																[Tweet_OBJ['user_listed']] +
																[Tweet_OBJ['user_statuses']] +
																[Tweet_OBJ['user_statuses']] 
															   )
							  )
			# commit commands 
			connection.commit()
		except pymysql.err.InternalError:
			pass
		finally:
			pass

	return None

"""
###########################################################################################################

Extract Filters

###########################################################################################################
"""

def ExtractFilter_Tags(connection, MySQL_DBkey, pin_time):
	'''
	MySQL_DBkey: ELINT_transUltra
	'''
	db_name = MySQL_DBkey['db']
	# tablename
	table_name = 'combined_tags'+pin_time.strftime('%Y_%m_%d_%H')
	tag_list = []
	# comd
	comd_extract = """
SELECT tag
FROM %s.%s;"""
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute( comd_extract % tuple( [db_name] + [table_name] )
						  )
			result = cursor.fetchall()
			# loop through all rows of this table
			for entry in result:
				# {u'tag': u'hillary2016forsanity', u'tag_Ncall': 1}
				tag = str( entry['tag'] ).decode('utf-8').encode('ascii', 'ignore')	
				tag_list.append(tag)
	except pymysql.err.ProgrammingError:
		pass
	finally:
		pass
	# convert tag_list to a dict()
	tag_list_dict = dict()
	for tag in tag_list:
		tag_list_dict[tag] = 0
	# end
	return tag_list

def ExtractFilter_tweetIDs(MySQL_DBkey, targetName):
	'''
	MySQL_DBkey: ELINT_Phase1
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

	db_name = MySQL_DBkey['db']
	# tablename
	table_name = 'tweetstack_tl_'+targetName
	tweetID_list = []
	# comd
	comd_extract = """
SELECT tweetID
FROM %s.%s;"""
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute( comd_extract % tuple( [db_name] + [table_name] )
						  )
			result = cursor.fetchall()
			# loop through all rows of this table
			for entry in result:
				# str( SQL_bigint)
				tweetID = str( entry['tweetID'] )
				tweetID_list.append( tweetID )
	finally:
		pass
	connection.close()
	return tweetID_list

###########################################################################################################

def ExtractFilter_userIDs(variables_dict):
	'''
	MySQL_DBkey: ELINT_Phase1
	'''
	# list of userID_str
	userID_list = []
	for key in variables_dict['id2target_dict']:
		userID_list.append(key)
	return userID_list












