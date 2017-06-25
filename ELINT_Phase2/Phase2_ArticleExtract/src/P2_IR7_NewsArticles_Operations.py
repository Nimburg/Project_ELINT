#!/usr/bin/env python


import json
import os
import numpy as np 
import pandas as pd
import time
import collections as col

import pymysql.cursors

from P2_IR7_Alchemy import Alchemy_Extract_Article


"""
###############################################################################################################

Utilities

###############################################################################################################
"""

# create filteredNews_ tables at ELINT_Phase2 db
def filteredNews_init(variables_dict, MySQL_DBkey):
	'''
	------
	table columns:
	tweetID, tweetTime, userID, userName, tweetText, MenUserList_text, TagList_text, \
	title, author, twitter_url, root_url
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
	# filteredNews_+'China'
	tableName = variables_dict['filtered_newsTable_header']+variables_dict['tableName_timestamp']
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
		# Comd
		Comd_Init = """
CREATE TABLE IF NOT EXISTS %s
(
	id MEDIUMINT NOT NULL AUTO_INCREMENT,
	PRIMARY KEY (id),
	tweetID BIGINT NOT NULL,
	tweetTime TIMESTAMP NOT NULL,
	userID BIGINT NOT NULL,
	userName varchar(1000),
	filterName varchar(1000),
	tweetText varchar(1000) COLLATE utf8_bin,
	MenUserList_text varchar(1000),
	TagList_text varchar(1000),
	title varchar(1000), 
    author varchar(1000), 
    root_url varchar(1000)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"""
	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute( Comd_Init % tableName )
		# commit commands
		print tableName+" Initialized"
		connection.commit()
	finally:
		pass

	####################################################################
	connection.close()
	return tableName

###############################################################################################################

def twt_url_parser(tweetText):
	'''
	parse and return a list of twt_url, parsed from tweetText
	make sure remove any ' ' from twt_url
	'''
	# for the last idx pair
	tweetText = tweetText + ' ' 
	key_str = 'https'
	# url idx list, beginnings, plus -1
	idx_list = []
	start = 0
	twt_url_list = []
	while start < len(tweetText):
		index = tweetText.find(key_str, start)
		if index != -1:
			idx_list.append(index)
			start = index + 1
		if index == -1:
			break
	# for the last idx pair
	idx_list.append(-1)
	# generate twt_url_list
	for idx in range(len(idx_list)-1):
		twt_url_list.append( tweetText[ idx_list[idx]:idx_list[idx+1] ] )
	# rid of the _ of twt_url
	for idx in range(len(twt_url_list)):
		new_url = ''
		for ch in twt_url_list[idx]:
			if ch != ' ':
				new_url += ch
		twt_url_list[idx] = new_url
	# returns
	return twt_url_list

###############################################################################################################

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

###############################################################################################################

def Existing_Articles_tweetIDs(variables_dict):
	'''
	To save API and Time by passing Articles that has been extracted
	search in addr: variables_dict['Articles_save_addr'] 
	get tweetID from .json files
	'''
	# get list of json file names
	path_from = variables_dict['Articles_save_addr'] 
	fileName_list = [ f for f in os.listdir(path_from) if f[-5:]=='.json' ]
	print "Number of files found: %i" % len(fileName_list)
	
	# get tweetID
	tweetID_set = set() # set of ID_str
	for file in fileName_list:
		temp_id = None
		# open file
		file_extract = path_from + file
		f = open(file_extract,'r')
		text = f.read()
		f.close()
		# get json value
		try: 
			text_json = json.loads(text)
			temp_id = text_json['tweetID']
		except ValueError:
			pass
		except KeyError:
			pass
		finally:
			pass
		# append
		if temp_id is not None:
			tweetID_set.add(temp_id)
	# end of for file in fileName_list:
	return tweetID_set

"""
###############################################################################################################

Filter Tweets, Extract tweetText, and parse url

###############################################################################################################
"""

def Filter_Extract_MediasTweets(variables_dict, filter_key):
	'''
	On a per-filter-key basis
	Only changing variable is Media
	------
	go through each Media,
	
	create Article_Infor_dictionary per Key-basis
	(title, author, root_url)

	for each extracted_tweet, 
		check for correct url, update Article_Infor_dictionary
		extract text from Alchemy using url, update Article_Infor_dictionary
		saving extracted text after checking Article_Infor_dictionary for redundency
		dump tweet_data, (if) correct url into ELINT_Phase2 table
		dump Article_Infor_dictionary as .JSON
	------
	filter_key: e.g. 'filter_China_keywords'
	------
	returns:
	Article_Infor_dictionary:

	'''
	# filter_key is 'filter_China_keywords'
	filter_name = filter_key[7:-9]
	filter_keyList = variables_dict[filter_key]

	# UTL_db information
	UTL_db_dict = variables_dict['MySQL_key_ELINT']
	# filtered_Medias_tweets db information
	filteredNews_db_dict = variables_dict['MySQL_key_ELINT_P2']
	
	####################################################################
	# Connect to the database
	connection = pymysql.connect(host=UTL_db_dict['host'],
								 user=UTL_db_dict['user'],
								 password=UTL_db_dict['password'],
								 db=UTL_db_dict['db'],
								 charset=UTL_db_dict['charset'],
								 cursorclass=pymysql.cursors.DictCursor)

	####################################################################

	##############################################################################################

	# Article_Infor_dictionary is on per-filter level
	# Article_Infor_dictionary has key: tweetID
	#
	# medias_tweet information: tweetID, tweetTime, userID, userName, tweetText, MenUserList_text, TagList_text
	# article information: (title, author, root_url)
	# saved_file information: infor_json, txt,
	# post_Alchemy_json, post_coreNLP_json <--- not generated here
	Article_Infor_dictionary = col.defaultdict()

	##############################################################################################
	
	#################################################
	# Go through List of Media for given filter_key #
	#################################################
	# key of dict
	for key in variables_dict['media2id_dict']:
		media_name = key

		#######################
		# Apply Filter to UTL #
		#######################
		# db and table names
		UTL_db_name = UTL_db_dict['db']
		UTL_talbeName = variables_dict['UTL_tableName_header']+media_name
		UTL_query_timeStamp = variables_dict['query_timestamp']

		# Command_raw
		Comd_filteredNews_raw = """
SELECT tweetID, tweetTime, userID, tweetText, MenUserList_text, TagList_text
FROM %s.%s
Where tweetTime >= '%s' and ( %s );"""
		flter_raw = """ tweetText like "%s" """
		filter_full_str = ""
		# go through filter_keyList
		for idx in range(len(filter_keyList)):
			filter_full_str += flter_raw % ('%'+filter_keyList[idx]+'%')
			if idx != len(filter_keyList)-1:
				filter_full_str += 'or'
		# full command
		Comd_filteredNews = Comd_filteredNews_raw % (UTL_db_name, UTL_talbeName, UTL_query_timeStamp, filter_full_str)
		# Execute Comds
		print "extracting filtered Medias News from %s.%s" % (UTL_db_name, UTL_talbeName)
		print "Filter Name: %s Media Name: %s" % (filter_name, media_name)
		try:
			with connection.cursor() as cursor:
				cursor.execute( Comd_filteredNews )
				result = cursor.fetchall()
				# loop through all rows of this table
				for entry in result:
					# {u'tag': u'hillary2016forsanity', u'tag_Ncall': 1}
					# in MySQL, text format is utf8, but in raw data as ASCII
					# tweetID, tweetTime, userID, tweetText, MenUserList_text, TagList_text
					tweetID = str( entry['tweetID'] ).decode('utf-8').encode('ascii', 'ignore')
					tweetTime = str( entry['tweetTime'] ).decode('utf-8').encode('ascii', 'ignore')
					userID = str( entry['userID'] ).decode('utf-8').encode('ascii', 'ignore')
					tweetText = str( entry['tweetText'] ).decode('utf-8').encode('ascii', 'ignore')
					MenUserList_text = str( entry['MenUserList_text'] ).decode('utf-8').encode('ascii', 'ignore')
					TagList_text = str( entry['TagList_text'] ).decode('utf-8').encode('ascii', 'ignore')

					# add to Article_Infor_dictionary
					Article_Infor_dictionary[tweetID] = col.defaultdict()
					Article_Infor_dictionary[tweetID]['tweetID'] = tweetID
					Article_Infor_dictionary[tweetID]['tweetTime'] = tweetTime
					Article_Infor_dictionary[tweetID]['userID'] = userID
					Article_Infor_dictionary[tweetID]['userName'] = media_name
					Article_Infor_dictionary[tweetID]['filterName'] = filter_name
					Article_Infor_dictionary[tweetID]['tweetText'] = tweetText
					Article_Infor_dictionary[tweetID]['MenUserList_text'] = MenUserList_text
					Article_Infor_dictionary[tweetID]['TagList_text'] = TagList_text

				# end of for entry in result:
		except pymysql.err.ProgrammingError:
			print "\n\n### Command has Problems ###\n\n"
			#print Comd_filteredNews
			pass
		finally:
			pass
	
	# end of for key in variables_dict['media2id_dict']
	# end of media list
	connection.close()
	print "\nNumber of Tweets extracted: %i" % len(Article_Infor_dictionary)
	##############################################################################################

	# create tweetID_set from existing .json files
	tweetID_set = Existing_Articles_tweetIDs(variables_dict=variables_dict)
	print "\nNumber of TweetIDs by Existing Articles: %i" % len(tweetID_set)

	# create key list; key as tweetID
	API_counter = 0
	key_list = []
	for key in Article_Infor_dictionary:
		# check against tweetID_set from existing .json files
		if key not in tweetID_set:
			# append key (tweetID)
			key_list.append(key) 
	print "\nNumber of Tweets going into Alchemy API: %i" % len(key_list)	
	
	# go through each tweet in Article_Infor_dictionary
	for key in key_list:
		media_name = Article_Infor_dictionary[key]['userName']
		filter_name = Article_Infor_dictionary[key]['filterName']

		########################################
		# parse list of twt_url from tweetText #
		########################################
		# list of string
		# due to repeated tweetIDs and dict.pop()
		try:
			twt_url_list = twt_url_parser(tweetText=Article_Infor_dictionary[key]['tweetText'])
		except KeyError:
			print "already poped tweetID %s " % key
			pass
		finally:
			pass

		########################################################
		# Extract Title/Author/root_url/Full_Txt using Alchemy #
		########################################################
		flag_has_validurl = False
		for item in twt_url_list: 
			# call Alchemy API
			API_counter += 1
			print "Alchemy API Call: %i" % API_counter

			#if API_counter == 10:
			#	return Article_Infor_dictionary

			Alchemy_Results = Alchemy_Extract_Article(url_str=item)
			# check status
			if Alchemy_Results['status'] == 'OK':
				flag_has_validurl = True
				# update Article_Infor_dictionary
				Article_Infor_dictionary[key]['title'] = Alchemy_Results['title'].decode('utf-8').encode('ascii', 'ignore')
				Article_Infor_dictionary[key]['root_url'] = Alchemy_Results['root_url'].decode('utf-8').encode('ascii', 'ignore')
				# Alchemy_Results['author'] is a list
				Article_Infor_dictionary[key]['author'] = str( Alchemy_Results['author'] )

				#################################
				# save full_text and infor_json #
				#################################
				src_addr = os.path.dirname(os.path.realpath(__file__))
				save_addr = os.path.join(src_addr, variables_dict['Articles_save_addr']) 
				# full file names
				fileName_text=save_addr+filter_name+'_'+media_name+'_'+Article_Infor_dictionary[key]['title']+'.txt'
				fileName_json=save_addr+filter_name+'_'+media_name+'_'+Article_Infor_dictionary[key]['title']+'_infor.json'
				# write .txt
				try:
					with open(fileName_text, 'w') as text_file:
						text_file.write( Alchemy_Results['full_text'] )	
					# write json
					with open(fileName_json, 'w') as json_file:
						json.dump(Article_Infor_dictionary[key], json_file, sort_keys=True, indent=4)
				except IOError:
					print "bad file name"
					pass
				finally:
					pass						
		# end of for item in twt_url_list: 
		# check flag_has_validurl and delete entry
		if flag_has_validurl == False:
			Article_Infor_dictionary.pop(key, None)
	# end of for key in Article_Infor_dictionary:
	print "\nNumber of Tweets with valid url: %i" % len(Article_Infor_dictionary)
	##############################################################################################
	return Article_Infor_dictionary

"""
###############################################################################################################

Save FilteredNews tweets into filteredNews_table

###############################################################################################################
"""

def Load_MediasTweets(variables_dict, filteredNews_tweetStack):
	'''
	'''
	# filtered_Medias_tweets db information
	filteredNews_db_dict = variables_dict['MySQL_key_ELINT_P2']
	db_name = variables_dict['MySQL_key_ELINT_P2']['db']
	# flteredNews_tableName: flteredNews_'timeStamp'
	flteredNews_tableName = variables_dict['filtered_newsTable_header']+variables_dict['tableName_timestamp']
	
	####################################################################
	# Connect to the database
	connection = pymysql.connect(host=filteredNews_db_dict['host'],
								 user=filteredNews_db_dict['user'],
								 password=filteredNews_db_dict['password'],
								 db=filteredNews_db_dict['db'],
								 charset=filteredNews_db_dict['charset'],
								 cursorclass=pymysql.cursors.DictCursor)

	####################################################################

	# convert to ram
	convert2MEMORY(connection=connection, tableName=flteredNews_tableName)
	print "%s converted to RAM table" % flteredNews_tableName

	####################################################################

	# root command
	comd_Insert = """
INSERT INTO %s.%s (tweetID, tweetTime, userID, userName, filterName, \
tweetText, MenUserList_text, TagList_text, \
title, author, root_url)
VALUES ( %s, '%s', %s, '%s', '%s', \
'%s', '%s', '%s', \
'%s', '%s', '%s');"""	

	# through filteredNews_tweetStack by key(filter_name)
	for key_filter in filteredNews_tweetStack:
		# through filteredNews_tweetStack[filter_name] by key(tweetID)
		for key_ID in filteredNews_tweetStack[key_filter]:
			flag_execute = True

			#######################
			# extract information #
			#######################
			try:
				tweetID = filteredNews_tweetStack[key_filter][key_ID]['tweetID']
				tweetTime = filteredNews_tweetStack[key_filter][key_ID]['tweetTime']			
				userID = filteredNews_tweetStack[key_filter][key_ID]['userID']		
				userName = filteredNews_tweetStack[key_filter][key_ID]['userName']
				filterName = filteredNews_tweetStack[key_filter][key_ID]['filterName']
				tweetText = filteredNews_tweetStack[key_filter][key_ID]['tweetText']
				MenUserList_text = filteredNews_tweetStack[key_filter][key_ID]['MenUserList_text']
				TagList_text = filteredNews_tweetStack[key_filter][key_ID]['TagList_text']
				title = filteredNews_tweetStack[key_filter][key_ID]['title']
				author = filteredNews_tweetStack[key_filter][key_ID]['author']
				root_url = filteredNews_tweetStack[key_filter][key_ID]['root_url']
			except KeyError:
				flag_execute = False
				pass
			finally:
				pass

			####################
			# execute commands #
			####################
			if flag_execute == True:
				try:
					with connection.cursor() as cursor:
						cursor.execute( comd_Insert % tuple( [db_name]+
															 [flteredNews_tableName]+
															 [tweetID]+
															 [tweetTime]+
															 [userID]+
															 [userName]+
															 [filterName]+
															 [tweetText]+
															 [MenUserList_text]+
															 [TagList_text]+
															 [title]+
															 [author]+
															 [root_url]
															)
									  )
					# commit commands 
					connection.commit()
				except pymysql.err.InternalError:
					pass
				except pymysql.err.ProgrammingError:
					pass
				finally:
					pass
		# end of for key_ID in filteredNews_tweetStack[key_filter]:
	# end of for key_filter in filteredNews_tweetStack:
	####################################################################

	# convert to ram
	convert2InnoDB(connection=connection, tableName=flteredNews_tableName)
	print "%s converted to InnoDB table" % flteredNews_tableName

	####################################################################
	connection.close()
	return None

"""
###############################################################################################################



###############################################################################################################
"""









