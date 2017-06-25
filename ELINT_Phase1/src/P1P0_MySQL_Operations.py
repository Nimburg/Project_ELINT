

import os
import numpy as np 
import pandas as pd
import collections as col
import json

import pymysql.cursors


'''
########################################################################################################
'''

def Get_TableNames_headerlist(MySQL_DBkey, start_time, end_time, header_list):
	'''
	get table names

	header_list: list of basic table names
	start_time: pd.to_datetime( time_string)
	end_time: 
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
	list_table_names = []
	# loop through header_list:
	for header in header_list: 
		pin_time = start_time
		# loop through time
		while (pin_time < end_time):
			# table to check
			table_name = header + pin_time.strftime('_%Y_%m_%d_%H')
			# comd
			comd_table_check = """
SELECT IF( 
(SELECT count(*) FROM information_schema.tables
WHERE table_schema = '%s' AND table_name = '%s'), 1, 0);"""
			# execute command
			try:
				with connection.cursor() as cursor:
					cursor.execute( comd_table_check % tuple( [db_name] + [table_name] )
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
				print "%s exists." % table_name 
				list_table_names.append(table_name)
			# go to next time point
			pin_time = pin_time + np.timedelta64(3600,'s')
		# end of time loop
	# end of header loop
	connection.close()
	return list_table_names

'''
########################################################################################################
'''

def Transfer_Tables(MySQL_key_from, MysQL_key_to, list_tableNames):
	'''
	Transfer selected RSB tables from Ultra to ELINT_transUltra
	on per-day basis

	MySQL_key_from: Ultra
	MysQL_key_to: ELINT_transUltra
	'''

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=MySQL_key_from['host'],
								 user=MySQL_key_from['user'],
								 password=MySQL_key_from['password'],
								 db=MySQL_key_from['db'],
								 charset=MySQL_key_from['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	
	####################################################################

	db_to = MysQL_key_to['db']
	db_from = MySQL_key_from['db']
	# through list_tableNames
	for tableName in list_tableNames:
		# comd
		comd_table_transfer = """
CREATE TABLE IF NOT EXISTS %s.%s AS
SELECT * FROM %s.%s;"""
		# execute command
		try:
			with connection.cursor() as cursor:
				cursor.execute( comd_table_transfer % tuple( [db_to] + 
															 [tableName] +
															 [db_from] +
															 [tableName] 
															)
							  )
				result = cursor.fetchall()
				#print result
				print "transfering %s" % tableName
				#print pin
		finally:
			pass
	# end of list_tableNames loop
	connection.close()
	return None

'''
########################################################################################################
'''

def Concatenate_Tags(MysQL_key, list_tableNames, variables_dict, pin_time, 
					 concatenate_tableName='combined_tags'):
	'''
	MysQL_key: ELINT_transUltra
	pin_time: for combined_tags_time
	'''

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=MysQL_key['host'],
								 user=MysQL_key['user'],
								 password=MysQL_key['password'],
								 db=MysQL_key['db'],
								 charset=MysQL_key['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	
	####################################################################
	
	combined_tags = col.defaultdict()

	filter_key_Ncall = variables_dict['filter_key_Ncall']
	filter_relev_Ncall = variables_dict['filter_relev_Ncall']
	filter_relev_score = variables_dict['filter_relev_score']
	
	####################################################################
	
	#########################
	# extract tags from RSB #
	#########################
	db_name = MysQL_key['db']	
	# through list_tableNames
	for tableName in list_tableNames:
		# Comd to extract data from each table		
		# check against key
		if 'key' in tableName:
			Comd_Extract_key = """
SELECT tag, tag_Ncall
FROM %s.%s
WHERE tag_Ncall >= %i
ORDER BY tag_Ncall DESC;"""
			Comd = Comd_Extract_key % tuple([db_name]+
											[tableName]+
											[filter_key_Ncall])
		# check against relev
		if 'relev' in tableName: 
			Comd_Extract_relev = """
SELECT tag, tag_Ncall 
FROM %s.%s
WHERE tag_Ncall >= %i AND tag_Score >= %i
ORDER BY tag_Score DESC;"""
			Comd = Comd_Extract_relev % tuple([db_name]+
											  [tableName]+
											  [filter_relev_Ncall]+
											  [filter_relev_score])
		# Execute Comds
		print "extracting hash tags from %s" % tableName
		try:
			with connection.cursor() as cursor:
				cursor.execute( Comd )
				result = cursor.fetchall()
				# loop through all rows of this table
				for entry in result:
					# {u'tag': u'hillary2016forsanity', u'tag_Ncall': 1}
					# in MySQL, tag format is utf8, but in raw data as ASCII
					tag = str( entry['tag'] ).decode('utf-8').encode('ascii', 'ignore')				
					Ncall = entry['tag_Ncall']					
					# if tag in dict, update if Ncall is larger
					if tag in combined_tags:
						if Ncall > combined_tags[tag]: 
							combined_tags[tag] = Ncall
					# if tag not in dict
					else:
						combined_tags[tag] = Ncall
				# end of table result
		finally:
			pass
	# end of list_tableNames loop
	
	####################################################################

	##############################
	# create combined_tags table #
	##############################
	new_tableName = concatenate_tableName + pin_time.strftime('%Y_%m_%d_%H')

	# Comds for Initializing 6 tables in 3 categories
	Comd_Create_combTags = """
DROP TABLE IF EXISTS %s;
CREATE TABLE IF NOT EXISTS %s
(
	id MEDIUMINT NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (id),
    tag varchar(255),
    tag_Ncall int
)ENGINE=MEMORY;"""
	# Comds for Inserting values into 6 tables
	comd_Insert_combTags = """
INSERT INTO %s (tag, tag_Ncall)
VALUES ('%s', %s);"""
	# Comds for Converting in-RAM tables into InnoDB tables
	comd_convert = """
ALTER TABLE %s ENGINE=InnoDB;"""

	# Initialize table
	try:
		with connection.cursor() as cursor:
			cursor.execute( Comd_Create_combTags % tuple( [new_tableName]+[new_tableName] ) 
						  )
		# commit commands
		connection.commit()
	finally:
		pass
	# Insert values
	print "loading %s" % new_tableName
	for tag in combined_tags:
		try:
			with connection.cursor() as cursor:
				cursor.execute( comd_Insert_combTags % tuple( [new_tableName] + 
															  [str(tag)] + 
															  [str(combined_tags[tag])]
															) 
							  )
			# commit commands
			connection.commit()
		finally:
			pass
	# end of combined_tags
	# Convert tables
	try:
		with connection.cursor() as cursor:
			cursor.execute( comd_convert % new_tableName )
		# commit commands
		connection.commit()
	finally:
		pass

	####################################################################
	connection.close()
	return None

'''
########################################################################################################
'''


































