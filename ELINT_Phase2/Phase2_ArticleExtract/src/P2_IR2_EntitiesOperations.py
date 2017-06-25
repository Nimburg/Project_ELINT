#!/usr/bin/env python


import json
import os
import numpy as np 
import pandas as pd
import time
import collections as col

import pymysql.cursors



"""
###############################################################################################################

Extract, Concatenate Entities from .json files

###############################################################################################################
"""
# get ride of utf-8
def removeUtf(text_input):
	listtext = list(text_input)
	for j in range(len(listtext)): # handle utf-8 issue
		try:
			listtext[j].encode('utf-8')
		except UnicodeEncodeError:
			listtext[j] = ''
		except UnicodeDecodeError:
			listtext[j] = ''

		if listtext[j] == '\n': # for those with multiple line text
			listtext[j] = ''
	text_input = ''.join(listtext)
	return text_input

def Extract_Concatenate_Entities(variables_dict, json_list=None):
	'''
	json_list: default as None. If it is None, then extract all json files from specified location


	'''
	##################
	# get data files #
	##################
	# addr of json files
	path_from = variables_dict['json_Entities_addr']
	# concatenate selected or all json files
	if json_list is not None:
		fileName_list = json_list
		print "Number of files to be concatenated: %i" % len(fileName_list)
	# concatenate all data files
	else:
		# select json files, select entities_to_speech
		fileName_list = [ f for f in os.listdir(path_from) if (f[-5:]=='.json') and (f[4:12]=='entities') ]
		print "Number of files found: %i" % len(fileName_list)

	############################
	# go through fileName_list #
	############################
	
	# basic data structure
	# defaultdict(), with entities as higher level key
	# Entities_Concatenated[entity] = col.defaultdict()
	# 
	# Entities_Concatenated[entity][Ncall] = int, cumulative added through json files
	# 
	# Entities_Concatenated[entity][relevance_list] = list of float
	# Entities_Concatenated[entity][relevance_ave] = float, ave of relevance_list
	# 
	# Entities_Concatenated[entity][type] = col.Counter()
	# Entities_Concatenated[entity][type][type_i] = count_i
	Entities_Concatenated = col.defaultdict()

	# set() of different entity types
	entity_types_set = set()

	for file in fileName_list:

		print "\nworking on: %s" % file
		# add path
		file_extract = path_from + file
		# read text
		f = open(file_extract,'r')
		text = f.read()
		f.close()
		# convert to json
		entities_json = json.loads(text)

		###########################################
		# go through each entity in entities_json #
		###########################################
		# key is the Alchemy extracted entity, string of words
		# Alchemy distinguishes upper and lower cases
		for key in entities_json:
			# unify upper and lower cases
			entity_name_alk = key
			entity_name = key.lower()
			entity_name = removeUtf(entity_name)

			# if entity_name already in Entities_Concatenated
			if Entities_Concatenated.has_key(entity_name):
				# update on Ncall
				Entities_Concatenated[entity_name]['Ncall'] += entities_json[entity_name_alk]['Ncall']
				
				# extract relevance list for key in entities_json
				relevance_list = []
				# entities_json[entity_name]['relevance'] is dict as (file_name, float_str)
				for key in entities_json[entity_name_alk]['relevance']:
					# here key is file_name
					relevance_list.append( float(entities_json[entity_name_alk]['relevance'][key]) )
				# update relevance list for key in entities_json
				Entities_Concatenated[entity_name]['relevance_list'].extend(relevance_list)

				# update on entity types from each file
				# here key as file_name
				for key in entities_json[entity_name_alk]['type']:
					type_name = entities_json[entity_name_alk]['type'][key].lower()
					type_name = removeUtf(type_name)
					# add into entity_types_set, if 1st encounter
					entity_types_set.add(type_name)
					# Entities_Concatenated[entity_name]['type'] is col.Counter()
					Entities_Concatenated[entity_name]['type'][type_name] += 1
			# if this is a new entity_name
			else:
				# create Entities_Concatenated[entity_name]
				Entities_Concatenated[entity_name] = col.defaultdict()
				# create and update Ncall
				Entities_Concatenated[entity_name]['Ncall'] = entities_json[entity_name_alk]['Ncall']
				# create relevance_list and relevance_ave
				Entities_Concatenated[entity_name]['relevance_list'] = []
				Entities_Concatenated[entity_name]['relevance_ave'] = 0.0
				# create type Counter()
				Entities_Concatenated[entity_name]['type'] = col.Counter()

				# extract relevance list for key in entities_json
				relevance_list = []
				# entities_json[entity_name]['relevance'] is dict as (file_name, float_str)
				for key in entities_json[entity_name_alk]['relevance']:
					# here key is file_name
					relevance_list.append( float(entities_json[entity_name_alk]['relevance'][key]) )
				# update relevance list for key in entities_json
				Entities_Concatenated[entity_name]['relevance_list'].extend(relevance_list)

				# update on entity types from each file
				# here key as file_name
				for key in entities_json[entity_name_alk]['type']:
					type_name = entities_json[entity_name_alk]['type'][key].lower()
					type_name = removeUtf(type_name)
					# add into entity_types_set, if 1st encounter
					entity_types_set.add(type_name)					
					Entities_Concatenated[entity_name]['type'][type_name] += 1
		# end of for key in entities_json:
		print "Number of Entity Types Encountered: %i, print out Types: \n%s" % (len(entity_types_set), entity_types_set)
	# end of for file in fileName_list:
	
	print "Number of Entities Extracted: %i" % len(Entities_Concatenated)

	# calculate relevance_ave
	for key in Entities_Concatenated:
		entity_name = key
		Entities_Concatenated[entity_name]['relevance_ave'] = \
							 np.average( Entities_Concatenated[entity_name]['relevance_list'] )
		#print ""		
		#print entity_name.encode('utf-8')
		#print Entities_Concatenated[entity_name]['Ncall']
		#print Entities_Concatenated[entity_name]['relevance_ave']
		#print Entities_Concatenated[entity_name]['type']
	# end of calculating relevance_ave

	#############################################################################
	# saving model_options to .json file
	json_name = '../Results/Concatenated_Entities.json'
	with open(json_name, 'w') as fp:
		json.dump(Entities_Concatenated, fp, sort_keys=True, indent=4)	
	# returns
	return entity_types_set, Entities_Concatenated

"""
###############################################################################################################

Load Concatenate Entities to ELINT_Phase1

###############################################################################################################
"""

def Load_Entities(variables_dict, Entities_Concatenated, flag_fullText=None):
	'''
	Universal Functions, so long has a correct Entities_Concatenated

	Entities_Concatenated: col.defaultdict()
	# defaultdict(), with entities as higher level key
	# Entities_Concatenated[entity] = col.defaultdict()
	# 
	# Entities_Concatenated[entity][Ncall] = int, cumulative added through json files
	# 
	# Entities_Concatenated[entity][relevance_list] = list of float
	# Entities_Concatenated[entity][relevance_ave] = float, ave of relevance_list
	# 
	# Entities_Concatenated[entity][type] = col.Counter()
	# Entities_Concatenated[entity][type][type_i] = count_i	
	'''
	####################################################################
	
	MySQL_DBkey = variables_dict['MySQL_key_ELINT']
	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey['host'],
								 user=MySQL_DBkey['user'],
								 password=MySQL_DBkey['password'],
								 db=MySQL_DBkey['db'],
								 charset=MySQL_DBkey['charset'],
								 cursorclass=pymysql.cursors.DictCursor)

	####################################################################
	
	# get table name
	if flag_fullText == True:
		tableName = variables_dict['tableName_cedEnti_fullText']
	elif flag_fullText == False:
		tableName = variables_dict['tableName_cedEnti_filteredText']
	else:
		print "Please specify table name"
		return None # exit

	####################################################################

	#######################
	# create Entity table #
	#######################	
	#Comd
	Comd_Entities_Init = """
DROP TABLE IF EXISTS %s;
CREATE TABLE IF NOT EXISTS %s
(
	id MEDIUMINT NOT NULL AUTO_INCREMENT,
	PRIMARY KEY (id),
	entity_name varchar(300),
	Ncall int,
	relevance_ave FLOAT, 
	primary_type varchar(300),
	primary_type_pct FLOAT default 0.0,
	secondary_type varchar(300) default "",
	secondary_type_pct FLOAT default 0.0,
	tertiary_type varchar(300) default "",
	tertiary_type_pct FLOAT default 0.0
)ENGINE=MEMORY DEFAULT CHARSET=utf8;"""
	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute( Comd_Entities_Init % (tableName, tableName) )
		# commit commands
		print tableName+" Initialized"
		connection.commit()
	finally:
		pass

	##############################
	# insert Entities into table #
	##############################
	# Comds for Inserting values into 6 tables
	comd_Insert_Entities = """
INSERT INTO %s (entity_name, Ncall, relevance_ave, \
primary_type, primary_type_pct, secondary_type, secondary_type_pct, tertiary_type, tertiary_type_pct)
VALUES ('%s', %i, %s, \
'%s', %s, '%s', %s, '%s', %s);"""
	# go through 
	for key in Entities_Concatenated:
		entity_name = key

		#########################
		# get pri-sec-ter types #
		#########################
		type_list = ["", "", ""]
		type_pct_list = [0.0, 0.0, 0.0]	
		# sorted col.Counter(), decending order
		sorted_tuple_list = Entities_Concatenated[entity_name]['type'].most_common()
		# update 
		length = len(sorted_tuple_list)
		if length >= 3:
			length = 3
		for idx in range( length ):
			type_list[idx] = sorted_tuple_list[idx][0]
			type_pct_list[idx] = 1.0*sorted_tuple_list[idx][1]/Entities_Concatenated[entity_name]['Ncall']

		# execute comd_Insert_Entities
		try:
			with connection.cursor() as cursor:
				cursor.execute( comd_Insert_Entities % tuple( [tableName] + 
															  [entity_name] + 
															  [Entities_Concatenated[entity_name]['Ncall']] + 
															  [str(Entities_Concatenated[entity_name]['relevance_ave'])] + 
															  [ type_list[0] ] + 
															  [str(type_pct_list[0])] + 
															  [ type_list[1] ] + 
															  [str(type_pct_list[1])] + 
															  [ type_list[2] ] + 
															  [str(type_pct_list[2])]
															) 
							  )
			# commit commands
			connection.commit()
		except pymysql.err.ProgrammingError:
			pass
		finally:
			pass

	########################
	# convert Entity table #
	########################
	# Comds for Converting in-RAM tables into InnoDB tables
	comd_convert = """
ALTER TABLE %s ENGINE=InnoDB;"""
	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute( comd_convert % tableName )
		# commit commands
		print tableName+" converted to InnoDB"
		connection.commit()
	finally:
		pass

	####################################################################
	connection.close()
	return None


"""
########################################################################################################

Independent Execution

########################################################################################################
"""

if __name__ == "__main__":

	#################################################################################################

	variables_dict = col.defaultdict()

	#################################################################################################

	# list of .json files for Entities extracted from Speeches/Statements
	json_Entities_list = ['HSp_entities_to_fileName.json','HSt_entities_to_fileName.json',\
						  'TSp_entities_to_fileName.json','TSt_entities_to_fileName.json']

	# address of data files
	json_Entities_addr = '../Results/Alchemy_Entities_Results/'

	variables_dict['json_ENtities_list'] = json_Entities_list
	variables_dict['json_Entities_addr'] = json_Entities_addr	

	#################################################################################################

	Extract_Concatenate_Entities(variables_dict=variables_dict, json_list=None)






