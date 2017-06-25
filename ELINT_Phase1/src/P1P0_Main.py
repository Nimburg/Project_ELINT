

import os
import numpy as np 
import pandas as pd
import json
import collections as col

import pymysql.cursors

from P1P0_MySQL_Operations import Get_TableNames_headerlist, Transfer_Tables, Concatenate_Tags


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

Set up SQL in-RAM table variables

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
	####################################################################
	return None

"""
########################################################################################################

Transfer RSB tables from Ultra to ELINT_transUltra

########################################################################################################
"""

def Phase1_Part0_TableTransfers(variables_dict):
	'''
	variables_dict: dictionary of variables' values
					Universal through out Phase1_Part0
	'''
	# extract variables
	start_time = variables_dict['Start_Time']
	end_time = variables_dict['End_Time']
	time_period = variables_dict['Time_Period']
	MySQL_key_Ultra = variables_dict['MySQL_key_Ultra']
	MySQL_key_transUltra = variables_dict['MySQL_key_transUltra']
	header_list = variables_dict['header_list']

	#######################
	# Transfer RSB tables #
	#######################
	# initialize for the first period
	pin_start_time = start_time
	pin_end_time = start_time + time_period
	# while loop until pin_start_time is later than end_time
	while pin_start_time < end_time:
		
		print "Extracting between %s and %s" % tuple( [pin_start_time.strftime('_%Y_%m_%d_%H')] + 
													  [pin_end_time.strftime('_%Y_%m_%d_%H')] 
													)
		# extract table names from data base
		list_table_names = Get_TableNames_headerlist(MySQL_DBkey=MySQL_key_Ultra, 
													start_time=pin_start_time, end_time=pin_end_time, 
													header_list=header_list)
		# if tables are not found
		if len(list_table_names) == 0:
			print "No talbes found between %s and %s" % tuple( [pin_start_time.strftime('_%Y_%m_%d_%H')] + 
													  		   [pin_end_time.strftime('_%Y_%m_%d_%H')] 
															 )
		# transfer tables according to list_table_names
		if len(list_table_names) > 0:
			Transfer_Tables(MySQL_key_from=MySQL_key_Ultra, 
							MysQL_key_to=MySQL_key_transUltra, 
							list_tableNames=list_table_names)
		# update pin_start_time
		pin_start_time = pin_start_time + time_period
		pin_end_time = pin_end_time + time_period
	# end of while loop
	
	####################################################################
	return None

"""
########################################################################################################

Construct Tag_to_Index dictionary

########################################################################################################
"""

def Phase1_Part0_tags2index(variables_dict):
	'''
	variables_dict: dictionary of variables' values
					Universal through out Phase1_Part0
	'''
	# extract variables
	start_time = variables_dict['Start_Time']
	end_time = variables_dict['End_Time']
	time_period = variables_dict['Time_Period']
	MySQL_key_transUltra = variables_dict['MySQL_key_transUltra']
	header_list = variables_dict['header_list']

	########################
	# create tags_to_index #
	########################
	# initialize for the first period
	pin_start_time = start_time
	pin_end_time = start_time + time_period
	# while loop until pin_start_time is later than end_time
	while pin_start_time < end_time:
		
		print "Operating between %s and %s" % tuple( [pin_start_time.strftime('_%Y_%m_%d_%H')] + 
													 [pin_end_time.strftime('_%Y_%m_%d_%H')] 
													)
		# extract table names from data base
		list_table_names = Get_TableNames_headerlist(MySQL_DBkey=MySQL_key_transUltra, 
													start_time=pin_start_time, end_time=pin_end_time, 
													header_list=header_list)
		# if tables are not found
		if len(list_table_names) == 0:
			print "No talbes found between %s and %s" % tuple( [pin_start_time.strftime('_%Y_%m_%d_%H')] + 
													  		   [pin_end_time.strftime('_%Y_%m_%d_%H')] 
															 )
		# combine RSB tags, per-day basis
		if len(list_table_names) > 0:
			Concatenate_Tags(MysQL_key=MySQL_key_transUltra, 
							 list_tableNames=list_table_names, 
							 variables_dict=variables_dict, 
							 pin_time=pin_start_time, 
							 concatenate_tableName='combined_tags')
		# update pin_start_time
		pin_start_time = pin_start_time + time_period
		pin_end_time = pin_end_time + time_period
	# end of while loop
	
	####################################################################
	return None

"""
########################################################################################################

Main Function of Phase1 Part0

########################################################################################################
"""

def Phase1_Part0_Main(variables_dict):
	'''
	variables_dict: dictionary of variables' values
					Universal through out Phase1_Part0
	'''
	# check against sys.arg from command line

	####################################################################

	# set in-RAM table size
	MySQL_key_transUltra = variables_dict['MySQL_key_transUltra']
	N_GB = variables_dict['N_GB']
	Set_TempTable_Variables(MySQL_DBkey = MySQL_key_transUltra, N_GB = N_GB)

	####################################################################

	# transfer RSB tables from Ultra to ELINT_transUltra
	if variables_dict['flag_transtable'] == True:
		Phase1_Part0_TableTransfers(variables_dict)

	# create tag2index dictionary
	if variables_dict['flag_tag2index'] == True: 
		Phase1_Part0_tags2index(variables_dict)

	####################################################################
	# convert Timestamp to string
	variables_dict['Start_Time'] = variables_dict['Start_Time'].strftime('%Y_%m_%d_%H')
	variables_dict['End_Time'] = variables_dict['End_Time'].strftime('%Y_%m_%d_%H')
	variables_dict['Time_Period'] = str(variables_dict['Time_Period'])
	# saving model_options to .json file
	json_name = 'P1P0_ExecutedComds.json'
	with open(json_name, 'w') as fp:
		json.dump(variables_dict, fp, sort_keys=True, indent=4)	
	return None

"""
########################################################################################################

# Execution of Phase1_Part0

########################################################################################################
"""

if __name__ == "__main__":

	####################################################################

	# Phase1 Part0 Universal Variables' Values
	variables_dict = dict()

	# flag_transTable
	variables_dict['flag_transtable'] = False
	# flag_tag2index
	variables_dict['flag_tag2index'] = False
	# RAM table size
	variables_dict['N_GB'] = 4
	
	####################################################################
	
	# Ultra data base
	MySQL_key_Ultra = {'host':'localhost','user':'','password':'','db':'','charset':'utf8'}
	# data base with selected Ultra tables transfered
	MySQL_key_transUltra = {'host':'localhost','user':'','password':'','db':'','charset':'utf8'}
	# ELINT data base, so far for Phase1
	MySQL_key_ELINT = {'host':'localhost','user':'','password':'','db':'','charset':'utf8'}

	variables_dict['MySQL_key_Ultra'] = MySQL_key_Ultra
	variables_dict['MySQL_key_transUltra'] = MySQL_key_transUltra
	variables_dict['MySQL_key_ELINT'] = MySQL_key_ELINT

	####################################################################

	Start_Time = '2016-07-12 00:00:00'
	Start_Time = pd.to_datetime(Start_Time)
	print "start time: ", Start_Time.strftime('_%Y_%m_%d_%H')

	End_Time = '2016-11-20 00:00:00'
	End_Time = pd.to_datetime(End_Time)
	print "end time: ", End_Time.strftime('_%Y_%m_%d_%H')
	
	# go to next time point
	# pin_time = pin_time + np.timedelta64(3600,'s')
	Time_Period = np.timedelta64(3600*24,'s') # one day

	variables_dict['Start_Time'] = Start_Time
	variables_dict['End_Time'] = End_Time
	variables_dict['Time_Period'] = Time_Period

	####################################################################

	# RSB table headers
	header_list = ['rsb_tag_key1','rsb_tag_key2','rsb_tag_relev1','rsb_tag_relev2']

	variables_dict['header_list'] = header_list

	####################################################################

	variables_dict['filter_key_Ncall'] = 5
	variables_dict['filter_relev_Ncall'] = 10
	variables_dict['filter_relev_score'] = 5

	####################################################################

	Phase1_Part0_Main(variables_dict)







