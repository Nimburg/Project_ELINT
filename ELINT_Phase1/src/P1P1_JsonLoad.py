

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

####################################################################
# parse multi-line string noto single line
def parse_MultiLine_text(input_str):
	temp_list = input_str.splitlines()
	res = ""
	for item in temp_list:
		res += item + ' '
	return res

# force json.loads() take it as ASCii
def transASC(input): 
    if isinstance(input, dict):
    	tempdic = dict()
    	for key,value in input.iteritems():
    		tempdic[transASC(key)] = transASC(value)
    	return tempdic
    elif isinstance(input, list):
        return [transASC(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input

# get ride of utf-8
def removeUtf(text_input):
	listtext = list(text_input)
	for j in range(len(listtext)): # handle utf-8 issue
		try:
			listtext[j].encode('utf-8')
		except UnicodeDecodeError:
			listtext[j] = ''
		if listtext[j] == '\n': # for those with multiple line text
			listtext[j] = ''
	text_input = ''.join(listtext)
	return text_input

"""
###########################################################################################################

Json Load

###########################################################################################################
"""

def UserTimeLine_JsonLoad(input_str, index_line):
	'''
	'''
	# main data structure, contains information from each tweet
	Tweet_OBJ = col.defaultdict()
	# tweet
	Tweet_OBJ['tweet_time'] = None # pd.timestamp
	Tweet_OBJ['tweet_id'] = None # all id are id_str
	Tweet_OBJ['text'] = "" # make sure text is onto a single line.....
	Tweet_OBJ['lang'] = ""
	Tweet_OBJ['coordinates'] = "" # Null, or {"coordinates":[-75.14310264,40.05701649],"type":"Point"}

	Tweet_OBJ['reply_to_userID'] = "-1" # set of tuple(userID, userName)
	Tweet_OBJ['quoted_status_id'] = "-1"
	Tweet_OBJ['in_reply_to_status_id'] = "-1" # set of tweetID (set of strings)

	Tweet_OBJ['retweet_count'] = 0
	Tweet_OBJ['favorite_count'] = 0	
	
	# user
	Tweet_OBJ['user_id'] = "-1" # all id are id_str
	Tweet_OBJ['user_name'] = "" 
	Tweet_OBJ['user_verified'] = False # default
	
	Tweet_OBJ['user_followers'] = 0 # default 0
	Tweet_OBJ['user_friends'] = 0
	Tweet_OBJ['user_favourites'] = 0
	Tweet_OBJ['user_listed'] = 0
	Tweet_OBJ['user_statuses'] = 0
	
	# hash tag
	Tweet_OBJ['Tag'] = set() # set of strings
	# mentions
	Tweet_OBJ['mentioned_userID'] = set() # set of tuple(userID, userName)

	#################################################################################
	# json load, extract tweet time and id_str
	flag_TidTimeAuthor = True # flag for tweet id, time and author
	try:
		# load json
		tweet_json = json.loads(input_str)
	except ValueError: 
		print "Line: {}, json loads Error".format(index_line)
		flag_TidTimeAuthor = False
	else:	
		# extract date-time from mainbody
		try:
			time_str = tweet_json['created_at']
			tweet_id = tweet_json['id_str']
		except ValueError:
			flag_TidTimeAuthor = False
			pass
		except KeyError:
			flag_TidTimeAuthor = False
			pass
		else:
			# convert to pandas timestamp
			try:
				time_dt = pd.to_datetime(time_str)
				if pd.isnull(time_dt):
					flag_TidTimeAuthor = False
					print "Line: {}, date-time is NaT".format(index_line)
			except ValueError:
				flag_TidTimeAuthor = False
				print "Line: {}, date-time convertion failed".format(index_line)
				pass
			else:
				# upload to RetD_TimeUserTag
				if flag_TidTimeAuthor:
					Tweet_OBJ['tweet_time'] = time_dt
					Tweet_OBJ['tweet_id'] = tweet_id

	#################################################################################
	# extract user information sub-json
	if flag_TidTimeAuthor:
		try:
			user_json = tweet_json['user']
		except ValueError:
			flag_TidTimeAuthor = False
			pass
		except KeyError:
			flag_TidTimeAuthor = False
			pass
		else:
			# extract user statistics
			try: 
				user_id = user_json['id_str']
				user_name = user_json['screen_name']
				if len(user_name) > 253:
					user_name = user_name[:250]
				user_followers = user_json['followers_count']
				user_friends = user_json['friends_count']
			except ValueError:
				flag_TidTimeAuthor = False
				pass
			except KeyError:
				flag_TidTimeAuthor = False
				pass
			else:
				if flag_TidTimeAuthor:
					Tweet_OBJ['user_id'] = user_id
					Tweet_OBJ['user_name'] = user_name
					Tweet_OBJ['user_followers'] = user_followers
					Tweet_OBJ['user_friends'] = user_friends
					
	#################################################################################
	# extract tweet direct information
	if flag_TidTimeAuthor:

		# extract coordinates information
		try:
			geo_json = tweet_json['coordinates']
			coordinates = str( geo_json['coordinates'] )
		except ValueError:
			pass
		except KeyError:
			pass
		except AttributeError:
			pass
		except TypeError:
			pass
		else:
			Tweet_OBJ['coordinates'] = coordinates

		# extract lang information
		try:
			lang = tweet_json['lang']
		except ValueError:
			pass
		except KeyError:
			pass
		except AttributeError:
			pass
		except TypeError:
			pass
		else:
			Tweet_OBJ['lang'] = lang

		# extract retweet_count information
		try:
			retweet_count = tweet_json['retweet_count']
		except ValueError:
			pass
		except KeyError:
			pass
		except AttributeError:
			pass
		except TypeError:
			pass
		else:
			Tweet_OBJ['retweet_count'] = retweet_count

		# extract favorite_count information
		try:
			favorite_count = tweet_json['favorite_count']
		except ValueError:
			pass
		except KeyError:
			pass
		except AttributeError:
			pass
		except TypeError:
			pass
		else:
			Tweet_OBJ['favorite_count'] = favorite_count
		
		# extract reply_to_user information
		try:
			reply_userID_str = tweet_json['in_reply_to_user_id_str']
			# if userID == null, raise error; if not full digit str, raise false
			flag_idstr = reply_userID_str.isdigit()
		except ValueError:
			pass
		except KeyError:
			pass
		except AttributeError:
			pass
		except TypeError:
			pass
		else:
			if flag_idstr == True:
				Tweet_OBJ['reply_to_userID'] = reply_userID_str
		
		# extract in_reply_to_status_id information
		try:
			reply_tweetID_str = tweet_json['in_reply_to_status_id_str']
			# if userID == null, raise error; if not full digit str, raise false
			flag_idstr = reply_tweetID_str.isdigit()
		except ValueError:
			pass
		except KeyError:
			pass
		except AttributeError:
			pass
		except TypeError:
			pass
		else:
			if flag_idstr == True:
				Tweet_OBJ['in_reply_to_status_id'] = reply_tweetID_str
		
		# extract quoted_status_id information
		try:
			quoted_status_id = tweet_json['quoted_status_id']
			# if userID == null, raise error; if not full digit str, raise false
			flag_idstr = quoted_status_id.isdigit()
		except ValueError:
			pass
		except KeyError:
			pass
		except AttributeError:
			pass
		except TypeError:
			pass
		else:
			if flag_idstr == True:
				Tweet_OBJ['quoted_status_id'] = quoted_status_id

	#################################################################################
	# extract tags from entities
	if flag_TidTimeAuthor:
		# extract tags from entities
		tag_list = set([]) # eliminate repeating tags
		try:
			entities_json = tweet_json['entities']
			Hashtags_json = entities_json['hashtags']
		except ValueError:
			pass
		except KeyError:
			pass
		except TypeError:
			pass
		else:		
			for entry in Hashtags_json:
				try:
					# THIS IS VERY VERY VERY IMPORTANT !!!!!
					tag_text = str(entry['text']).lower()
					if len(tag_text) > 253:
						tag_text = tag_text[:250]
					tag_list.add(tag_text) # THIS IS VERY VERY VERY IMPORTANT !!!!!
					# THIS IS VERY VERY VERY IMPORTANT !!!!!
					# MySQL cant distinguish upper and lower cases when str is used as name for table
					# which will result in confusion in data analysis
				except ValueError:
					pass
				except KeyError:
					pass
				except TypeError:
					pass
			# end of for
			for item in tag_list:
				Tweet_OBJ['Tag'].add(item)

	#############################################################
	# extract text
	if flag_TidTimeAuthor:
		# extract date-time from mainbody
		try:
			text_str = tweet_json['text']
			text_str = transASC(text_str)
			text_str = removeUtf(text_str)
			text_str = text_str.replace("'", "")
			text_str = parse_MultiLine_text(text_str)
		except ValueError:
			pass
		except KeyError:
			pass
		else:
			Tweet_OBJ['text'] = text_str

	#############################################################
	# extract mentioned_userID
	if flag_TidTimeAuthor:
		# extract entities and user_mentions	
		try:
			usermentions_json = entities_json['user_mentions']
		except ValueError:
			pass
		except KeyError:
			pass
		except TypeError:
			pass
		else:
			for entry in usermentions_json:
				try:
					Tweet_OBJ['mentioned_userID'].add(entry['id_str'])
				except ValueError:
					pass
				except KeyError:
					pass
				except TypeError:
					pass

	#############################################################
	return flag_TidTimeAuthor, Tweet_OBJ











