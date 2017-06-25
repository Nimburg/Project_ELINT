
import re

import nltk
from nltk.stem import WordNetLemmatizer

#######################################################################################################

def Negative_Parsing(input_str):
	'''
	input: string of sentence
	return: string of sentence

	explicitly parse negative expression
	'''
	# parse dictionary
	Dict_Negative_Parsing = dict()
	# will
	Dict_Negative_Parsing["won't"] = "will not"
	Dict_Negative_Parsing["wont"] = "will not"
	# would
	Dict_Negative_Parsing["wouldn't"] = "would not"
	Dict_Negative_Parsing["wouldnt"] = "would not"
	# is
	Dict_Negative_Parsing["isn't"] = "is not"
	Dict_Negative_Parsing["isnt"] = "is not"
	# are
	Dict_Negative_Parsing["aren't"] = "are not"
	Dict_Negative_Parsing["arent"] = "are not"
	# was
	Dict_Negative_Parsing["wasn't"] = "was not"
	Dict_Negative_Parsing["wasnt"] = "was not"
	# were
	Dict_Negative_Parsing["weren't"] = "were not"
	Dict_Negative_Parsing["werent"] = "were not"
	# do
	Dict_Negative_Parsing["don't"] = "do not"
	Dict_Negative_Parsing["dont"] = "do not"
	# did
	Dict_Negative_Parsing["didn't"] = "did not"
	Dict_Negative_Parsing["didnt"] = "did not"
	# can
	Dict_Negative_Parsing["can't"] = "can not"
	Dict_Negative_Parsing["cant"] = "can not"
	# could
	Dict_Negative_Parsing["couldn't"] = "could not"
	Dict_Negative_Parsing["couldnt"] = "could not"
	# must
	Dict_Negative_Parsing["mustn't"] = "must not"
	Dict_Negative_Parsing["mustnt"] = "must not"
	# shall
	Dict_Negative_Parsing["shalln't"] = "shall not"
	Dict_Negative_Parsing["shallnt"] = "shall not"
	# should
	Dict_Negative_Parsing["shouldn't"] = "should not"
	Dict_Negative_Parsing["shouldnt"] = "should not"

	#######################################################################
	res_str = ""
	word_list = input_str.split(' ')
	for word in word_list:
		if word in Dict_Negative_Parsing: 
			res_str += Dict_Negative_Parsing[word]+' '
		else:
			res_str += word+' '
	return res_str

#######################################################################################################

def Remove_http_tag_user(input_str):
	'''
	input: string of sentence
	output: string of sentence

	truncate sentence at first http
	remove RT_ at sentence start
	replace #sth with sth
	remove @username
	'''
	# truncate sentence at first http
	try:
		http_idx = input_str.index('http')
		temp_str = input_str[:http_idx]
	except ValueError:
		temp_str = input_str
		pass
	# remove RT_ at sentence start
	temp_str = temp_str.replace('rt ','')
	# remove #
	temp_str = temp_str.replace('#','')	
	# remove @username
	res_str = ""
	word_list = temp_str.split(' ')
	for word in word_list:
		if '@' not in word: 
			res_str += word+' '
	return res_str

#######################################################################################################

def track_keywords_filter(Tweet_Input, variables_dict):
	'''
	Tweet_Input: dict of single tweet infor

	# geo-political related <--- post mid-March, 2017
	variables_dict['key_china'] = ['china','chinese','beijing','xi jinping','shanghai',\
									'guangzhou','hong kong','tibet','xin jiang']
	variables_dict['key_taiwan'] = ['taipei','taiwan','taiwanese']
	variables_dict['key_japan'] = ['tokyo','japan','japanese']
	variables_dict['key_russia'] = ['russia','moscow','kremlin','russian','putin','ukraine','crimea','NATO']
	variables_dict['key_mid_east'] = ['syria','turkey','iran','isreal','isis','saudi','iraq','india','pakistan']
	variables_dict['key_europe'] = ['brexit','frexit','euro','european union','french','france','le pen','national front','fillon']

	# macro-economy related <--- post mid-March, 2017
	variables_dict['key_Economy'] = ['trade','debt','inflation','gdp','fiscal','FDI','investment','tariff','currency',\
									'exchange rate','outbound investment','employment','unemployment','subsidiary',\
									'philanthropy','environment',\
									'tax','TPP','NAFTA','WTO','IMF','AIIB','world bank','wall street','Dow Jones']

	# Chinese companies related <--- post mid-March, 2017
	variables_dict['key_CNcompany'] = ['Wanda','Shuanghui','Lenovo','Haier','Fosun','Sinopec','China National Offshore Oil Corporation',\
										'Anbang','China Investment Corporation','Aviation Industry Corporation of China',\
										'Hua Capital consortium','Yantai Xinchao','Zhang Xin family','Summitview Capital consortium',\
										'Huaneng','Wanxiang','HNA','Tencent','Cinda','Ningbo Joyson']

	returns: updated Tweet_Input
	'''
	##########################
	# check against keywords #
	##########################
	# temperary Set
	temp_key_set = set()
	# geo-political related <--- post mid-March, 2017
	for keyword in variables_dict['key_china']:
		if keyword in Tweet_Input['OriginalText']: 
			temp_key_set.add('china')
			break

	for keyword in variables_dict['key_korea']:
		if keyword in Tweet_Input['OriginalText']: 
			temp_key_set.add('korea')
			break

	for keyword in variables_dict['key_taiwan']:
		if keyword in Tweet_Input['OriginalText']: 
			temp_key_set.add('taiwan')
			break

	for keyword in variables_dict['key_japan']:
		if keyword in Tweet_Input['OriginalText']: 
			temp_key_set.add('japan')
			break

	for keyword in variables_dict['key_russia']:
		if keyword in Tweet_Input['OriginalText']: 
			temp_key_set.add('russia')
			break

	for keyword in variables_dict['key_mid_east']:
		if keyword in Tweet_Input['OriginalText']: 
			temp_key_set.add('mid_east')
			break

	for keyword in variables_dict['key_india']:
		if keyword in Tweet_Input['OriginalText']: 
			temp_key_set.add('india')
			break

	for keyword in variables_dict['key_europe']:
		if keyword in Tweet_Input['OriginalText']: 
			temp_key_set.add('europe')
			break

	# macro-economy related <--- post mid-March, 2017
	for keyword in variables_dict['key_Economy']:
		if keyword in Tweet_Input['OriginalText']: 
			temp_key_set.add('Economy')
			break

	# Chinese companies related <--- post mid-March, 2017
	for keyword in variables_dict['key_CNcompany']:
		if keyword in Tweet_Input['OriginalText']: 
			temp_key_set.add('CNcompany')
			break

	########################
	# convert temp_key_set #
	########################
	temp_key_str = ''
	for key in temp_key_set:
		temp_key_str += key+','

	Tweet_Input['Concerned_Keywords'] = temp_key_str
	return Tweet_Input

#######################################################################################################

if __name__ == '__main__':

	#######################################################################################################

	sentences_list = []

	data_str = """U.S. sinks to third place behind China, India as most attractive nation for renewables \
	investments https://t.co/tzocz5ZqGf """.lower().decode("utf-8").encode('ascii','ignore')
	sentences_list += nltk.sent_tokenize( data_str )

	data_str = """Top #investments in #Chinas #powerbank rental scene \
	https://t.co/TmQQyaaOfC via @allchinatech https://t.co/BDtzr6iAdI """.lower().decode("utf-8").encode('ascii','ignore')
	sentences_list += nltk.sent_tokenize( data_str )

	data_str = """India pips US on renewable energy investment, trails China: Report https://t.co/bvW1MirvmI """.lower().decode("utf-8").encode('ascii','ignore')
	sentences_list += nltk.sent_tokenize( data_str )

	data_str = """HFFs Dan Peek will speak at the Beijing Global Private Equities forum on Chinese Hotel \
	Investments in the U.S. https://t.co/W8izekjBnE #CRE """.lower().decode("utf-8").encode('ascii','ignore')
	sentences_list += nltk.sent_tokenize( data_str )

	data_str = """RT @idarose7777: #Cruelty Beyond #comprehension  And We Have to #Beg #China To #StopYulin\
	 #Beg #USA To #Boycott #Dogmeattrade . G  """.lower().decode("utf-8").encode('ascii','ignore')
	sentences_list += nltk.sent_tokenize( data_str )

	data_str = """RT @allchinatech: Top #investments in #Chinas #powerbank rental scene\
	 https://t.co/TmQQyaaOfC via @allchinatech https://t.co/BDtzr6iAdI """.lower().decode("utf-8").encode('ascii','ignore')
	sentences_list += nltk.sent_tokenize( data_str )

	data_str = """@leonalewis Seems like China isnt pulling its socks up on animal cruelty this year, ivory trade, \
	shark fin soup and n https://t.co/upVLUET2JU """.lower().decode("utf-8").encode('ascii','ignore')
	sentences_list += nltk.sent_tokenize( data_str )

	#print sentences_list

	#######################################################################################################

	for idx in range(len(sentences_list)):
		# parse negative expression
		sentences_list[idx] = Negative_Parsing(sentences_list[idx])
		# remove http, #, @ expression
		sentences_list[idx] = Remove_http_tag_user(sentences_list[idx])

	#######################################################################
	
	for sentence in sentences_list:
		print sentence



