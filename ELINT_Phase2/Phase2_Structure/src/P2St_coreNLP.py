
import re
import collections as col

import nltk

from coreNLP_API import StanfordCoreNLP_mod

#######################################################################################################

def P2St_NP_Structure(sentence_input):
	'''
	input: single sentence 
	preprocessed as: .lower().decode("utf-8").encode('ascii','ignore')
	with additional preprocessing by P2St_TextFilter.py

	return: list of str, list of NP
	'''
	
	#######################################################################
	# API 
	nlp = StanfordCoreNLP_mod('http://localhost:9000')

	#######################################################################
	# check on each sentence
	#print "\nworking on:"
	#print sentence_input
	
	# return value, list of lists
	List_Sentence_NPs = []
	# input
	text = (sentence_input)
	output = nlp.annotate(text, properties={
		'annotators': 'tokenize,ssplit,pos,depparse,parse',
		'outputFormat': 'json'
	})
	try:
		results = output['sentences'][0]
	except IndexError:
		return []
	
	#######################################################################		
	
	###############
	# Tokens Dict #
	###############
	#print output['sentences'][0]['tokens']
	# convert list of dicts to layered dicts
	Tokens_dict = col.defaultdict()
	Tokens_dict['word_tokens_set'] = set()
	for item in results['tokens']:
		Tokens_dict[item['word']] = col.defaultdict()
		Tokens_dict[item['word']] = item # results['tokens'] is a list of non-repeating dicts
		# update set()
		try:
			Tokens_dict['word_tokens_set'].add(item['word'])
		except AttributeError:
			pass

	#######################################################################	
	
	##############
	# Extract NP #
	##############
	# start index for (NP, (, ), NN
	LPNP_Index_List = [m.start() for m in re.finditer('\(NP', results['parse'])]
	#NN_Index_List = [m.start() for m in re.finditer("NN", results['parse'])]
	LP_Index_List = [m.start() for m in re.finditer("\(", results['parse'])]
	RP_Index_List = [m.start() for m in re.finditer("\)", results['parse'])]

	#######################################################################
	# LPRP value list
	LPRP_values = [0]*len(results['parse'])
	# add LP as -1, RP as +1
	for idx in LP_Index_List:
		LPRP_values[idx] = -1
	for idx in RP_Index_List:
		LPRP_values[idx] = 1

	#######################################################################
	# extract NP
	NP_list = []
	for LPNP_index in LPNP_Index_List:
		LPRP_sum = 0
		for idx in range(LPNP_index, len(LPRP_values)):
			LPRP_sum += LPRP_values[idx]
			if LPRP_sum == 0:
				NP_list.append( (LPNP_index, idx) )
				break
	
	#######################################################################
	NP_str_list = []
	for pair in NP_list:
		temp_str = results['parse'][ pair[0]:pair[1]+1 ]
		# remove (, ), \n
		temp_str = temp_str.replace("(", "")
		temp_str = temp_str.replace(")", "")
		temp_str = temp_str.replace("\n", "")
		temp_str = temp_str.replace("\r", "")
		NP_str_list.append(temp_str)

	# extract words without tag
	for idx in range(len(NP_str_list)):
		NP_str = NP_str_list[idx]
		temp_str_list = NP_str.split(' ')
		temp_str = ""
		for word in temp_str_list: 
			if word in Tokens_dict['word_tokens_set']:
				temp_str += word+' '
		NP_str_list[idx] = temp_str
	# check on each sentence
	#print NP_str_list
	# end of processing for each sentence
	return NP_str_list


#######################################################################################################

if __name__ == '__main__':

	#######################################################################################################

	sentences_list = []

	data_str = """Why The Chinese Yuan Will Not Be The Worlds Reserve Currency""".lower().decode("utf-8").encode('ascii','ignore')
	sentences_list += nltk.sent_tokenize( data_str )

	data_str = """India pip United State on renewable energy investment, trails China: Report """.lower().decode("utf-8").encode('ascii','ignore')
	sentences_list += nltk.sent_tokenize( data_str )

	data_str = """HFFs Dan Peek will speak at the Beijing Global Private Equities forum on Chinese Hotel Investments in the U.S. #CRE""".lower().decode("utf-8").encode('ascii','ignore')
	sentences_list += nltk.sent_tokenize( data_str )

	#######################################################################################################
	res = P2St_NP_Structure(sentence_list=sentences_list)

	for NPs in res: 
		print NPs





