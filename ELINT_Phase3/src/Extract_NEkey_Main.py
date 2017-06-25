#!/usr/bin/env python
# coding:utf-8
#__author__ = "Lavanya Sharan"


import sys
from Extract_NEkey_extraction import get_keywordclassifier, extract_keywords

"""
################################################################################################

Main Function for Unsupervised Extraction of NE Keywords

################################################################################################
"""

def NEkey_Extract_Main(Variables_dict, Input_Data):
	'''
	Input_Data: a single string of text
	'''
	###########################
	# load keyword classifier #
	###########################
	keyword_classifier = get_keywordclassifier(Variables_dict=Variables_dict , 
											   classifier_type=Variables_dict['classifier_type'])['model']

	##########################
	# extract top k keywords #
	##########################	
	top_k = Variables_dict['top_k']

	keywords = extract_keywords(Input_Data=Input_Data, 
								keyword_classifier=keyword_classifier, 
								Variables_dict=Variables_dict)  
	print '\nTOP-%d KEYWORDS returned by model: %s\n' % (top_k,', '.join(keywords))

	return None


"""
################################################################################################

Execution

################################################################################################
"""

if __name__ == '__main__':

	##########################################################################
	
	Variables_dict = col.defaultdict()

	Variables_dict['saved_addr'] = 'NEkey_Extract/'
	Variables_dict['saved_fileName'] = 'tfidf_preprocessing.pkl'
	
	Variables_dict['top_k'] = 15 # top N keywords
	Variables_dict['classifier_type'] = 'logistic'

	##########################################################################	

	NEkey_Extract_Main(Variables_dict=Variables_dict, Input_Data="")

