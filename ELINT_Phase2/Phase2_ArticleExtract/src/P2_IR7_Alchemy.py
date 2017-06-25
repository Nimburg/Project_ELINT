#!/usr/bin/env python

import os
import re
import requests
import pyPdf
import glob
import json
import time
import collections as col

from alchemyapi import AlchemyAPI
Alchemy_API = AlchemyAPI()


"""
###############################################################################################################

Extract Title, Author(list), root_url, Full_Text by twt_url

###############################################################################################################
"""

def Alchemy_Extract_Article(url_str):
	'''
	returns: 
	if working: {'status':'OK', .......}
	if not working: returns {'status':'Failed'}
	'''
	print url_str

	flag_correct_url = False
	
	title = ''
	root_url = ''
	author = []
	full_text = ""

	#########################
	# Extract Title and url #
	#########################
	response_title = Alchemy_API.title(flavor='url', data=url_str)
	# check results
	if response_title['status'] == 'OK':
		title = response_title['title'].encode('utf-8')
		root_url = response_title['url'].encode('utf-8')
		if (title == 'Twitter') or title == '':
			return {'status':'Failed'}
		else:
			flag_correct_url = True
	else:
		flag_correct_url = False

	if flag_correct_url == True:

		##################
		# Extract Author #
		##################
		response_author = Alchemy_API.author(flavor='url', data=url_str)
		# check results
		author_res = None
		if response_author['status'] == 'OK':
			# one possible format returns list
			try:
				author_res = response_author['authors']['names'].encode('utf-8') # type as list
			except KeyError:
				pass
			finally:
				pass
			# other possible format returns string
			try:
				author_res = response_author['author'].encode('utf-8') # type as string
			except KeyError:
				pass
			finally:
				pass	
		if author_res is None:
			print "No Author Found"
		else:
			author.extend(author_res)

		################
		# Extract Text #
		################
		response_text = Alchemy_API.text(flavor='url', data=url_str, options={'useMetadata':0,'extractLinks':0})
		# check results
		if response_text['status'] == 'OK':
			full_text = response_text['text'].encode('utf-8')
		else:
			flag_correct_url = False

	###################################################################################
	# convert into a dict
	if flag_correct_url == True:
		return {'status':'OK','title':title,'root_url':root_url,'author':author,'full_text':full_text}
	else:
		return {'status':'Failed'}

"""
###############################################################################################################



###############################################################################################################
"""






