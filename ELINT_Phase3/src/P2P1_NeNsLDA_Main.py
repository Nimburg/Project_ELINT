#!/usr/bin/env python

import sys
import os
from os import listdir
from os.path import isfile, join
import re
import random
import numpy as np
import pandas as pd
import json
import pickle
import string
import collections as col

import nltk
from nltk.corpus import stopwords
stoplist = stopwords.words('english')

import gensim
from gensim import corpora, models, similarities

import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

from Extract_NEkey_Main import NEkey_Extract_Main


"""
################################################################################################

Utility Functions

################################################################################################
"""

def Search_Addr(data_addr):
	'''
	return list of file names under data_addr
	'''
	# search if there is previously constructed discussion tree
	src_addr = os.path.dirname(os.path.realpath('__file__'))
	search_addr = os.path.join(src_addr, data_addr)
	# get file names under search_addr
	fileName_list = [ f for f in listdir(search_addr) if isfile(join(search_addr, f)) ]
	return fileName_list

def Get_DataFile(file_name_list, file_idex, file_addr):
	'''
	return text from indexed data file from file_name_list
	'''
	# read tweets.txt file data
	src_addr = os.path.dirname(os.path.realpath('__file__'))
	search_addr = os.path.join(src_addr, file_addr)
	
	InputFile_addr = os.path.join(search_addr, file_name_list[file_idex]) # ../ get back to upper level
	#Inputfilename = os.path.abspath(os.path.realpath(Inputfilename))
	print file_name_list[file_idex]
	file_input = open(InputFile_addr,'r')
	text = file_input.read()
	file_input.close()
	return text

def FileName_Parse(file_Name):
	'''
	Extract datetime_str from file name
	'''
	# rid .txt
	post_Parse = file_Name[:-4]
	# parse by _
	Parsed_list = post_Parse.split('_')
	Date_str = Parsed_list[-1].lower()
	# parse Month, Day, Year
	Month_Day, Year = Date_str.split(', ')
	Month, Day = Month_Day.split(' ')
	
	# Month to Numerical dict
	Month2Digit_dict = {'january':1, 
						'februray':2,
						'march':3,
						'april':4,
						'may':5,
						'june':6,
						'july':7,
						'august':8,
						'september':9,
						'october':10,
						'november':11,
						'december':12}
	# convert Month to digist
	if Month in Month2Digit_dict:
		Month = str(Month2Digit_dict[Month])
	else:
		return None

	# combine into str format
	datetime_str = Year+'-'+Month+'-'+Day+' 00:00:00'
	return datetime_str

"""
################################################################################################

NLP Related Functions

################################################################################################
"""

def remove_punctuation(text):
	"""
	Returns text free of punctuation marks
	text: a list of words and punctuations
	returns: a string, joined up by the words
	"""
	exclude = set(string.punctuation)
	text_return = ''
	for ch in text: 
		if ch not in exclude:
			text_return += ch
		else:
			text_return += ' '
	return text_return

	#exclude = set(string.punctuation)
	#return ''.join([ch for ch in text if ch not in exclude])


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
################################################################################################

N_sentences, NE_keysearch Cycles Control

################################################################################################
"""

def N_sentence_Search(Variables_dict, full_text):
	'''
	full_text: list of (each document as single string)
	
	returns: list of (sentences as list of words)
	'''
	N_documents = len(full_text)
	# list of (sentences as list of words)
	Extracted_Sentences = []
	Extracted_NEkey_Sentences = []
	
	# go through full_text
	for article_idx in range( len(full_text) ):
		article = full_text[article_idx]

		#################################
		# tokenize into list.list.words #
		#################################
		# using nltk to tokenize sentences
		try:
			article = article.encode('utf-8')
			sentences = nltk.sent_tokenize(article.lower())        
		except:
			article = article.decode('utf-8')
			sentences = nltk.sent_tokenize(article.lower()) 
		# parse article into list of (sentences as list of words)
		sentences_words = [ [remove_punctuation(word) for word in sentence.lower().split() if word not in stoplist] 
							for sentence in sentences]
		
		#######################################
		# extract sentences from this article #
		#######################################
		N_sentences = len(sentences) 
		# list of (list of words from each sentences)
		# using stoplist to remove known irrelevant words
		sentence_idx_list = []
 		for sent_idx in range(N_sentences):
 			flag_select = False
 			# go through each sentence
 			for word in sentences_words[sent_idx]:
 				# check each word once against a hash table
 				if (word in Variables_dict['Lv0_Keys']) or (word in Variables_dict['Lv1_Keys']):
 					flag_select = True
 					break
 			# end of this sentence
 			if flag_select == True: 
 				sentence_idx_list.append(sent_idx)
		# end of this article
		# extract from this article
		for sent_idx in sentence_idx_list:
			# sentence as list of words
			each_extraction = [] 
			# sentence as string
			NEkey_extraction = []




			start = sent_idx-Variables_dict['N_SentWindow']
			if start < 0:
				start = 0
			end = sent_idx+Variables_dict['N_SentWindow']
			if end > N_sentences-1:
				end = N_sentences-1
			




			each_extraction = sentences_words[start:end]
			NEkey_extraction = sentences[start:end]
			# do not use append
			Extracted_Sentences = Extracted_Sentences + each_extraction
			Extracted_NEkey_Sentences = Extracted_NEkey_Sentences + NEkey_extraction
	# end of each article
	return Extracted_Sentences, Extracted_NEkey_Sentences

################################################################################################

def Corpus_Tfidf_Operations(document_words):
	'''
	document_words: from N_sentence_Search(Variables_dict, full_text); list of (each document as single string)
	
	returns: corpus=truncated_corpus_tfidf, id2word=dictionary; feed into LDA modeling
	'''
	#####################
	# corpus generation #
	#####################
	# word to index operations
	dictionary = corpora.Dictionary(document_words)
	# doc to bag-of-word vector
	# with each text represented as a vector/list of tuples (token_id, count)
	document_corpus = [ dictionary.doc2bow(sentence) for sentence in document_words ]
	
	# construct id2word dict
	id2word_dict = dict()
	for key in dictionary.token2id:
		val = dictionary.token2id[key]
		id2word_dict[ str(val) ] = key
	
	##################
	# Tfidf Modeling #
	##################
	# train the Tfidf model on corpus
	tfidf = models.TfidfModel(document_corpus, normalize=True)
	# obtain Tfidf transformation of corpus
	document_corpus_tfidf = tfidf[document_corpus]    
	print "\n\nDone tf-idf transformation"
	print "length of document_corpus_tfidf: %i " % len(document_corpus_tfidf)

	#########################
	# filter by Tfidf score #
	#########################
	# tf-idf filter list
	original_tfidf_list = []
	for idx in range( len(document_corpus_tfidf) ):
		for item in document_corpus_tfidf[idx]:
			original_tfidf_list.append(item)
	# total number of tokens
	N_tokens = len(original_tfidf_list)
	print "total number of tokens: %i" % N_tokens
	
	# reversed tf-idf token list
	reversed_tfidf_list = sorted(original_tfidf_list, key=lambda x: x[1], reverse=True)
	# cut the threshold
	Threshold = 1.0*Variables_dict['tfidf_threshold']*N_tokens
	reversed_tfidf_list = reversed_tfidf_list[:int(Threshold)]
	print "%i tokens left" % len(reversed_tfidf_list)
	# get lowest tf-idf score
	Threshold_tfidf_score = reversed_tfidf_list[-1][1]
	print "lowest tf-idf score is: %f" % Threshold_tfidf_score
	print "highest tf-idf score is: %f" % reversed_tfidf_list[0][1]

	# create truncated_corpus_tfidf
	truncated_corpus_tfidf = []
	# go through document_corpus_tfidf, applying filters
	for idx in range( len(document_corpus_tfidf) ):
		temp_list = []
		for item in document_corpus_tfidf[idx]:
			# check against Lv0_Keys and Lv1_Keys
			if (id2word_dict[str(item[0])] in Variables_dict['Lv0_Keys']) or (id2word_dict[str(item[0])] in Variables_dict['Lv1_Keys']):
				#print "found key: %s" % id2word_dict[str(item[0])]
				temp_list.append(item)
				continue
			# check against tf-idf score
			if item[1] >= Threshold_tfidf_score:
				temp_list.append(item)
		# add to truncated_corpus_tfidf
		truncated_corpus_tfidf.append(temp_list)
	print "length of truncated_corpus_tfidf: %i" % len(truncated_corpus_tfidf)

	return truncated_corpus_tfidf, dictionary

################################################################################################


"""
################################################################################################

Main Function

################################################################################################
"""

def P2P1_NeNsLDA_Main(Variables_dict):
	'''
	'''
	######################
	# Extract Data Files #
	######################
	# get file_name_list
	file_name_list = Search_Addr(data_addr=Variables_dict['data_addr'])
	# extract entire data set	
	full_text = []
	for idx in range(len(file_name_list)):
		text = Get_DataFile(file_name_list=file_name_list, file_idex=idx, file_addr=Variables_dict['data_addr'])
		full_text.append(text)
	
	##############################################
	# extract according to Lv0_Keys and Lv1_Keys #
	##############################################	
	# returns: list of (sentences as list of words)
	document_words, Extracted_NEkey_Sentences = N_sentence_Search(Variables_dict=Variables_dict, full_text=full_text)




	# convert document_words into a single string
	NEkey_Input = ""
	for sentence in Extracted_NEkey_Sentences:
		NEkey_Input += sentence
		NEkey_Input += " "

	#NEkey_Input = removeUtf(NEkey_Input)
	# extract NEkey from document_words
	NEkey_Extract_Main(Variables_dict=Variables_dict, Input_Data=NEkey_Input)






	########################################################
	# generate Corpus, Tf-idf transformation and filtering #
	########################################################
	truncated_corpus_tfidf, dictionary = Corpus_Tfidf_Operations(document_words=document_words)

	################
	# LDA Modeling #
	################
	# extract 100 LDA topics, using 1 pass and updating once every 1 chunk (10,000 documents)
	lda = gensim.models.ldamodel.LdaModel(corpus=truncated_corpus_tfidf, 
										  id2word=dictionary, 
										  num_topics=10, 
										  update_every=0,
										  passes=10)
	# print results
	topic_list = lda.print_topics(10)
	
	rank_list = []
	for topic in topic_list:
		print topic[0]
		print topic[1].encode('utf-8')

	return None

"""
################################################################################################

Execution

################################################################################################
"""

if __name__ == '__main__':

	##########################################################################
	
	Variables_dict = col.defaultdict()

	##########################################################################	
	# data files' related
	Variables_dict['data_addr'] = '../Data/Speech_Trump/'

	# Lv0_Keys related 
	Variables_dict['Lv0_Keys'] = set(['trade', 'tpp', 'nafta', 'job'])
	# Lv1_Keys related 
	Variables_dict['Lv1_Keys'] = set()

	# LDA Model Parameters
	Variables_dict['Fold'] = 1 # how many Lv0_keys -> N_sentences -> NE_keysearch cycles to perform
	Variables_dict['N_SentWindow'] = 3 # on both sides
	Variables_dict['tfidf_threshold'] = 1.0# dictionary cutoff threshold, by tf-idf score

	##########################################################################

	# NEkey Extraction parameters
	Variables_dict['saved_addr'] = 'NEkey_Extract/'
	Variables_dict['saved_fileName'] = 'tfidf_preprocessing.pkl'
	
	Variables_dict['top_k'] = 50 # top N keywords
	Variables_dict['classifier_type'] = 'logistic'


	##########################################################################

	P2P1_NeNsLDA_Main(Variables_dict)

	##########################################################################




