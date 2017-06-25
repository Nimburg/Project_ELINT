#!/usr/bin/env python
"""
Contains functions for keyword extraction using a classifer trained on the Crowd500 dataset [Marujo2012]
"""

import os
import re
import random
import numpy as np
import pickle
import string
from collections import defaultdict

import nltk
from nltk.corpus import stopwords
stoplist = stopwords.words('english')

from gensim import corpora, models, similarities

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import cross_val_score

from Extract_NEkey_features import remove_punctuation
from Extract_NEkey_features import get_namedentities, get_nounphrases, get_trigrams, extract_features


"""
################################################################################################

# Keyword Classifier, Tf-idf Model, Supervised Training

################################################################################################
"""

def get_keywordclassifier(Variables_dict, classifier_type='logistic'):
	"""
	Returns a keyword classifier trained and tested on dataset derived from Crowd500 [Marujo2012]

	preload: value=1 for saved cases; else for starting from scratch
	classifier_type: 
	returns: 
	"""  

	train_XY = pickle.load(open( Variables_dict['saved_addr']+'trainXY_crowd500.pkl','rb'))
	test_XY = pickle.load(open( Variables_dict['saved_addr']+'testXY_crowd500.pkl','rb'))    
	if classifier_type=='logistic':
		model = pickle.load(open( Variables_dict['saved_addr']+'logisticregression_crowd500.pkl','rb'))    
	else:
		model = pickle.load(open( Variables_dict['saved_addr']+'randomforest_crowd500.pkl','rb'))    
 
	##################################
	# show performance of classifier #
	##################################	
	in_sample_acc = cross_val_score(estimator=model, 
									X=train_XY['features'], 
									y=train_XY['labels'], 
									cv=4)
	out_sample_acc = cross_val_score(estimator=model, 
									 X=test_XY['features'], 
									 y=test_XY['labels'], 
									 cv=4)
	print '---------------------------------------------------------------------------------------------------------'
	if classifier_type=='logistic':
		print 'Using logistic regression model for keyword classification (0 = non-keyword, 1 = keyword)'
	else:
		print 'Using random forest model for keyword classification (0 = non-keyword, 1 = keyword)'    
	print 'Trained and tested on dataset derived from Crowd500 [Marujo2012]'
	print 'Number of features = %d, Number of training samples = %d, Number of test samples %d' % (train_XY['features'].shape[1],train_XY['features'].shape[0],test_XY['features'].shape[0])
	print 'In-sample cross-validated accuracy: %.4f, Out-of-sample cross-validated accuracy: %.4f, Chance: 0.5' % (in_sample_acc.mean(),out_sample_acc.mean())
	print '---------------------------------------------------------------------------------------------------------'
	
	return {'model': model, 'train_XY':train_XY, 'test_XY':test_XY}

"""
################################################################################################

# Extract Keywords

################################################################################################
"""

def generate_candidates(text, num_trigrams=None):
	"""
	Returns candidate words that occur in named entities, noun phrases, or top trigrams
	This function seems to overlap with codes from features.py extract_features()
	"""
	if num_trigrams is None:
		num_trigrams = 5
	
	# list of words as named_entities
	named_entities = get_namedentities(text)
	# list of (noun_phrases represented as list of words)
	noun_phrases = get_nounphrases(text)
	# list of (trigram converted to a list)
	top_trigrams = get_trigrams(text, num_trigrams)
	# list of (words, lists of words)
	return list( set.union(set(named_entities), set(noun_phrases), set(top_trigrams)) )

################################################################################################

def extract_keywords(Input_Data, keyword_classifier, Variables_dict):
	"""
	Returns top k keywords using specified keyword classifier
	"""
	#####################
	# load tf-idf model #
	#####################	
	file_addr = Variables_dict['saved_addr']+Variables_dict['saved_fileName']
	preprocessing = pickle.load(open(file_addr,'rb'))
	dictionary = preprocessing['dictionary']
	tfidf = preprocessing['tfidf_model'] 
	
	top_k = Variables_dict['top_k'] 

	####################################################
	# Process input Input_Data and extract feature_set #
	####################################################
	# list of words
	text_processed = [word for word in Input_Data.lower().split() if word not in stoplist]
	# list of single sub-list (bow vector)
	corpus = [ dictionary.doc2bow(text_processed) ]
	# here corpus is the new corpus from Input_Data, which has a single entry
	corpus_entry = tfidf[corpus][0]    
	# extract NE, NP, N-grams from Input_Data
	candidate_keywords = generate_candidates(text=Input_Data, num_trigrams=5)
	# THIS IS PROBlEMATIC
	if len(candidate_keywords) < top_k:
		candidate_keywords = text_processed   

	# select from candidate keywords ; feature_names:
	# ['Named Entity','Noun Phrase','N-gram','Term Freq','TF-IDF','Term Length',
	# 'First Occurence','Spread','Capitalized','Wikipedia frequency']
	feature_set = extract_features(text=Input_Data, 
								   candidate_keywords=candidate_keywords, 
								   corpus_entry=corpus_entry, 
								   dictionary=dictionary)
	
	##############################################
	# use feature_set to extract chosen_keywords #
	##############################################
	# keyword_classifier is either LogisticRegression or RandomForestClassifier
	# feature_set['features'] has shape [n_samples, n_features], n_samples as candidate_keywords
	# predicted_prob has shape shape = [n_samples, n_classes], here n_classes=2
	predicted_prob = keyword_classifier.predict_proba(feature_set['features'])
	# np.where(condition) returns the tuple (row_idx, column_idx) where condition is True.
	# here, np.where(...) is ( array([1], dtype=int64), ) since keyword_classifier.classes_ is [0., 1.]
	# this selects the correct column index
	this_column = np.where(keyword_classifier.classes_==1)[0][0]
	# enumerate returns tuple (idx, predicted_prob[:,this_column][]idx)
	# reverse=True means descending order over prob
	# sorted(...) ordered list of tuples, i[0] is the sorted index
	sorted_indices = [ i[0] for i in sorted( enumerate(predicted_prob[:,this_column]), key=lambda x:x[1], reverse=True ) ]
	chosen_keywords = [ candidate_keywords[j] for j in sorted_indices[:top_k] ]

	return chosen_keywords

################################################################################################







