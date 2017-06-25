#!/usr/bin/env python
"""
Contains functions to extract features for keyword classification
"""

import re
import string
from collections import defaultdict
import numpy as np

from gensim import corpora, models, similarities
import wikiwords
import nltk
from nltk.tag import StanfordNERTagger
from nltk.collocations import *


################################################################################################

def remove_punctuation(text):
	"""
	Returns text free of punctuation marks
	text: a list of words and punctuations
	returns: a string, joined up by the words
	"""
	exclude = set(string.punctuation)
	return ''.join([ch for ch in text if ch not in exclude])

"""
################################################################################################

identify name entities, noun phrases, and ngrams

################################################################################################
"""

def get_namedentities(text):
	"""
	Returns named entities in text using StanfordNERTagger
	
	StanfordNERTagger as Stanford Named Entity Recognizer (NER) tagger
	http://nlp.stanford.edu/software/CRF-NER.shtml

	text: string
	returns: list of words as named_entities
	"""
	st = StanfordNERTagger('utils/english.conll.4class.caseless.distsim.crf.ser.gz','utils/stanford-ner.jar')   
	# list of tuples(word, tag)
	ner_tagged = st.tag(text.lower().split())     
	# extract named_entities
	named_entities = []
	if len(ner_tagged) > 0:
		# n is a tuple
		for n in ner_tagged:
			if n[1]!='O':
				named_entities.append(remove_punctuation(n[0]))
	# check against None from remove_punctuation(n[0])
	named_entities = [n for n in named_entities if n] 
	return named_entities


##########################################
# Determines Extracted Targets' POS Type #
##########################################
def get_nounphrases(text):
	"""
	Returns noun phrases in text
	text: paragraph made of sentences
	returns: list of noun_phrases represented as list of words
	"""
	grammar = r""" 
		NBAR:
				{<NN.*|JJ>*<NN.*>}  

		NP:
				{<NBAR>}
				{<NBAR><IN><NBAR>}   # from Alex Bowe's nltk tutorial
	"""    
	# regular expresion parser
	chunker = nltk.RegexpParser(grammar)
	
	# list of strings
	sentences = nltk.sent_tokenize(text.lower())
	# list of list
	sentences = [nltk.word_tokenize(sent) for sent in sentences]
	# POS tagging, list of (list of tuples)
	sentences = [nltk.pos_tag(sent) for sent in sentences]

	# extract noun_phrases
	# list of noun_phrases represented as list of words
	noun_phrases = []
	for sent in sentences:
		# regular expresion parser, check NLTK book Chapter 7
		tree = chunker.parse(sent)
		for subtree in tree.subtrees():
			if subtree.label() == 'NP': 
				# extend noun_phrases by a list of words
				# with each word as a leave from subtree
				noun_phrases.extend( [w[0] for w in subtree.leaves()] )

	# remove punctuation and check against None cases
	noun_phrases = [remove_punctuation(nphrase) for nphrase in noun_phrases]
	noun_phrases = [n for n in noun_phrases if n]    
	
	return noun_phrases

def get_trigrams(text, num_trigrams):
	"""
	Return all members of most frequent trigrams
	text: string, paragraph
	num_trigrams: threshold for 'most frequent'
	returns: list of (trigram converted to a list)
	"""
	# Collocations are expressions of multiple words which commonly co-occur. 
	trigram_measures = nltk.collocations.TrigramAssocMeasures()
	finder = TrigramCollocationFinder.from_words(text.lower().split())
	# While these words are highly collocated, the expressions are also very infrequent.
	finder.apply_freq_filter(1) # ignore trigrams that occur only once
	top_ngrams = finder.nbest(trigram_measures.pmi, num_trigrams)
	
	# list of (trigram converted to a list)
	ngrams = []
	for ng in top_ngrams:
			ngrams.extend(list(ng))    

	# check against punctuation and None cases
	ngrams = [remove_punctuation(n) for n in list(set(ngrams))]
	ngrams = [n for n in ngrams if n]
	
	return ngrams

# is the word in a named entity, noun phrase, or ngram?
def get_binaryfeature(words, selected_words):
	"""
	Returns a 0/1 encoding indicating membership in the set of selected words 
	words: list of words
	selected_words: list or set of words
	returns: feature is a list of 1 and 0, with same length as words
	"""
	# map(function_to_apply, list_of_inputs)
	# feature is a list of 1 and 0, with same length as words
	# with 1 corresponding to word in selected_words
	feature = map(lambda x: 1 if x else 0, [(w in selected_words) for w in words])
	return feature

################################################################################################

# Term Frequency, TF-IDF scores, Length, Occurance, Capitalization, etc

################################################################################################

def get_termfrequency(text, candidate_keywords):
	"""
	Returns normalized term frequency for given keywords in text
	text: string, paragraph or sentence
	candidate_keywords: list or set of keywords
	returns: normalized frequency (percentage of entire text) for all candidate keywords
	"""
	words = [remove_punctuation(w) for w in text.lower().split()]
	return [sum([1 for w in words if w==c])/float(len(words)) for c in candidate_keywords]
	#return [len(re.findall(re.escape(c),text.lower()))/float(len(words)) for c in candidate_keywords]

def get_tfidf(candidate_keywords, corpus_entry, dictionary):
	"""
	Returns tf-idf scores for keywords using a tf-idf transformation of the text containing keywords
	This function does NOT calculate tfidf scores
	https://en.wikipedia.org/wiki/Vector_space_model

	candidate_keywords: 
	corpus_entry: 
	dictionary: 
	returns: list of tfidf_scores, same length as candidate_keywords
	"""
	weights = []
	# check the corpus if exists
	if corpus_entry:
		for candidate in candidate_keywords:
			# if candidate keyword in dictionary
			if candidate in dictionary.token2id:
				# w[0] as word_id, w[1] as tfidf score of w[0] ???
				tfidf_score = [w[1] for w in corpus_entry if w[0]==dictionary.token2id[candidate]]
				# if has valid tfidf score
				if len(tfidf_score)>0:
					weights.append(tfidf_score[0])
				else:
					weights.append(0)
			else:
				weights.append(0)
	else:
		weights = [0]*len(candidate_keywords)
			
	return weights  

def get_length(candidate_keywords):
	"""
	Returns normalized number of characters in each keyword
	candidate_keywords: list of word, each word as a string
	"""
	max_chars = 50
	return [len(c)/float(max_chars) for c in candidate_keywords]

def get_position(text, candidate_keywords):
	"""
	Returns first occurence of each keyword divided by total number of words in text
	"""
	words = [remove_punctuation(w) for w in text.lower().split()]  
	position = []
	for candidate in candidate_keywords:
		occurences = [pos for pos,w in enumerate(words) if w == candidate]
		# if candidate is in text
		if len(occurences)>0:
			position.append( occurences[0]/float(len(words)) )
		# candidate not in text
		else:
			position.append(0)
					
	return position  

def get_spread(text, candidate_keywords):
	"""
	Returns the spread of each keyword in text. 
	Spread is defined as the number of words between the first and last occurence of a keyword 
	divided by the total number of words in text
	"""
	words = [remove_punctuation(w) for w in text.lower().split()]  
	spread = []
	for candidate in candidate_keywords:
		occurences = [pos for pos,w in enumerate(words) if w == candidate]
		if len(occurences)>0:
			spread.append( (occurences[-1]-occurences[0])/float(len(words)) )
		else:
			spread.append(0)
					
	return spread

def get_capitalized(text, candidate_keywords):
	"""
	Returns a 0/1 encoding indicating if any occurence of keyword included capitalization
	1 indicates at least once with capitalization
	"""
	words_original = [remove_punctuation(w) for w in text.split()]
	words_lower = [remove_punctuation(w) for w in text.lower().split()]
	
	caps = []
	for candidate in candidate_keywords:
		occurences = [pos for pos,w in enumerate(words_lower) if w == candidate]
		if len(occurences)>0:
			any_caps = sum( [ 1 for o in occurences if words_original[o]!=words_lower[o] ] )
			if any_caps>0:
				caps.append(1)
			else:
				caps.append(0)
		else:
			caps.append(0)
	
	return caps

def get_wikifrequencies(candidate_keywords):
	"""
	Return normalized word frequency for each keyword in Wikipedia
	"""
	max_frequency = wikiwords.freq('the')
	return [wikiwords.freq(w)/float(max_frequency) for w in candidate_keywords]

"""
################################################################################################

Main function for extracting features

################################################################################################
"""

def extract_features(text, candidate_keywords, corpus_entry, dictionary, num_trigrams=None):
	"""
	Returns features for each candidate keyword using: 
	(i) the original text the keywords were derived from
	(ii) tf-idf transformation of original text

	text: original text with words
	candidate_keywords: NE, NP, N-grams from text
	corpus_entry: post bag-of-word, post tf-idf transformation vector
	dictionary: from the training corpus for the model, not for the text input

	returns: 
	"""
	# setup 
	num_features = 10
	if num_trigrams is None:
		num_trigrams = 5

	########################################
	# Named Entites, Noun Phrases, N-grams #
	########################################
	
	# identify name entities, noun phrases, and ngrams
	named_entities = get_namedentities(text)
	noun_phrases = get_nounphrases(text)
	top_trigrams = get_trigrams(text, num_trigrams)

	########################
	# Statistical Features #
	########################

	# features 0-2: is the word in a named entity, noun phrase and n-gram
	# list of 1 and 0 converted to np.array
	# whether candidate_keywords in named_entities or not
	ne_feature = np.array(get_binaryfeature(words=candidate_keywords, selected_words=named_entities))
	np_feature = np.array(get_binaryfeature(words=candidate_keywords, selected_words=noun_phrases))
	ng_feature = np.array(get_binaryfeature(words=candidate_keywords, selected_words=top_trigrams))

	# feature 3: term frequency
	tf_feature = np.array(get_termfrequency(text,candidate_keywords))
	# feature 4: tf-idf score
	tfidf_feature = np.array(get_tfidf(candidate_keywords,corpus_entry,dictionary))
	
	# feature 5: term length
	length_feature = np.array(get_length(candidate_keywords))
	# feature 6: first occurence of term in text
	position_feature = np.array(get_position(text,candidate_keywords))
	# feature 7: spread of term occurrences in text
	spread_feature = np.array(get_spread(text,candidate_keywords))
	# feature 8: capitalized?
	caps_feature = np.array(get_capitalized(text,candidate_keywords))
	# feature 9: frequency of occurence in wikipedia
	wiki_feature = np.array(get_wikifrequencies(candidate_keywords))

	####################
	# collect features #
	####################

	# with features of each keyword on a row
	features = np.zeros( (len(candidate_keywords), num_features) )
	features[:,0] = ne_feature
	features[:,1] = np_feature
	features[:,2] = ng_feature
	features[:,3] = tf_feature
	features[:,4] = tfidf_feature
	features[:,5] = length_feature
	features[:,6] = position_feature
	features[:,7] = spread_feature
	features[:,8] = caps_feature
	features[:,9] = wiki_feature

	feature_names = ['Named Entity','Noun Phrase','N-gram','Term Freq','TF-IDF',\
					 'Term Length','First Occurence','Spread','Capitalized','Wikipedia frequency']
	return {'features': features, 'names': feature_names}

################################################################################################





