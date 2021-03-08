import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
import collections
import nltk
from nltk.corpus import stopwords

# Function Definitions

def remove_url_punctuations(X):
	'''
	This function will remove URLs and punctuations from a tweet's text.
	Function is adopted from a TA's video tutorial.
	'''
	url_pattern = re.compile(r'https?://\S+|www\.\S+')
	replace_url = url_pattern.sub(r'', str(X))
	punct_pattern = re.compile(r'[^\w\s]')
	no_punct = punct_pattern.sub(r'', replace_url).lower()

	return no_punct

def split_words(X):
	'''
	This function will split words in a tweet's text into tokens.
	Words are split by single spaces (" ")
	Function is adopted from a TA's video tutorial.
	'''
	split_word_list = X.split(" ")
	return split_word_list

def remove_stopwords(X):
	'''
	This function will remove stop words among word tokens.
	Stop words are based on an English nltk stop words list from nltk.corpus.stopwords.
	Function is adopted from a TA's video tutorial.
	'''
	global stop_words	# stop_words should be declared outside the function before use.
	words = []
	for word in X:
		if word not in stop_words and len(word) > 2 and word != 'nan':
			words.append(word)

	return words

def make_tokens_and_lists(dataset_file):
	'''
	This function will read tweet texts from dataset_file and return the following:
		* a pandas dataframe containing different stages of text processing
		* a list containing word tokens with no repeated words (unique)
		* a list containing word tokens with repeated words (non-unique), and
		* a collection of word tokens paired with word counts.
	dataset_file must be in the same directory as this program.
	'''

	'''
	Function will create at pandas dataframe that starts with two columns:
		* id: contains id of tweet
		* text: contains raw text of tweet
	The function will fill these columns with data from dataset_file.
	'''
	columns = ['id', 'text']
	df = pd.read_csv(dataset_file, names=columns, sep='\t')

	# Text Processing Section

	'''
	1) Apply remove_url_punctuations() to 'text' column to clean up tweets
	Cleaned up tweets get stored in new dataframe column 'cleaned_text'
	'''
	df['cleaned_text'] = df['text'].apply(remove_url_punctuations)

	'''
	2) Apply split_words() to 'cleaned_text' column to tokenize words
	Tokenized words get stored in new dataframe column 'word_list'
	'''
	df['word_list'] = df['cleaned_text'].apply(split_words)

	'''
	3) apply remove_stopwords() to 'word_list'
	Significant word tokens get stored in new dataframe column 'sig_word_list'
		* use global variable stop_words to store list of English stop words
		  for use in remove_stopwords()
	'''
	global stop_words
	stop_words = set(stopwords.words('english'))
	df['sig_word_list'] = df['word_list'].apply(remove_stopwords)

	# List and Collection Creation Section

	'''
	1) Extract list of unique words from dataframe's 'sig_word_list' column
	   word_list_unique will be used to answer Q 1.1.
	'''
	word_list_unique = (df['sig_word_list'].explode()).unique()

	# 2) Extract list of non-unique words from dataframe's 'sig_word_list' column
	word_list = list(df['sig_word_list'].explode())

	# 3) Use word_list to make a collection container datatype
	word_count_dict = collections.Counter(word_list)

	return df, word_list_unique, word_list, word_count_dict

def make_top_100_tokens_file(ranking, write_to):
	'''
	Function will write the top 100 words in ranking into file write_to.
	ranking must have words ranked in descending order before using function.
	This function is used to help answer Q 1.2.
	'''
	f = open(write_to, 'w')
	for i in range(100):
		f.write(str(i+1) + '\t' + ranking[i][0]
				+ '\t\t\t' + str(ranking[i][1]) + '\n')
	f.close()

def make_top_100_d2_tokens_higher_than_d1_file(d1_ranking, d2_ranking):
	'''
	Function will write a file containing the top 100 words in
	d2_ranking that are ranked higher than in d1_ranking.
	Both d1_ranking and d2_ranking must be ranked in descending order before
	using function.
	Function writes to 'top100d2vsd1.txt'.
	This function is used to help answer Q 1.2.
	'''
	f = open('top100d2vsd1.txt', 'w')
	count = 0
	for i in range(len(d2_ranking)):
		if count == 100:
			break
		j = 0
		while j <= i and d1_ranking[j][0] != d2_ranking[i][0]:
			j += 1
		if j > i:
			f.write(str(count + 1) + '\t' + d2_ranking[i][0] + '\n')
			count += 1
	f.close()

def make_normalized_frequency(ranking, total_words):
	'''
	Function will return a list of normalized frequencies
	of words in ranking.
	Normalized frequencies are calculating by dividing
	the frequency of a word by the total amount of words 
	in corresponding word_list of a dataset.
	This function is used to help answer Q 1.3 and 1.4.
	'''
	ranking_norm = []
	for entry in ranking:
		ranking_norm.append(entry[1]/total_words)
	return ranking_norm

def do_question_1_4(d1_ranking, d2_ranking, d1_ranking_norm, d2_ranking_norm):
	'''
	Function writes list of d2_ranking words within the top 500 ranked words that
	have higher normalized frequencies than in d1_ranking.
	d2_ranking words meeting criteria are written into 'q1_4.txt' file alongside
	difference between its D2 normalized frequency and D1 normalized frequency.
	'''
	f = open('q1_4.txt', 'w')
	for i in range(500):
		j = 0
		while j < len(d1_ranking) and d1_ranking[j][0] != d2_ranking[i][0]:
			j += 1
		if j == len(d1_ranking):
			# If D2 word is not found in d1_ranking
			f.write(str(i+1) + '\t' + d2_ranking[i][0]
					+ '\t\t\t' + str(d2_ranking_norm[i]) + '\n')
		elif j < len(d1_ranking) and d2_ranking_norm[i] > d1_ranking_norm[j]:
			# If D2 word is found in d1_ranking but has a higher normalized frequency value
			f.write(str(i+1) + '\t' + d2_ranking[i][0] 
					+ '\t\t\t' + str(d2_ranking_norm[i] - d1_ranking_norm[j]) + '\n')
	f.close()

# End of function definitions.

# Q 1.1

df1, word_list_unique1, word_list1, word_count_dict1 = make_tokens_and_lists('d1.txt')
df2, word_list_unique2, word_list2, word_count_dict2 = make_tokens_and_lists('d2.txt')

# Unique word counts per dataset are printed out.
print('Unique words in d1: ', str(len(word_list_unique1)))
print('Unique words in d2: ', str(len(word_list_unique2)))

# Q 1.2

# Make rankings for D1 and D2.
token_ranking_d1 = word_count_dict1.most_common()
token_ranking_d2 = word_count_dict2.most_common()

# Make top 100 word tokens for D1 and D2.
make_top_100_tokens_file(token_ranking_d1, 'top100d1.txt')
make_top_100_tokens_file(token_ranking_d2, 'top100d2.txt')

# Make top 100 D2 word tokens ranked higher than in D1.
make_top_100_d2_tokens_higher_than_d1_file(token_ranking_d1, token_ranking_d2)

# Q 1.3

# Make normalized frequencies of words.
norms_d1 = make_normalized_frequency(token_ranking_d1, len(word_list1))
norms_d2 = make_normalized_frequency(token_ranking_d2, len(word_list2))

# Make x-axis for each Zipf Dist.
x1 = []
for i in range(len(token_ranking_d1)):
	x1.append(i+1)
x2 = []
for i in range(len(token_ranking_d2)):
	x2.append(i+1)

# Draw D1 Zipf Dist log-log graph
plt.loglog(x1, norms_d1, 'k+')
plt.title('D1 Zipf Distribution')
plt.xlabel('Ranking')
plt.ylabel('P(r)')
plt.show()

# Draw D2 Zipf Dist log-log graph
plt.loglog(x2, norms_d2, 'k+')
plt.title('D2 Zipf Distribution')
plt.xlabel('Ranking')
plt.ylabel('P(r)')
plt.show()

## Calculate constants for D1, D2 sets via linear regression.

# 1) Calculating log values for ranks in D1 and D2.
x1_log = np.log10(x1)
x2_log = np.log10(x2)

# 2) Calculate log values for normalized frequency of words in D1 and D2.
norms_d1_log = np.log10(norms_d1)
norms_d2_log = np.log10(norms_d2)

# 3) Perform linear regression using numpy.polyfit function on D1, D2 log values.
#	* numpy.polyfit returns coefficients of equation
#		* coeff[0] = log(c)
coeff1 = np.polyfit(x1_log,norms_d1_log,1)
coeff2 = np.polyfit(x2_log,norms_d2_log,1)
# 4) Calculate constant c for D1 and D2 using 10 ** log(c) = c
c1 = 10 ** coeff1[0]	# for D1
c2 = 10 ** coeff2[0]	# for D2


print('D1 constant: ', str(c1))
print('D2 constant: ', str(c2))

### QUESTION 1.4 ###

do_question_1_4(token_ranking_d1, token_ranking_d2, norms_d1, norms_d2)