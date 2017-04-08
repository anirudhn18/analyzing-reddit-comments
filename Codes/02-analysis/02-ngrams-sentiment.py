

from pyspark.sql import SQLContext, DataFrame
from nltk.util import ngrams
import pandas as pd
from functools import reduce

def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)

	
sqlCtx = SQLContext(sc)

# read first five files as dataframe
df1 = sqlCtx.jsonFile('file:/home/training/Downloads/datafiles/subreddits\
												000000000000').cache()
df2 = sqlCtx.jsonFile('file:/home/training/Downloads/datafiles/subreddits\
												000000000001').cache()
df3 = sqlCtx.jsonFile('file:/home/training/Downloads/datafiles/subreddits\
												000000000002').cache()
df4 = sqlCtx.jsonFile('file:/home/training/Downloads/datafiles/subreddits\
												000000000003').cache()
df5 = sqlCtx.jsonFile('file:/home/training/Downloads/datafiles/subreddits\
												000000000004').cache()												

												
dftot = unionAll(df1,df2,df3,df4,df5)


# count total number of comments in the first 5 files


# filter out all comments under AskReddit
dftot.registerTempTable("subrds1")
text1 = sqlCtx.sql("""
	SELECT *
	from subrds1
	where subreddit = "AskReddit"""").cache()
	

# change SQL spark dataframe to panda dataframe and split ‘body’ of comments to
words list.
text2 = text1.toPandas()
a = text2['body'].str.split().tolist()

# use two dictionaries to capture the N-grams generated by ngrams function in two for loops
his = dict()
hjs = dict()
j = 0
for i in range(0,len(a)):
	bigrams = ngrams(a[i],2)
	for gram in bigrams:
		hjs[j] = gram
		j += 1
	his[i] = hjs


# group by N-grams and count the number of times each N-grams appears in the
# comments under AskReddit, then order the result descendingly
s = pd.Series(his[0],index=his[0].keys())
term = s.groupby(s).count()
term.sort_values(ascending = False)
# The same process was applied to NFL, funny, news subreddit as well