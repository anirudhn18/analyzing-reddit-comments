


# configure SQL context
from pyspark.sql import SQLContext, DataFrame
from functools import reduce

def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)


sqlCtx = SQLContext(sc)

# read the first file as dataframe df1
df1 = sqlCtx.jsonFile('file:/home/training/Downloads/datafiles/subreddits000000000000').cache()
df1.registerTempTable("subrds1")


# calculate the total number of subreddits commented in 2015 (PS: Since many
# “author”s are noted as “deleted”, so we removed those rows to check the number of
# valid subreddits.)

numofsub = sqlCtx.sql("""
	SELECT distinct subreddit
	from subrds
	where author <> '[deleted]'""").cache()
	
numofsub.count()


# generate dataframe topDF1 about subreddit, number of comments under that
# subreddit, average score of comments under that subreddit for the first file


topDF1 = sqlCtx.sql("""
	SELECT subreddit, COUNT(*) as cnt, AVG(score) as avgScore
	from subrds1
	where author <> '[deleted]'
	group by subreddit
	order by cnt desc""").cache()


# The same process was run 16 times by reading different files to dataframes
# sequentially from subreddits000000000000 to subreddits000000000015 and creating
# dataframes from topDF1 to topDF16.



topcom = unionAll(topDF1,topDF2,topDF3,topDF4,topDF5,topDF6,topDF7,topDF8,topDF9, 
topDF10,topDF11,topDF12,topDF13,topDF14,topDF15,topDF16).cache()

sqlCtx = SQLContext(sc)



# topcom is the combined dataframe, from which we need to get the total number of
# comments for each subreddit and the average score of comments under that subreddit.

topcom.registerTempTable("topcom")
topcomDF = sqlCtx.sql("""
	SELECT subreddit, sum(cnt) as totcnt, AVG(avgScore) as avg16Score
	from topcom
	group by subreddit
	order by totcnt desc""").cache()
	

# pick subreddits with top 20 number of comments from topcomDF
topcomDF.registerTempTable("topcomDF")
top20df = sqlCtx.sql("""
	SELECT *
	from topcomDF
	limit 20""").cache()
	