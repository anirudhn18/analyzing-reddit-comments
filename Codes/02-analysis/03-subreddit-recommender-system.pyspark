

# configure SQL context
from pyspark.sql import SQLContext, DataFrame
from functools import reduce

def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)


sqlCtx = SQLContext(sc)

# read the first file as dataframe df1
df1 = sqlCtx.jsonFile('file:/home/training/Downloads/datafiles/subreddits000000000000').cache()
df1.registerTempTable("subrds1")


# Producing ratings data
# load mapping files

user = sc.textFile('author_mapping').map(lambda line: line.split(','))
subr = sc.textFile('subreddit_mapping').map(lambda line: line.split(','))


# remove header and create dataframes
userh = user.take(1)[0]
subrh = subr.take(1)[0]
userRDD = user.filter(lambda line: line!=userh)
subRDD = subr.filter(lambda line: line!=subrh)
userdf = sqlCtx.createDataFrame(userRDD)
subdf = sqlCtx.createDataFrame(subRDD)


df.registerTempTable("subrds")
userdf.registerTempTable("user")
subdf.registerTempTable("subreddit")
rating = sqlCtx.sql("""
	SELECT author, subreddit, COUNT(*) as cnt
	from subrds
	where author <> '[deleted]'
	group by author, subreddit
	order by cnt desc""")

rating.registerTempTable("rating")
rating1 = sqlCtx.sql("""
	SELECT r.author, u._2 as userId, r.subreddit, s._2 as subredditId,
	r.cnt
	from rating as r
	join user as u
	on r.author = u._1
	join subreddit as s
	on r.subreddit = s._1""")
	

rRDD = rating1.rdd.map(lambda line: (line[1],line[3],line[4]))
# Prepare data for training, validation, testing
training_RDD, validation_RDD, test_RDD = rRDD.randomSplit([6, 2, 2], seed=1234)

validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))

test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

# Train ALS model
from pyspark.mllib.recommendation import ALS
import math
# Build the recommendation model using Alternating Least Squares
rank = 10
numIterations = 10
model = ALS.train(training_RDD, rank, numIterations)

# Evaluate the model on training data
predictions = model.predictAll(validation_for_predict_RDD).\
							map(lambda r: ((int(r[0]), int(r[1])), float(r[2])))
