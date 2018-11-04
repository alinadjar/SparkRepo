# Code for Chapter 4


# Example 4-1
pairs = lines.map(lambda x: (x.split(" ")[0], x))

#Example 4-3
result = pairs.filter(lambda keyValue: len(keyValue[1]) < 20)

#Example 4-5
rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

#Example 4-7
rdd = sc.textFile("s3://...")
words = rdd.flatMap(lambda x: x.split(" "))
result = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y) 

#Example 4-9
r = sc.parallelize([(1,2),(3,4),(5,6),(7,8)])
r2 = r.mapValues(lambda y: y+1)
r2.collect()
# [(1, 3), (3, 5), (5, 7), (7, 9)]
r3 = sc.parallelize([(1,8)])

r4 = r2.union(r3)
r4.collect()
# [(1, 3), (3, 5), (5, 7), (7, 9), (1, 8)]

sumCount = r4.combineByKey((lambda x: (x,1)),
                        (lambda x, y: (x[0] + y, x[1] + 1)),
                         (lambda x, y: (x[0] + y[0], x[1] + y[1])))
sumCount.collect()
# [(1, (11, 2)), (3, (5, 1)), (5, (7, 1)), (7, (9, 1))]
sumCount.map(lambda key, xy: (key, xy[0]/xy[1])).collectAsMap() 
# [(1, (11, 2)), (3, (5, 1)), (5, (7, 1)), (7, (9, 1))]

#Example 4-11
data = [("a", 3), ("b", 4), ("a", 1)]
sc.parallelize(data).reduceByKey(lambda x, y: x + y) # Default parallelism
sc.parallelize(data).reduceByKey(lambda x, y: x + y, 10) # Custom parallelism 


#Example 4-13
>>> fractions = {"a": 0.2, "b": 0.1}
>>> rdd = sc.parallelize(fractions.keys()).cartesian(sc.parallelize(range(0, 1000)))
>>> sample = dict(rdd.sampleByKey(False, fractions, 2).groupByKey().collect())
>>> 100 < len(sample["a"]) < 300 and 50 < len(sample["b"]) < 150
True
>>> max(sample["a"]) <= 999 and min(sample["a"]) >= 0
True


#Example 4-16
rdd.sortByKey(ascending=True, numPartitions=None, keyfunc = lambda x: str(x))


nums = sc.parallelize([5,1,9,6,8])
result = nums.sortBy(lambda x: x)
result.collect()
# [1, 5, 6, 8, 9]

#Example 4-23
import urlparse
def hash_domain(url):
    return hash(urlparse.urlparse(url).netloc)
rdd.partitionBy(20, hash_domain) # Create 20 partitions

