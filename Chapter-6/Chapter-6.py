# Code for Chapter 6


# Example 6-1
accum  = sc.accumulator(0)
sc.parallelize([1,2,3,4]).foreach(lambda x: accum.add(x))



# Example 6-2
file = sc.textFile("README.md")
# Create Accumulator[Int] initialized to 0
blankLines = sc.accumulator(0)

def extractCallSigns(line):
      global blankLines # Make the global variable accessible
      if (line == ""):
           blankLines += 1
      return line.split(" ")

callSigns = file.flatMap(extractCallSigns)
callSigns.saveAsTextFile("blankLines" + "/callsigns")
print ("Blank lines: ", blankLines.value )



# Example 6-5
# Create Accumulators for validating call signs
validSignCount = sc.accumulator(0)
invalidSignCount = sc.accumulator(0)

def validateSign(sign):
      global validSignCount, invalidSignCount
      if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
         validSignCount += 1
         return True
     else:
           invalidSignCount += 1
           return False
# Count the number of times we contacted each call sign
validSigns = callSigns.filter(validateSign)
contactCount = validSigns.map(lambda sign: (sign, 1)).reduceByKey(lambda (x, y): x + y)

# Force evaluation so the counters are populated
contactCount.count()
if invalidSignCount.value < 0.1 * validSignCount.value:
    contactCount.saveAsTextFile(outputDir + "/contactCount")
else:
      print "Too many errors: %d in %d" % (invalidSignCount.value, validSignCount.value)

        
        

# Example 6-6
accum = sc.accumulator(0)
def g(x)
        accum.add(x)
        return f(x)
data.map(g)



# Example 6-7
class VectorAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return Vector.zeros(initialValue.size)

    def addInPlace(self, v1, v2):
        v1 += v2
        return v1

# Then, create an Accumulator of this type:
vecAccum = sc.accumulator(Vector(...), VectorAccumulatorParam())



# Example 6-10
broadcastVar = sc.broadcast([1, 2, 3])
broadcastVar.value




# Example 6-11
hoods = ((1, "Mission"), (2, "SOMA"), (3, "Sunset"), (4, "Haight Ashbury"))
checkins = ((234, 1),(567, 2), (234, 3), (532, 2), (234, 4))
hoodsRdd = sc.parallelize(hoods)
checkRdd = sc.parallelize(checkins)

broadcastedHoods = sc.broadcast(hoodsRdd.collectAsMap())

rowFunc = lambda x: (x[0], x[1], broadcastedHoods.value.get(x[1], -1))
def mapFunc(partition):
    for row in partition:
        yield rowFunc(row)

checkinsWithHoods = checkRdd.mapPartitions(mapFunc,
preservesPartitioning=True)

checkinsWithHoods.take(5)
# [(234, 1, 'Mission'), (567, 2, 'SOMA'), (234, 3, 'Sunset'), (532, 2, 'SOMA'), (234, 4, 'Haight Ashbury')]





# Example 6-12
def combineCtrs(c1, c2):
       return (c1[0] + c2[0], c1[1] + c2[1])

def basicAvg(nums):
      """Compute the average"""
       nums.map(lambda num: (num, 1)).reduce(combineCtrs) 


    
    
# Example 6-13
def partitionCtr(nums):
     """Compute sumCounter for partition"""
     sumCount = [0, 0]
     for num in nums:
           sumCount[0] += num
           sumCount[1] += 1
     return [sumCount]

def fastAvg(nums):
      """Compute the avg"""
      sumCount = nums.mapPartitions(partitionCtr).reduce(combineCtrs)
      return sumCount[0] / float(sumCount[1])


    
    
# Example 6-15
path = "wordcount.txt" 
#path = "/public/randomtextwriter/part-m-00000"

sc.textFile(path).
  mapPartitions(lines => {
    // Using Scala APIs to process each partition
    lines.flatMap(_.split(" ")).map((_, 1))
  }).
  reduceByKey((total, agg) => total + agg). take(100).foreach(println)





# Example 6-17
# Convert our RDD of strings to numeric data so we can compute stats and
# remove the outliers.
distanceNumerics = distances.map(lambda string: float(string))
stats = distanceNumerics.stats()
stddev = stats.stdev()
mean = stats.mean()
reasonableDistances = distanceNumerics.filter(
     lambda x: math.fabs(x - mean) < 3 * stddev)
print reasonableDistances.collect()

