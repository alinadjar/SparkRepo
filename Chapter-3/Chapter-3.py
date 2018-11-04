# Code for Chapter 3
# as of Python 2.x


# Example 3-1
 lines = sc.parallelize(["pandas", "i like pandas"])

  
# Example 3-2
inputRDD = sc.textFile("log.txt")
errorsRDD = inputRDD.filter(lambda x: "error" in x)



# Example 3-5
errorsRDD = inputRDD.filter(lambda x: "error" in x)
warningsRDD = inputRDD.filter(lambda x: "warning" in x)
badLinesRDD = errorsRDD.union(warningsRDD)



# Example 3-6
print "Input had " + badLinesRDD.count() + " concerning lines"
print "Here are 10 examples:"
for line in badLinesRDD.take(10):
       print line
        
        
        
# Example 3-8
nums = sc.parallelize([1, 2, 3, 4])
squared = nums.map(lambda x: x * x).collect()
for num in squared:
      print "%i " % (num)
        
        
# Example 3-10
sum = rdd.reduce(lambda x, y: x + y)        
        
    
        
# Example 3-12
sumCount = nums.aggregate((0, 0),
                               (lambda acc, value: (acc[0] + value, acc[1] + 1)), 
                               (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])))
avg = sumCount[0] / float(sumCount[1])



# Example 3-14
word = rdd.filter(lambda s: "error" in s)
def containsError(s):
       return "error" in s
word = rdd.filter(containsError)



# Example 3-15
class SearchFunctions(object):
      def __init__(self, query):
             self.query = query
      def isMatch(self, s):
             return self.query in s
      def getMatchesFunctionReference(self, rdd):
             # Problem: references all of "self" in "self.isMatch"		
             return rdd.filter(self.isMatch)
      def getMatchesMemberReference(self, rdd):
             # Problem: references all of "self" in "self.query"
             return rdd.filter(lambda x: self.query in x)


    
    # Example 3-16
    class WordFunctions(object):
      ...
      def getMatchesNoReference(self, rdd):
            # Safe: extract only the field we need into a local variable
            query = self.query
            return rdd.filter(lambda x: query in x)

