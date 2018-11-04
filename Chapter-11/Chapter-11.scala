//Code for Chapter 11


// Example 11-2
import org.apache.spark.mllib.linalg.{Vector, Vectors}

// Create a dense vector (1.0, 0.0, 3.0).
val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0))

                                 
                                 
                                 
                                 
// Example 11-4                              
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

// Create a labeled point with a positive label and a dense feature vector.
val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

// Create a labeled point with a negative label and a sparse feature vector.
val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
                                

                                 
                                 
//Example 11-6 
import org.apache.spark.mllib.linalg.Vectors

// Create the dense vector <1.0, 2.0, 3.0>; Vectors.dense takes values or an array
val denseVec1 = Vectors.dense(1.0, 2.0, 3.0)
val denseVec2 = Vectors.dense(Array(1.0, 2.0, 3.0))

// Create the sparse vector <1.0, 0.0, 2.0, 0.0>; Vectors.sparse takes the size of
// the vector (here 4) and the positions and values of nonzero entries
val sparseVec1 = Vectors.sparse(4, Array(0, 2), Array(1.0, 2.0))
                                
                                 
                                 
                                 
                                 
//Example 11-7                                  
import org.apache.spark.ml.feature.Tokenizer
val tokenizer = new Tokenizer()
    .setInputCol("text")     
    .setOutputCol("words")
val tokenizedData = tokenizer.transform(trainingData)

                                 
                                 
                                 
     
                                 
                                 
                                 
//Example 11-8 
import org.apache.spark.ml.feature.RegexTokenizer
val rtokenizer = new RegexTokenizer()
    .setInputCol("text ")
    .setOutputCol("words")
    .setPattern(" ") // starting simple
    .setToLowercase(true)
val tokenizedData = rtokenizer.transform(trainingData)
                                 
                                 
                                 
 
                                 
//Example 11-9                                  
import org.apache.spark.ml.feature.StopWordsRemover

val englishStopWords = StopWordsRemover
    .loadDefaultStopWords("english")
val stops = new StopWordsRemover()
    .setStopWords(englishStopWords)
    .setInputCol("words")
stops.transform(tokenizedData)

                                 
                                 
 


                                 
//Example 11-13                                   
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD

val observations = sc.parallelize(
  Seq(
    Vectors.dense(1.0, 10.0, 100.0),
    Vectors.dense(2.0, 20.0, 200.0),
    Vectors.dense(3.0, 30.0, 300.0)
  )
)

// Compute column summary statistics.
val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
println(summary.mean)  // a dense vector containing the mean value for each column
println(summary.variance)  // column-wise variance
println(summary.numNonzeros)  // number of nonzeros in each column


val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))  // a series
// must have the same number of partitions and cardinality as seriesX
val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 555))

// compute the correlation using Pearson's method. Enter "spearman" for Spearman's
// method. If a method is not specified, Pearson's method will be used by default.

val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
println(s"Correlation is: $correlation")

val data: RDD[Vector] = sc.parallelize(
  Seq(
    Vectors.dense(1.0, 10.0, 100.0),
    Vectors.dense(2.0, 20.0, 200.0),
    Vectors.dense(5.0, 33.0, 366.0))
)  // note that each Vector is a row and not a column

// calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method
// If a method is not specified, Pearson's method will be used by default.
val correlMatrix: Matrix = Statistics.corr(data, "pearson")
println(correlMatrix.toString)

                                 
                                 
                                 
 
                                 
                                 
// Example 11-14
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

// Load and parse the data
val data = sc.textFile("data/mllib/ridge-data/lpsa.data")
val parsedData = data.map { line =>
  val parts = line.split(',')
  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1)
                               .split(' ').map(_.toDouble)))
}.cache()

// Building the model
val numIterations = 100
val stepSize = 0.00000001
val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

// Evaluate model on training examples and compute training error
val valuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{ case(actual, predicted) => 
                       math.pow((actual - predicted), 2) }.mean()
println(s"training Mean Squared Error $MSE")

// Save and load model
model.save(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
val sameModel = LinearRegressionModel.load(sc, "target/tmp/scalaLinearRegressionWithSGDModel")

val savedLRModel = LinearRegressionModel.load(sc, " target/tmp/scalaLinearRegressionWithSGDModel ")

// check the model parameters
val intercept = savedLRModel.intercept()

val weights = savedLRModel.weights()
val lrModelPMML = model.toPMML()

                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
// Example 11-15
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

// Load training data in LIBSVM format.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

// Split data into training (60%) and test (40%).
val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

// Run training algorithm to build the model
val numIterations = 100
val model = SVMWithSGD.train(training, numIterations)

// Clear the default threshold.
model.clearThreshold()

// Compute raw scores on the test set.
val scoreAndLabels = test.map { point =>
  val score = model.predict(point.features)
  (score, point.label)
}

// Get evaluation metrics.
val metrics = new BinaryClassificationMetrics(scoreAndLabels)
val auROC = metrics.areaUnderROC()

println(s"Area under ROC = $auROC")

// Save and load model
model.save(sc, "target/tmp/scalaSVMWithSGDModel")
val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")

                                 
                                 
                                 
                                 
                                 
                                 
                                 
 // Example 11-17                               
                                 
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
val spam = sc.textFile("spam.txt")
val normal = sc.textFile("normal.txt")
// Create a HashingTF instance to map email text to vectors of 10,000 features.
val tf = new HashingTF(numFeatures = 10000)
// Each email is split into words, and each word is mapped to one feature.
val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
val normalFeatures = normal.map(email => tf.transform(email.split(" ")))
// Create LabeledPoint datasets for positive (spam) and negative (normal) examples.
val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
val trainingData = positiveExamples.union(negativeExamples)
trainingData.cache() // Cache since Logistic Regression is an iterative algorithm.
// Run Logistic Regression using the SGD algorithm.
val model = new LogisticRegressionWithSGD().run(trainingData)
// Test on a positive example (spam) and a negative one (normal).
val posTest = tf.transform(
   "O M G GET cheap stuff by sending money to ...".split(" "))
val negTest = tf.transform(
   "Hi Dad, I started studying Spark the other ...".split(" "))
println("Prediction for positive test example: " + model.predict(posTest))
println("Prediction for negative test example: " + model.predict(negTest))


                                 
                                 
// Example 11-19                                 
import org.apache.spark.mllib.classification.NaiveBayes

val data= // ..
// Split data into training (60%) and test (40%).
val Array(training, test) = data.randomSplit(Array(0.6, 0.4))
val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()


                                 
        
                                 
                                 
                                 
// Example 11-20                                
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))
// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 32
val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
// Evaluate model on test instances and compute test error
val labelAndPreds = testData.map { point => 
   val prediction = model.predict(point.features)
   (point.label, prediction)
}
val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
println(s"Test Error = $testErr")


                                 
                                 
                                 


                                 
                                 
                                 
// Example 11-22
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel

# Split the data into training and test sets (30% held out for testing)
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "variance"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainRegressor(trainingData,categoricalFeaturesInfo, impurity, maxDepth, maxBins)

                                 
                                 
                                 

                                 
                                 
 // Example 11-24                                
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("data/mllib/kmeans_data.txt")
val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

// Cluster the data into two classes using KMeans
val numClusters = 2
val numIterations = 20
val clusters = KMeans.train(parsedData, numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)
println(s"Within Set Sum of Squared Errors = $WSSSE")

// Save and load model
clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
 

val obs1 = Vectors.dense(9.0, 9.0, 9.0)
val clusterIndex1 = kMeansModel.predict(obs1)

val clusterIndicesRDD = kMeansModel.predict(vectors)
val clusterIndices = clusterIndicesRDD.collect()

// Export to PMML to a String in PMML format
println(s"PMML Model:\n ${clusters.toPMML}")

// Export the model to a local file in PMML format
clusters.toPMML("/tmp/kmeans.xml")

// Export the model to a directory on a distributed file system in PMML format
clusters.toPMML(sc, "/tmp/kmeans")

// Export the model to the OutputStream in PMML format
clusters.toPMML(System.out)


         
                                 
                                 
 // ALS related Code Snippets:
                                 
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
// Create a RDD[String] from a dataset
val lines = sc.textFile("data/mllib/als/test.data")
// Transform each text line into a Rating
val ratings = lines.map {line =>
val Array(user, item, rate) = line.split(',')
Rating(user.toInt, item.toInt, rate.toDouble)
}
val rank = 10
val numIterations = 10
// Train a MatrixFactorizationModel
val mfModel = ALS.train(ratings, rank, numIterations, 0.01)
val userId = 1
val prodId = 1
val predictedRating = mfModel.predict(userId, prodId)
//--------------------------------------------------------------     
                                 
                                 
                                 
// Create a RDD of user-product pairs
val usersProducts = ratings.map { case Rating(user, product, rate) => (user, product) }
// Generate predictions for all the user-product pairs
val predictions = mfModel.predict(usersProducts)
// Check the first five predictions
val firstFivePredictions = predictions.take(5)
//--------------------------------------------------------------


                                 
val userId = 1
val numProducts = 3
// to recommend the specified number of products for a given user
val recommendedProducts = mfModel.recommendProducts(userId, numProducts)
//--------------------------------------------------------------

    
                                 
                                 

// to recommend the specified number of top products for all users
val numProducts = 2
val recommendedProductsForAllUsers = mfModel.recommendProductsForUsers(numProducts)
//--------------------------------------------------------------

                                 
   
                                 
                                 
// to recommend the specified number of users for a given product
val productId = 2
val numUsers = 3
val recommendedUsers = mfModel.recommendUsers(productId, numUsers)
//--------------------------------------------------------------
                                 


                                 
// to recommend the specified number of users for all products.  It takes the number of users to recommend as an argument and 
// returns an RDD of products and
corresponding top recommended users.
val numUsers = 2
val recommendedUsersForAllProducts = mfModel.recommendUsersForProducts(numUsers)
val ruFor4Products = recommendedUsersForAllProducts.take(4)
//--------------------------------------------------------------
                                 
                                 

                                 
                                 
// Example 11-26
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

// A class to represent documents -- will be turned into a SchemaRDD
case class LabeledDocument(id: Long, text: String, label: Double)
val documents = // (load RDD of LabeledDocument)
val sqlContext = new SQLContext(sc)
import sqlContext._

// Configure an ML pipeline with three stages: tokenizer, tf, and lr; each stage
// outputs a column in a SchemaRDD and feeds it to the next stage's input column
val tokenizer = new Tokenizer() // Splits each email into words
                                       .setInputCol("text")
                             .setOutputCol("words")
val tf = new HashingTF() // Maps email words to vectors of 10000 features
                             .setNumFeatures(10000)
                             .setInputCol(tokenizer.getOutputCol)
                             .setOutputCol("features")
val lr = new LogisticRegression() // Uses "features" as inputCol by default
val pipeline = new Pipeline().setStages(Array(tokenizer, tf, lr))

// Fit the pipeline to the training documents
val model = pipeline.fit(documents)

// Alternatively, instead of fitting once with the parameters above, we can do a
// grid search over some parameters and pick the best model via cross-validation
val paramMaps = new ParamGridBuilder()
                             .addGrid(tf.numFeatures, Array(10000, 20000))
                             .addGrid(lr.maxIter, Array(100, 200))
                             .build() // Builds all combinations of parameters
val eval = new BinaryClassificationEvaluator()
val cv = new CrossValidator()
                             .setEstimator(lr)
                             .setEstimatorParamMaps(paramMaps)
                             .setEvaluator(eval)
val bestModel = cv.fit(documents)
                                 
                                 
                                 
                                 
                                 
                                 
                                 
