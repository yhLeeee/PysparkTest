from pyspark import SparkContext
sc = SparkContext("local", "CollectTest")
words = sc.parallelize(
    ["scala",
     "java",
     "hadoop",
     "spark",
     "akka",
     "spark vs hadoop",
     "pyspark",
     "pyspark and spark"
     ])

words_map = words.map(lambda x: (x, 1))
print(words_map.collectAsMap())
