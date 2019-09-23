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


def f(x):
    print(x)


words.foreach(f)
