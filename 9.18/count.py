from pyspark import SparkContext

if __name__ == '__main__':
    sc = SparkContext("local", "First Spark")
    sc.setLogLevel("WARN")
    logFile = "test"
    logData = sc.textFile(logFile).cache()
    numAs = logData.filter(lambda s: 'a' in s).count()
    numBs = logData.filter(lambda s: 'b' in s).count()
    print(numAs, numBs)

