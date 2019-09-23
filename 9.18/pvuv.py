from pyspark import SparkContext

def showResult(em):
    print(em)

# 数据样例
# 7.213.213.208    吉林    2018-03-29    1522294977303    1920936170939152672    www.dangdang.com    Login

# 各页面的访问量
def pv(lines):
    sitepair = lines.map(lambda line: (line.split("\t")[5], 1))
    result1 = sitepair.reduceByKey(lambda v1, v2: (v1+v2))
    # 按照降序排序
    result2 = result1.sortBy(lambda one: one[1], ascending=False)
    result2.foreach(lambda r: showResult(r))

# 用户访问量
def uv(lines):
    # 同一个ip，要distinct排重
    sitepair = lines.map(lambda line: line.split("\t")[0] + "_" + line.split("/t")[5]).distinct()
    result = sitepair.map(lambda line: (line.split("_")[1], 1)).reduceByKey(lambda v1, v2: v1 + v2).sortBy(lambda one: one[1], ascending=False)
    result.foreach(lambda r: showResult(r))

def exceptcity(lines, city):
    # 过滤，用到filter
    sitepair = lines.filter(lambda line: line.split("\t")[1] != city).map(lambda line: line.split("\t")[0] + "_" + line.split("/t")[5]).distinct()
    result = sitepair.map(lambda line: (line.split("_")[1], 1)).reduceByKey(lambda v1, v2: v1 + v2).sortBy(lambda one: one[1], ascending=False)
    result.foreach(lambda r: showResult(r))

def getTop2city(lines):
    sites = lines[0]
    citys = lines[1]

    citydic = {}

    for city in citys:
        if city in citydic:
            citydic[city] += 1
        else:
            citydic[city] = 1

    resultlist = []
    sortedlist = sorted(citydic.items(), key=lambda kv: kv[1], reverse=True)
    if len(sortedlist) < 2:
        resultlist = sortedlist
    else:
        for i in range(2):
            resultlist.append(sortedlist[i])
    return sites, resultlist

def getTopOperation(lines):
    site_operations = lines.map(lambda line: (line.split("\t")[5], line.split("\t")[6])).groupByKey()


def getSiteTopOperation(one):
    sites = one[0]
    operations = one[1]
    operationdic = {}
    for operation in operations:
        if operation in operationdic:
            operationdic[operation] += 1
        else:
            operationdic[operation] = 1
    resultlist = []
    sortedlist = sorted(operationdic.items(), key=lambda kv: kv[1], reverse=True)
    if len(sortedlist) < 2:
        resultlist = sortedlist
    else:
        for i in range(2):
            resultlist.append(sortedlist[i])

def getTop3User(lines):



if __name__ == '__main__':
    sc = SparkContext("local", "pvuv")
    sc.setLogLevel("WARN")
    lines = sc.textFile("111")
    # 统计pv、uv
    pv(lines)
    uv(lines)
    # 统计除了某城市的uv
    exceptcity(lines, "北京")
    # 统计每个网站最活跃的top2的地区
    getTop2city(lines)
    # 统计每个网站最热门的操作
    getTopOperation(lines)
    # 统计每个网站下最活跃的top3用户
    getTop3User(lines)
