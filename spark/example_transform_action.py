from pyspark import SparkContext

sc = SparkContext("local", 'transform_action')

data = [i for i in range(0,10)]
rdd = sc.parallelize(data)

rdd2 = rdd.map(lambda x: x*2)
rdd3 = rdd2.filter(lambda x: x >5)

result = rdd3.collect()

print(result)