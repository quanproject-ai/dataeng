p_path = 'G:\\Github\\deng\\Scripts'
s_path = 'G:\\SparkApp\\spark-3.5.1-bin-hadoop3'
import findspark
findspark.init(spark_home=s_path,python_path=p_path)

from pyspark import SparkContext

sc = SparkContext(appName="SampleLambda")
x = sc.parallelize([1, 2, 3, 4])
res = x.filter(lambda x: (x % 2 == 0))
print(res.collect())
sc.stop()