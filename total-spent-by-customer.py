from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('customerSpend')
sc = SparkContext(conf = conf)

def parse_line(line):
    fields = line.split(',')   
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = input.map(parse_line)
total_by_cust = rdd.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey()

results = total_by_cust.collect()
for result in results:
    total_spent, cust_id = result
    print(f"Total spent {total_spent:.2f} by customer: {cust_id}")
