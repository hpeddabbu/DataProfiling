
# Data-Profiling
dftables = sqlContext.tables('default')
print "To get Database  Tables and Row Counts"
df1 = sqlContext.sql("Use default")
arr = []
for t in dftables.collect():
    #print t.tableName
    query = "select " + '"' +t.tableName + '"' + " as tableName, count(*) as num_rows from " + t.tableName
    #print query
    df2 = sqlContext.sql(query)
    for r in df2.collect():
        df2.show()
        arr += [[r.tableName,r.num_rows]]

df = sc.parallelize(arr).toDF(["TableName","TableCount"])
df.show()
