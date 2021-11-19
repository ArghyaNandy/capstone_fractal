from pyspark.sql.types import *

#1 Aisles Schema:-
aisles_schema= StructType([StructField('aisle_id',IntegerType(),False),
                          StructField('aisle',StringType(),True)])

#2 Departments_schema:-
department_schema=StructType([StructField('department_id',IntegerType(),False),StructField('department',StringType(),True)])
#3 order_schema:-
orders_schema=StructType([StructField('order_id',IntegerType(),False),StructField('user_id',IntegerType(),True),
                              StructField('eval_set',StringType(),True),StructField('order_number',IntegerType(),True),
                              StructField('order_dow',IntegerType(),True),StructField('order_hour_of_day',IntegerType(),True),
                              StructField('days_since_prior_order',IntegerType(),True)])

#4 prior_order_schema and train_order_schema:-
prior_order_schema=StructType([StructField('order_id',IntegerType(),True),StructField('product_id',IntegerType(),True),
                              StructField('add_to_cart_order',IntegerType(),True),StructField('reordered',IntegerType(),True)])
#5 Products_schema:-
products_schema=StructType([StructField('product_id',IntegerType(),False),StructField('product_name',StringType(),True),
                              StructField('aisle_id',StringType(),True),StructField('department_id',StringType(),True)])

dataset_path='/home/fai10096/Data_sets/'


output_path=dataset_path+"/output/"           

#aisles
aisles_df = spark.read.schema(aisles_schema).option("delimeter",",").option("header","True").csv(dataset_path+'aisles.csv')

#departments:-
department_df = spark.read.schema(department_schema).option("header","True").csv(dataset_path+'departments.csv')
#orders:-
orders_df = spark.read.schema(orders_schema).option("header","True").csv(dataset_path+'orders.csv')

#prior_order:-
prior_order_df = spark.read.schema(prior_order_schema).option("header","True").csv(dataset_path+'prior_order.csv')

#products:- reading products file as rdd as it has some noises later on it has been converted to data frame after removing noises. 
#All other files have been read as csv
products_rdd = spark.sparkContext.textFile(dataset_path+'products.csv')

#train_order:-
train_order_df= spark.read.schema(prior_order_schema).option("header","True").csv(dataset_path+'train_order.csv')

#after removing noises we will convert it to dataframe
def remove_noise(row):
    if '"' in row:
        first=row.index('"')
        last=row.index('"',first+1)
        part_a=row[0:first]
        part_b=row[first:last+1].replace(", "," - ").replace('"','')
        part_c=row[last+1:]
        row=(part_a+part_b+part_c).replace('\"',"").split(",")
        return [int(row[0]),row[1],row[2],row[3]]
    else:
        row = row.replace('\"',"").split(",")
        return [int(row[0]),row[1],row[2],row[3]]

header=products_rdd.first()
products_rdd_mo=products_rdd.filter(lambda x : x!=header).map(lambda x : remove_noise(x))
products_df=products_rdd_mo.toDF(products_schema) # product dataframe creation from product rdd after removing noises.     

#showing columns of all data frames:
print('Showing columns of all data frames:')
print('\naisles :\n',aisles_df.columns)
print('\nproducts :\n',products_df.columns)
print('\ndepartments :\n',department_df.columns)
print('\norders :\n',orders_df.columns)
print('\nprior order :\n',prior_order_df.columns)
print('\ntrain order :\n',train_order_df.columns)

#showing Data types of all data frames:
aisles_df.printSchema()
department_df.printSchema()
products_df.printSchema()
orders_df.printSchema()
prior_order_df.printSchema()
train_order_df.printSchema()

#Creating Tables from dataframes for aggregation purposes:-
aisles_df.createOrReplaceTempView('aisles') # aisles table
department_df.createOrReplaceTempView('department') # department table
orders_df.createOrReplaceTempView('orders') # orders table
prior_order_df.createOrReplaceTempView('prior_order') #prior_order table
products_df.createOrReplaceTempView('products') #products table
train_order_df.createOrReplaceTempView('train_order') #train_order table

#Displaying the records from tables
print('\nDisplaying records from tables')
spark.sql("select * from aisles").show(2)
spark.sql("select * from department").show(2)
spark.sql("select * from orders").show(2)
spark.sql("select * from prior_order").show(2)
spark.sql("select * from products").show(2)
spark.sql("select * from train_order").show(2)

aggregated_table_part_1 =spark.sql('''SELECT p.product_id, product_name, aisle_id, department_id, order_id, add_to_cart_order, reordered
                                      FROM products p INNER JOIN train_order to ON to.product_id=p.product_id
                                      UNION ALL
                                      SELECT p.product_id, product_name, aisle_id, department_id, order_id,add_to_cart_order,reordered
                                      FROM products p INNER JOIN prior_order po ON po.product_id=p.product_id''')

#creating table from aggregated_table_part_1 dataframe for further aggregation
aggregated_table_part_1.createOrReplaceTempView("Combined_table")

#aggregating all tables as per the data model
combined_table = spark.sql('''SELECT product_id, product_name, t.aisle_id,aisle, d.department_id, department, o.order_id, user_id, 
                                    add_to_cart_order, reordered,eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order
                                   FROM Combined_table t 
                                   INNER JOIN orders o ON o.order_id=t.order_id 
                                   INNER JOIN aisles a ON a.aisle_id=t.aisle_id
                                   INNER JOIN department d ON d.department_id=t.department_id''')

combined_table.coalesce(1).write.option("header",True).csv(output_path)                                   