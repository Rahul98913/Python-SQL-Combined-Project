#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install mysql-connector-python


# In[3]:


import pandas as pd
import os 
import mysql.connector


# In[3]:


# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocatio'),
    ('payments.csv', 'payments') ,
    ('order_items.csv' , 'order_items')# Added payments.csv for specific handling
]

# Connect to the MySQL database 
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Rahulyadav@123',
    database='ecommerce'
)
cursor = conn.cursor()

# Folder containing the CSV files
folder_path = 'D:/RAHUL/IT VEDANT/SQL Ecommerce target'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

# Close the connection
conn.close()


# In[4]:


# Connect to the MySQL database 
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Rahulyadav@123',
    database='ecommerce'
)
cursor = conn.cursor()


# In[8]:


import numpy as np
import pandas as pd
import re
import seaborn as sns
import matplotlib.pyplot as plt
import mysql.connector


# In[ ]:





# In[13]:


db = mysql.connector.connect(host = "localhost" ,
                            username = "root" ,
                            password = "Rahulyadav@123" ,
                            database = "ecommerce")

cur = db.cursor()

cursor = conn.cursor()


#  # List all unique cities where customers are located.

# In[39]:


query = """ select distinct customer_city from customers"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data)

df.head()


# # Count of orders placed in 2017

# In[32]:


query = """ SELECT COUNT(*) AS orders_in_2017 FROM orders 
where year(order_purchase_timestamp) = 2017 """

cur.execute(query)

data = cur.fetchall()

"Total orders placed in 2017 are" , data[0][0]


# # 3. Find the total sales per category.

# In[58]:


query = """ select upper(products.product_category) category ,
round(sum(payments.payment_value),2) sales
from products join order_items
on products.product_id = order_items.product_id
join payments
on payments.order_id = order_items.order_id
group by category
ORDER BY sales DESC"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data , columns = ['Category', 'Sales'])

df.head(10)


# # 4. Calculate the percentage of orders that were paid in installments.

# In[34]:


query = """ select (sum(case when payment_installments >= 1 then 1 else 0 end)) / count(*) * 100 from payments """

cur.execute(query)

data = cur.fetchall()



"the percantage of order that were paid in instalments is " , data[0][0]


#  # 5. Count the number of customers from each state. 

# In[51]:


query = """ select customer_state , count(customer_id) as customer_count
from customers group by customer_state 
ORDER BY customer_count DESC"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data , columns = ['state', 'customer_count'])

plt.figure(figsize = (8,3))

plt.bar(df['state'] , df['customer_count'])

plt.xticks(rotation = 90)
plt.xlabel('states')
plt.ylabel("count of customers by states")
plt.show()


# # Calculate the number of orders per month in 2018.

# In[52]:


query = """ select monthname(order_purchase_timestamp) AS months  , count(order_id) as order_count
FROM orders 
where year(order_purchase_timestamp) = 2018 
group by months 
order by order_count DESC"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data , columns = ['months' , 'order_count'])

o = ['January' , 'February' , 'March' , 'April' , 'May' , 'June' , 'July' , 'August' , 'September' , 'October' , 'November',"December"]


ax = sns.barplot(x = df['months'] , y = df['order_count'] , data = df ,order = o)


plt.xticks (rotation = 90)
ax.bar_label(ax.containers[0])
plt.title("Count of orders by months in 2018")
plt.show()


#  # Find the average number of products per order, grouped by customer city.

# In[57]:


query = """
with count_per_order as
(select orders.order_id , orders.customer_id, count(order_items.order_id) as oc
from orders join order_items
on orders.order_id = order_items.order_id
group by orders.order_id , orders.customer_id)

select customers.customer_city , round(avg(count_per_order.oc),2) as average_orders
from customers join count_per_order
on customers.customer_id = count_per_order.customer_id
group by customers.customer_city
ORDER BY average_orders DESC
    

"""
cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data ,  columns = ['customer city' , 'average orders'])
df.head(10)


# #  Calculate the percentage of total revenue contributed by each product category.

# In[61]:


query = """select upper(products.product_category) category ,
round((sum(payments.payment_value) / (select sum(payment_value) from payments))*100 , 2)  sales
from products join order_items
on products.product_id = order_items.product_id
join payments
on payments.order_id = order_items.order_id
group by category
ORDER BY sales DESC """

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data , columns = ['Category', 'Percentage_Sales'])

df.head(10)


# #  4. Identify the correlation between product price and the number of times a product has been purchased.

# In[9]:


query = """ select products.product_category , 
count(order_items.product_id),
round(avg(order_items.price) , 2)
from products join order_items
on products.product_id = order_items.product_id
group by products.product_category """

cur.execute(query)
data = cur.fetchall()
df =  pd.DataFrame(data , columns = ['Category', "order_count" , 'price'])

arr1 = df['order_count']
arr2 = df['price']

a = np.corrcoef([arr1 , arr2])

print('the correlation between price and number of times the product has been puchased is ' ,a[0][1] )


# # Calculate the total revenue generated by each seller, and rank them by revenue.

# In[21]:


query = """ select *, dense_rank () over(order by revenue desc) as rn   
from (select order_items.seller_id , sum(payments.payment_value) as revenue
from order_items join payments
on order_items.order_id = payments.order_id
group by order_items.seller_id) as a""" 

cur.execute(query)
data = cur.fetchall() 

df = pd.DataFrame(data , columns = ['seller_id' , 'revenue' , 'rank'])
df=df.head()
sns.barplot(x='seller_id', y = 'revenue' , data = df)
plt.xticks(rotation = 90)
plt.show()


# #  Calculate the moving average of order values for each customer over their order history.

# In[23]:


query = """
select customer_id , order_purchase_timestamp , payment ,
avg(payment) over(partition by customer_id order by order_purchase_timestamp 
rows between 2 preceding and current row) as mov_avg
from
(select orders.customer_id , orders.order_purchase_timestamp,
payments.payment_value as payment
from payments join orders
on payments.order_id = orders.order_id) as a 
"""

cur.execute(query)
data = cur.fetchall()
df=pd.DataFrame(data , columns = ['customer_id' , 'order_purchase_timestamp' , 'payment' , 'mov_avg'])
df


# # Calculate the cumulative sales per month for each year.

# In[24]:


query = """ SELECT year,
       month,
       payment,
       SUM(payment) OVER (ORDER BY year, month) AS cumulative_sales
FROM (
    SELECT YEAR(orders.order_purchase_timestamp) AS year,
           MONTH(orders.order_purchase_timestamp) AS month,
           ROUND(SUM(payments.payment_value), 2) AS payment
    FROM orders
    JOIN payments ON orders.order_id = payments.order_id
    GROUP BY YEAR(orders.order_purchase_timestamp),
             MONTH(orders.order_purchase_timestamp)
    ORDER BY YEAR(orders.order_purchase_timestamp),
             MONTH(orders.order_purchase_timestamp)
) AS a
"""

cur.execute(query)
data = cur.fetchall()
df=pd.DataFrame(data , columns = ['year' , 'month' , 'payment' , 'cumulative_sales'])
df


# # Calculate the year-over-year growth rate of total sales.
# 

# In[28]:


query = """ WITH a AS (
    SELECT YEAR(orders.order_purchase_timestamp) AS years,
           ROUND(SUM(payments.payment_value), 2) AS payment
    FROM orders
    JOIN payments ON orders.order_id = payments.order_id
    GROUP BY YEAR(orders.order_purchase_timestamp)
    ORDER BY YEAR(orders.order_purchase_timestamp)
)
SELECT years,
       ((payment - LAG(payment, 1) OVER (ORDER BY years)) / LAG(payment, 1) OVER (ORDER BY years) )*100 AS payment_difference
FROM a"""

cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data , columns = ['years' , 'Y0Y % growth'])
df


# # Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase.

# In[29]:


query = """
WITH a AS (
    SELECT customers.customer_id, 
           MIN(orders.order_purchase_timestamp) AS first_order
    FROM customers
    JOIN orders ON customers.customer_id = orders.customer_id
    GROUP BY customers.customer_id
),

b AS (
    SELECT a.customer_id, 
           COUNT(DISTINCT orders.order_purchase_timestamp) AS order_count
    FROM a
    JOIN orders ON orders.customer_id = a.customer_id
    AND orders.order_purchase_timestamp > a.first_order
    AND orders.order_purchase_timestamp < DATE_ADD(a.first_order, INTERVAL 6 MONTH)
    GROUP BY a.customer_id
)

SELECT 100 * (COUNT(DISTINCT a.customer_id) / COUNT(DISTINCT b.customer_id)) AS percentage
FROM a
LEFT JOIN b ON a.customer_id = b.customer_id;
"""

cur.execute(query)
data = cur.fetchall()

data 


# # Identify the top 3 customers who spent the most money in each year.

# In[35]:


query = """ 
select years , customer_id , payment, d_rank
from
(select year(orders.order_purchase_timestamp) years,
orders.customer_id,
sum(payments.payment_value) payment,
dense_rank() over(partition by year(orders.order_purchase_timestamp) 
order by sum(payments.payment_value) desc) d_rank
from orders join payments
on payments.order_id = orders.order_id
group by year(orders.order_purchase_timestamp) ,
orders.customer_id) as a 
where d_rank <=3 """

cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data , columns = ["years" , "customer id" , "payment" , "rank"])

sns.barplot(x = 'customer id' , y ="payment" , data = df , hue = 'years')

plt.xticks(rotation = 90)
plt.show()


# In[ ]:




