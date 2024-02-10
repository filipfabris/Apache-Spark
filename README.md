# Apache Spark
* Database: `menu_csv`
* Course: `SYSTEMS AND METHODS FOR BIG AND UNSTRUCTURED DATA`

* Sample Query:
```sql
from pyspark.sql.functions import sum, avg, count, max

exploded_df.groupBy("Pizza Name").agg(
    sum("Price").alias("Sum Price"),
    avg("Price").alias("Average Price"),
    count("Ingredient").alias("Number of Ingredients"),
    max("Price").alias("Price")).show(truncate = False)
```

* Sample Output:
```
+---------------+------------------+-----------------+---------------------+-----+
|Pizza Name     |Sum Price         |Average Price    |Number of Ingredients|Price|
+---------------+------------------+-----------------+---------------------+-----+
|Calzone        |23.84999942779541 |7.949999809265137|3                    |7.95 |
|Diavola        |17.84999942779541 |5.949999809265137|3                    |5.95 |
|Fries          |3.950000047683716 |3.950000047683716|1                    |3.95 |
|Prosciutto     |23.84999942779541 |7.949999809265137|3                    |7.95 |
|Margherita     |17.84999942779541 |5.949999809265137|3                    |5.95 |
|Speck & Brie   |31.799999237060547|7.949999809265137|4                    |7.95 |
|Tonno & Cipolle|31.799999237060547|7.949999809265137|4                    |7.95 |
+---------------+------------------+-----------------+---------------------+-----+
```

* Sample Query:
```sql
# Filtering w.r.t. a list of elements
favourite_pizzas = ["Speck & Brie", "Tonno & Cipolle"]

# "is in" Filtering
df.filter(col("Pizza Name").isin(favourite_pizzas)).show(truncate = False)

# "is not in" Filtering
df.filter(col("Pizza Name").isin(favourite_pizzas) == False).show(truncate = False)
```

* Sample Output:
```
+---------------+-----+-----------------------------------------------+
|Pizza Name     |Price|Ingredients                                    |
+---------------+-----+-----------------------------------------------+
|Speck & Brie   |7.95 |[Tomato Sauce, Mozzarella Cheese, Speck, Brie] |
|Tonno & Cipolle|7.95 |[Tomato Sauce, Mozzarella Cheese, Tuna, Onions]|
+---------------+-----+-----------------------------------------------+

+----------+-----+---------------------------------------------------+
|Pizza Name|Price|Ingredients                                        |
+----------+-----+---------------------------------------------------+
|Margherita|5.95 |[Tomato Sauce, Mozzarella Cheese, Basil]           |
|Calzone   |7.95 |[Tomato Sauce, Mozzarella Cheese, Prosciutto Cotto]|
|Diavola   |5.95 |[Tomato Sauce, Mozzarella Cheese, Spicy Salame]    |
|Prosciutto|7.95 |[Tomato Sauce, Mozzarella Cheese, Prosciutto Cotto]|
|Fries     |3.95 |[Potatoes]                                         |
+----------+-----+---------------------------------------------------+
```
