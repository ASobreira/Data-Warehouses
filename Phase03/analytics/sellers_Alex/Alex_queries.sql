-- 1. Which 5 city and seller generates the highest / lowest profit?
--- 1.1 the highest profit?
SELECT sd.seller_name, SUM(ft.profit) AS total_profit
FROM facts_table ft
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
GROUP BY sd.seller_name
ORDER BY total_profit DESC
LIMIT 5;

--- 1.2 the lowest profit?
SELECT sd.seller_name, SUM(ft.profit) AS total_profit
FROM facts_table ft
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
GROUP BY sd.seller_name
ORDER BY total_profit ASC
LIMIT 5;

-----------------------------------------------------------------------------------------------------------------------------------------------------------

-- 2. Do the customers of the best seller belong to a state/city/region with high GDP values?
--- Aux Table
SELECT gd.gdp_state, cd.customer_name, sd.seller_name 
FROM customer_dimension cd
JOIN facts_table ft ON cd.customer_key = ft.customer_key
JOIN gdp_dimension gd ON cd.state_key_customer = gd.state_key_gdp
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Gilbert Wolf', 'Hadia Bousaid', 'Chandrakant Chaudhri', 'Nicodemo Bautista', 'Derrick Snyders');

--- Now we need to get the count of customers of the best sellers per state and order them to see states with most customers from best sellers, get top 5.
SELECT gd.state, COUNT(DISTINCT cd.customer_key) AS customer_count
FROM customer_dimension cd
JOIN facts_table ft ON cd.customer_key = ft.customer_key
JOIN gdp_dimension gd ON cd.state_key_customer = gd.state_key_gdp
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Gilbert Wolf', 'Hadia Bousaid', 'Chandrakant Chaudhri', 'Nicodemo Bautista', 'Derrick Snyders')
GROUP BY gd.state
ORDER BY customer_count DESC
LIMIT 5;

--- Now lets get the states with high GDP values (AVG of 4 years) and see if they match with the states with more customers from the best sellers
SELECT state, (average_gdp_2012_billions + average_gdp_2013_billions + average_gdp_2014_billions + average_gdp_2015_billions) / 4 AS total_avg
FROM (
    SELECT state, average_gdp_2012_billions, average_gdp_2013_billions, average_gdp_2014_billions, average_gdp_2015_billions
    FROM gdp_dimension
) subquery
ORDER BY total_avg desc 
limit 5;

-----------------------------------------------------------------------------------------------------------------------------------------------------------

-- 3.1. What is the most frequent customer segment and product category / sub category of the highest seller?
--- 3.1.1 most frequent customer segment (Best Sellers)
---- Create Aux Table
SELECT cd.customer_segment, cd.customer_name, sd.seller_name
FROM customer_dimension cd
JOIN facts_table ft ON cd.customer_key = ft.customer_key
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Gilbert Wolf', 'Hadia Bousaid', 'Chandrakant Chaudhri', 'Nicodemo Bautista', 'Derrick Snyders')

---- most frequent customer segment (Best Sellers)
SELECT cd.customer_segment , COUNT(cd.customer_name) AS customer_count
FROM customer_dimension cd
JOIN facts_table ft ON cd.customer_key = ft.customer_key
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Gilbert Wolf', 'Hadia Bousaid', 'Chandrakant Chaudhri', 'Nicodemo Bautista', 'Derrick Snyders')
GROUP BY cd.customer_segment

--- 3.1.2 most frequent product category (Best Sellers)
---- Create Aux Table
SELECT pd.category, pd.sub_category, pd.product_name, sd.seller_name 
FROM product_dimension pd
JOIN facts_table ft ON pd.product_key = ft.product_key
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Gilbert Wolf', 'Hadia Bousaid', 'Chandrakant Chaudhri', 'Nicodemo Bautista', 'Derrick Snyders')
----  most frequent product category  (Best Sellers)
SELECT pd.category , COUNT(pd.product_name) AS category_count
FROM product_dimension pd
JOIN facts_table ft ON pd.product_key = ft.product_key
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Gilbert Wolf', 'Hadia Bousaid', 'Chandrakant Chaudhri', 'Nicodemo Bautista', 'Derrick Snyders')
GROUP BY pd.category
ORDER BY category_count desc;

--- 3.1.3 most frequent product sub category (Best Sellers)
SELECT pd.sub_category , COUNT(pd.product_name) AS sub_category_count
FROM product_dimension pd 
JOIN facts_table ft ON pd.product_key = ft.product_key
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Gilbert Wolf', 'Hadia Bousaid', 'Chandrakant Chaudhri', 'Nicodemo Bautista', 'Derrick Snyders')
GROUP BY pd.sub_category
ORDER BY sub_category_count desc;

										----------------------------------

-- 3.2. What is the most frequent customer segment and product category / sub category of the lowest seller?
--- 3.2.1 most frequent customer segment (Worst Sellers)
---- Create Aux Table
SELECT cd.customer_segment, cd.customer_name, sd.seller_name
FROM customer_dimension cd
JOIN facts_table ft ON cd.customer_key = ft.customer_key
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Kaoru Xun', 'katlego Akosua', 'Cansu Peynirci', 'Preecha Metharom', 'Wasswa Ahmed')
---- most frequent customer segment (Worst Sellers)
SELECT cd.customer_segment , COUNT(cd.customer_name) AS customer_count
FROM customer_dimension cd
JOIN facts_table ft ON cd.customer_key = ft.customer_key
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Kaoru Xun', 'katlego Akosua', 'Cansu Peynirci', 'Preecha Metharom', 'Wasswa Ahmed')
GROUP BY cd.customer_segment

--- 3.2.2 most frequent product category (Worst Sellers)
---- Create Aux Table
SELECT pd.category, pd.sub_category, pd.product_name, sd.seller_name 
FROM product_dimension pd
JOIN facts_table ft ON pd.product_key = ft.product_key
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Kaoru Xun', 'katlego Akosua', 'Cansu Peynirci', 'Preecha Metharom', 'Wasswa Ahmed')

----  most frequent product category  (Worst Sellers)
SELECT pd.category , COUNT(pd.product_name) AS category_count
FROM product_dimension pd
JOIN facts_table ft ON pd.product_key = ft.product_key
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Kaoru Xun', 'katlego Akosua', 'Cansu Peynirci', 'Preecha Metharom', 'Wasswa Ahmed')
GROUP BY pd.category
ORDER BY category_count desc;

--- 3.1.3 most frequent product sub category (Worst Sellers)
SELECT pd.sub_category , COUNT(pd.product_name) AS sub_category_count
FROM product_dimension pd 
JOIN facts_table ft ON pd.product_key = ft.product_key
JOIN seller_dimension sd ON ft.seller_key = sd.seller_key
WHERE sd.seller_name IN ('Kaoru Xun', 'katlego Akosua', 'Cansu Peynirci', 'Preecha Metharom', 'Wasswa Ahmed')
GROUP BY pd.sub_category
ORDER BY sub_category_count desc;

