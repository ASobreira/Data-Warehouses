# Phase 3 - ETL System
#- Grupo 1
#    - Tiago Rodrigues (49593)
#    - Ivo Oliveira (50301)
#    - Martim Silva (51304)
#    - Alexandre Sobreira (59451)


# Librarys
import pandas as pd
import psycopg2 as pg
import time
import numpy as np


# Functions
def get_season(date):
    if date.month in [12, 1, 2]:
        return 'Winter'
    elif date.month in [3, 4, 5]:
        return 'Spring'
    elif date.month in [6, 7, 8]:
        return 'Summer'
    else:
        return 'Autumn'
    
def get_semester(date):
    if date.month in [1, 2, 3, 4, 5, 6]:
        return '1'
    else:
        return '2' 

def get_weekend_indicator(day_of_week):
    if day_of_week < 5:
        return 'Non-Weekend'
    else:
        return 'Weekend'
    
def get_holiday_indicator(Holiday_Key):
    if Holiday_Key != 999:
        return 'Holiday'
    else:
        return 'Non-Holiday'

    
            ######################################## ETL ########################################
start_total = time.time()

# Extraction
start = time.time()

orders = pd.read_excel('orders_RAW.xlsx')
orders = pd.DataFrame(orders)
customers_USA = pd.read_csv('customers_USA_RAW.csv', encoding='latin1')
customers_USA = pd.DataFrame(customers_USA)
returns = pd.read_csv('returns_RAW.csv', encoding='latin1')
returns = pd.DataFrame(returns)
sellers = pd.read_csv('sellers_RAW.csv', delimiter=';')
sellers = pd.DataFrame(sellers)
gdp = pd.read_csv("GDP_USA_RAW.csv", delimiter=",", encoding="windows-1252")
gdp = pd.DataFrame(gdp)
holiday = pd.read_csv("holiday_USA_RAW.csv", delimiter=",", encoding="windows-1252")
holiday = pd.DataFrame(holiday)

end = time.time()
print("Extraction took: ", end - start, " seconds")

                    ############################# Transformation #############################

start = time.time()
                                         ###### Holiday_USA Table ######
                                
# Add new Row for "No Holiday"
new_row = pd.DataFrame({
    "Date": ['2012-01-01'],
    "Holiday": ["No_Holiday"],
    "WeekDay": ['No_Holiday'],
    "Month": [1],
    "Day": [1],
    "Year": [2012]
})
holiday = pd.concat([new_row, holiday], ignore_index=True)

# Filter the years that are not between 2012 and 2015
holiday = holiday[holiday['Year'].isin([2012, 2013, 2014, 2015])]

# Convert the column Date to a date data type
holiday['Date'] = pd.to_datetime(holiday['Date'], infer_datetime_format = True)

# Correct  " New Yearâ€™s Eve " & " Valentineâ€™s Day " 
holiday["Holiday"] = holiday["Holiday"].replace({
    "New Yearâ€™s Eve": "New Years Eve",
    "Valentineâ€™s Day": "Valentines Day"
})

                                        ######  Orders Table ###### 
                                
# Convert Row ID and Quantity to integer data type
orders['Row ID'] = orders['Row ID'].astype(int)
orders['Quantity'] = orders['Quantity'].astype(int)

# Convert Order ID, Customer ID, Customer Name, City, State, Country, Region, Market, Product ID and Product Name to string data type
orders['Order ID'] = orders['Order ID'].astype(str)
orders['Customer ID'] = orders['Customer ID'].astype(str)
orders['Customer Name'] = orders['Customer Name'].astype(str)
orders['City'] = orders['City'].astype(str)
orders['State'] = orders['State'].astype(str)
orders['Country'] = orders['Country'].astype(str)
orders['Region'] = orders['Region'].astype(str)
orders['Market'] = orders['Market'].astype(str)
orders['Product ID'] = orders['Product ID'].astype(str)
orders['Product Name'] = orders['Product Name'].astype(str)

# Convert Ship Date and Order Date to date data type
orders['Ship Date'] = pd.to_datetime(orders['Ship Date'])
orders['Order Date'] = pd.to_datetime(orders['Order Date'])

# Convert Segment, Category, Sub-Category and Order Priority to category data type
orders['Segment'] = orders['Segment'].astype('category')
orders['Category'] = orders['Category'].astype('category')
orders['Sub-Category'] = orders['Sub-Category'].astype('category')
orders['Order Priority'] = orders['Order Priority'].astype('category')

## Orders and Returns for Order info table
order_info_aux_order_info = orders.copy()
returns_aux_order_info = returns.copy()

# Dropping unrelated columns and rows with entirely duplicate records
order_info_aux_order_info = orders.drop(['Row ID', 'Order Date', 'Ship Date', 'Customer ID', 'Customer Name', 'Segment', 'Postal Code', 'City', 'State', 'Country', 'Region', 'Market', 'Product ID', 'Category', 'Sub-Category', 'Product Name', 'Sales', 'Quantity', 'Discount', 'Profit', 'Shipping Cost'], axis=1)
_order_info = returns.drop(['Region'], axis=1)

# Checking if data types used are correct
order_info_aux_order_info['Order ID'] = order_info_aux_order_info['Order ID'].astype(str)
order_info_aux_order_info['Ship Mode'] = order_info_aux_order_info['Ship Mode'].astype(str)
order_info_aux_order_info['Order Priority'] = order_info_aux_order_info['Order Priority'].astype(str)

# Filter orders that are repeated (same Order ID)
order_info_aux_order_info = order_info_aux_order_info.drop_duplicates(subset='Order ID')

order_info_aux_order_info = pd.merge(order_info_aux_order_info, _order_info, on=['Order ID'], how="left")
order_info_aux_order_info.rename(columns = {'Returned':'Returned Indicator'}, inplace = True)
order_info_aux_order_info["Returned Indicator"] = order_info_aux_order_info["Returned Indicator"].map({"Yes": "Returned"})
order_info_aux_order_info["Returned Indicator"].replace(np.nan, "Not Returned", inplace=True)

## Orders and Returns for Seller table
order_info_aux_seller = orders.copy()
order_info_aux_seller.drop(['Row ID', 'Order ID', 'Order Date', 'Ship Date', 'Customer ID', 'Customer Name', 'Segment', 'Postal Code', 'Product ID', 'Category', 'Sub-Category', 'Product Name', 'Sales', 'Quantity', 'Discount', 'Profit', 'Shipping Cost', 'Ship Mode', 'Order Priority'], axis=1, inplace=True)

# Checking if data types used are correct
order_info_aux_seller['City'] = order_info_aux_seller['City'].astype(str)
order_info_aux_seller['State'] = order_info_aux_seller['State'].astype(str)
order_info_aux_seller['Country'] = order_info_aux_seller['Country'].astype(str)
order_info_aux_seller['Region'] = order_info_aux_seller['Region'].astype(str)
order_info_aux_seller['Market'] = order_info_aux_seller['Market'].astype(str)
order_info_aux_seller['Market'] = order_info_aux_seller['Market'].astype(str)

order_info_aux_seller = order_info_aux_seller.drop_duplicates(subset='City')#se nao der certo secalhar tirar o keep='first'

# Filter orders that have repeated City and State together
#since there may be Cities in different locations with the same name but no repeated combinations of City and State together
order_info_aux_seller = order_info_aux_seller.drop_duplicates(subset=['City', 'State'], keep='first')#se nao der certo secalhar tirar o keep='first'

# Removing quotation marks and double quotation marks from any values
order_info_aux_seller['City'] = order_info_aux_seller['City'].str.replace('[\'\"]', '')
order_info_aux_seller['State'] = order_info_aux_seller['State'].str.replace('[\'\"]', '')
order_info_aux_seller['Country'] = order_info_aux_seller['Country'].str.replace('[\'\"]', '')
order_info_aux_seller['Region'] = order_info_aux_seller['Region'].str.replace('[\'\"]', '')
order_info_aux_seller['Market'] = order_info_aux_seller['Market'].str.replace('[\'\"]', '')

                                        ######  Sellers Table ###### 
sellers['Person'] = sellers['Person'].astype(str)
sellers['Region'] = sellers['Region'].astype(str)
sellers["Person"] = sellers["Person"].str.replace('[\'\"]', '')
sellers["Region"] = sellers["Region"].str.replace('[\'\"]', '')

seller_df = pd.merge(order_info_aux_seller, sellers, how = "left",left_on=['Region'], right_on=['Region']).fillna("Canada") # Eartern and Western Canada do not exist on the orders table hence the value Canada is used as default value.

seller_df.rename(columns = {'Person':'Seller Name'}, inplace = True)
seller_df.rename(columns = {'Market':'Seller Market'}, inplace = True)
seller_df.rename(columns = {'Region':'Seller Region'}, inplace = True)
seller_df.rename(columns = {'Country':'Seller Country'}, inplace = True)
seller_df.rename(columns = {'State':'Seller State'}, inplace = True)
seller_df.rename(columns = {'City':'Seller City'}, inplace = True)


                                        ######  GDP_USA Table ######
                                    
# Filter out all entries where the state is either Alaska or Hawaii.
gdp = gdp[(gdp['State'] != "Alaska") & (gdp['State'] != "Hawaii")]

# Remove Region, SUB-REGION, County columns
gdp = gdp.drop(['Region', 'SUB_REGION', 'County'], axis=1)

# Filter the years that are not between 2012 and 2015
gdp = gdp[gdp['Year'].isin([2012, 2013, 2014, 2015])]

# Convert Year to an integer date type
gdp['Year'] = pd.to_datetime(gdp['Year'], format='%Y')

# Calculate the mean of the GDP values for each state and year
result = gdp.groupby(['State', 'Year'])['GDP (Chained $)'].mean().reset_index()

# Remove duplicate states
result = result.drop_duplicates(subset=['State'])

# Add a new column 'State Key' as a unique identifier for each state
result['State_Key_GDP'] = range(1, len(result) + 1)

# Pivot the data to get average GDP for each year
gdp_pivot = pd.pivot_table(gdp, values='GDP (Chained $)', index='State', columns='Year').reset_index()

# Rename the columns with year-specific average GDP
gdp_pivot.rename(columns={pd.to_datetime('2012'): 'Average_GDP_2012_billions',
                          pd.to_datetime('2013'): 'Average_GDP_2013_billions',
                          pd.to_datetime('2014'): 'Average_GDP_2014_billions',
                          pd.to_datetime('2015'): 'Average_GDP_2015_billions'}, inplace=True) 


                                        ###### Customers_USA Table ######
                                        
# Convert Row ID and Quantity to integer data type
customers_USA['Row ID'] = customers_USA['Row ID'].astype(int)
customers_USA['Quantity'] = customers_USA['Quantity'].astype(int)

# Convert Order ID, Customer ID, Customer Name, City, State, Country, Region, Market, Product ID and Product Name to string data type
customers_USA['Order ID'] = customers_USA['Order ID'].astype(str)
customers_USA['Customer ID'] = customers_USA['Customer ID'].astype(str)
customers_USA['Customer Name'] = customers_USA['Customer Name'].astype(str)
customers_USA['City'] = customers_USA['City'].astype(str)
customers_USA['State'] = customers_USA['State'].astype(str)
customers_USA['Country'] = customers_USA['Country'].astype(str)
customers_USA['Region'] = customers_USA['Region'].astype(str)
customers_USA['Product ID'] = customers_USA['Product ID'].astype(str)
customers_USA['Product Name'] = customers_USA['Product Name'].astype(str)

# Convert Ship Date and Order Date to date data type
customers_USA['Ship Date'] = pd.to_datetime(customers_USA['Ship Date'])
customers_USA['Order Date'] = pd.to_datetime(customers_USA['Order Date'])

# Convert Segment, Category, Sub-Category and Order Priority to category data type
customers_USA['Segment'] = customers_USA['Segment'].astype('category')
customers_USA['Category'] = customers_USA['Category'].astype('category')
customers_USA['Sub-Category'] = customers_USA['Sub-Category'].astype('category')

end = time.time()
print("Transformation took: ", end - start, " seconds")


                    ############################# Dims and Facts Creation #############################

start = time.time()

# The folowing order needs to be folowed:
# 1st Outrigers: Holiday Dim; GDP Dim
# 2nd Dimension: Ship / Order Date Dim; Product Dim; Order Information Dim; Seller Dim; Customer Dim
# 3th Facts: Facts Table


                                            ###### Holiday_Dimension ######                                      
#Empty DataFrame
Holiday_Dimension = pd.DataFrame()
#Columns
#Full Holiday Date
Holiday_Dimension["Full_Holiday_Date"] = holiday["Date"]
# Holiday Key (PK)
Holiday_Dimension['Holiday_Key'] = range(1, len(Holiday_Dimension)+1)
# Holiday Name
Holiday_Dimension["Holiday_Name"] = holiday["Holiday"]
# Year Holiday
Holiday_Dimension["Year_Holiday"] = holiday["Year"]
# Month Holiday	
Holiday_Dimension["Month_Holiday"] = holiday["Month"]
# Day Month Holiday
Holiday_Dimension["Day_Month_Holiday"] = holiday["Day"]
# Day Week Holiday
Holiday_Dimension['Day_Week_Holiday'] = Holiday_Dimension['Full_Holiday_Date'].dt.strftime('%A')
# Change No Holiday Key
Holiday_Dimension.at[0, 'Holiday_Key'] = 999


                                            ###### GDP_Dimension ######
# Merge the result with the pivot table
GDP_Dimension = pd.merge(result[['State', 'State_Key_GDP']], gdp_pivot, on='State')
# Reorder the columns with 'State Key' as the first column
GDP_Dimension = GDP_Dimension[['State_Key_GDP', 'State', 'Average_GDP_2012_billions', 'Average_GDP_2013_billions', 'Average_GDP_2014_billions', 'Average_GDP_2015_billions']]
GDP_Dimension[['Average_GDP_2012_billions', 'Average_GDP_2013_billions', 'Average_GDP_2014_billions', 'Average_GDP_2015_billions']] = GDP_Dimension[['Average_GDP_2012_billions', 'Average_GDP_2013_billions', 'Average_GDP_2014_billions', 'Average_GDP_2015_billions']].div(1000000000).round(1)


                                        ###### Ship_Date_Dimension ######
#Empty DataFrame
Ship_Date_Dimension = pd.DataFrame()
# Columns
# Ship_Full_Date_Description
Ship_Date_Dimension["Ship_Full_Date_Description"] = orders["Ship Date"].unique()
# Ship Year
Ship_Date_Dimension['Ship_Year'] = Ship_Date_Dimension['Ship_Full_Date_Description'].dt.year
# Ship Season   
Ship_Date_Dimension['Ship_Season'] = Ship_Date_Dimension['Ship_Full_Date_Description'].apply(get_season)    
# Ship Semester    
Ship_Date_Dimension['Ship_Semester'] = Ship_Date_Dimension['Ship_Full_Date_Description'].apply(get_semester)
# Ship Month Number Year
Ship_Date_Dimension['Ship_Month_Number_Year '] = Ship_Date_Dimension['Ship_Full_Date_Description'].dt.month
# Ship Week Number Year
Ship_Date_Dimension['Ship_Week_Number_Year'] = Ship_Date_Dimension['Ship_Full_Date_Description'].dt.isocalendar().week
# Ship Day Number Month
Ship_Date_Dimension['Ship_Day_Number_Month'] = Ship_Date_Dimension['Ship_Full_Date_Description'].dt.days_in_month
# Ship Day  Number Week
Ship_Date_Dimension['Ship_Day_Number_Week'] = Ship_Date_Dimension['Ship_Full_Date_Description'].dt.day_of_week
# Ship Day Name Week
Ship_Date_Dimension['Ship_Day_Name_Week'] = Ship_Date_Dimension['Ship_Full_Date_Description'].dt.strftime('%A')
# Weekend Indicator   
Ship_Date_Dimension['Weekend_Indicator'] = Ship_Date_Dimension['Ship_Full_Date_Description'].dt.dayofweek.apply(get_weekend_indicator)
# Holiday Key
Holiday_Dimension_selected = Holiday_Dimension[["Full_Holiday_Date",'Holiday_Key']]
Ship_Date_Dimension = pd.merge(Ship_Date_Dimension, Holiday_Dimension_selected, left_on='Ship_Full_Date_Description', right_on='Full_Holiday_Date', how='left')
Ship_Date_Dimension.drop("Full_Holiday_Date", axis=1, inplace=True)
#Ship_Date_Dimension['Holiday_Key'].fillna('No Key', inplace=True)
Ship_Date_Dimension["Holiday_Key"] = Ship_Date_Dimension["Holiday_Key"].fillna(999)
# Holiday indicator   
Ship_Date_Dimension['Holiday_Indicator'] = Ship_Date_Dimension['Holiday_Key'].apply(get_holiday_indicator)
# Ship Date Key
Ship_Date_Dimension['Ship_Date_Key'] = range(1, len(Ship_Date_Dimension["Ship_Full_Date_Description"])+1)


                                        ###### Order_Date_Dimension ######
#Empty DataFrame
Order_Date_Dimension = pd.DataFrame()
# Columns
# Order_Full_Date_Description
Order_Date_Dimension["Order_Full_Date_Description"] = orders["Order Date"].unique()
# Order Year
Order_Date_Dimension['Order_Year'] = Order_Date_Dimension['Order_Full_Date_Description'].dt.year
# Order Season    
Order_Date_Dimension['Order_Season'] = Order_Date_Dimension['Order_Full_Date_Description'].apply(get_season)  
# Order Semester   
Order_Date_Dimension['Order_Semester'] = Order_Date_Dimension['Order_Full_Date_Description'].apply(get_semester)
# Order Month Number Year
Order_Date_Dimension['Order_Month_Number_Year'] = Order_Date_Dimension['Order_Full_Date_Description'].dt.month
# Order Week Number Year
Order_Date_Dimension['Order_Week_Number_Year'] = Order_Date_Dimension['Order_Full_Date_Description'].dt.isocalendar().week
# Order Day Number Month
Order_Date_Dimension['Order_Day_Number_Month'] = Order_Date_Dimension['Order_Full_Date_Description'].dt.days_in_month
# Order Day  Number Week
Order_Date_Dimension['Order_Day_Number_Week'] = Order_Date_Dimension['Order_Full_Date_Description'].dt.day_of_week
# Order Day Name Week
Order_Date_Dimension['Order_Day_Name_Week'] = Order_Date_Dimension['Order_Full_Date_Description'].dt.strftime('%A')
# Weekend Indicator 
Order_Date_Dimension['Weekend_Indicator'] = Order_Date_Dimension['Order_Full_Date_Description'].dt.dayofweek.apply(get_weekend_indicator)
# Holiday Key
Holiday_Dimension_selected = Holiday_Dimension[["Full_Holiday_Date",'Holiday_Key']]
Order_Date_Dimension = pd.merge(Order_Date_Dimension, Holiday_Dimension_selected, left_on='Order_Full_Date_Description', right_on='Full_Holiday_Date', how='left')
Order_Date_Dimension.drop("Full_Holiday_Date", axis=1, inplace=True)
#Order_Date_Dimension['Holiday_Key'].fillna('No Key', inplace=True)
Order_Date_Dimension["Holiday_Key"] = Order_Date_Dimension["Holiday_Key"].fillna(999)
# Holiday indicator
Order_Date_Dimension['Holiday_Indicator'] = Order_Date_Dimension['Holiday_Key'].apply(get_holiday_indicator)
# Order Date Key
Order_Date_Dimension['Order_Date_Key'] = range(1, len(Order_Date_Dimension["Order_Full_Date_Description"])+1)


                                            ###### Product_Dimension ######                                    
#Empty DataFrame
Product_Dimension = pd.DataFrame()
# Product ID
Product_Dimension["Product_ID"] = orders["Product ID"]
# Product Name
Product_Dimension["Product_Name"] = orders["Product Name"]
# Category
Product_Dimension["Category"] = orders["Category"]
# Sub Category
Product_Dimension["Sub_Category"] = orders["Sub-Category"]
# Drop Duplicates
Product_Dimension.drop_duplicates(inplace=True)
# Product Key
Product_Dimension['Product_Key'] = range(1, len(Product_Dimension)+1)
# Re establish Correct Order
Product_Dimension = Product_Dimension[["Product_ID", "Product_Key", "Product_Name", "Category", "Sub_Category"]]


                                            ###### Order_Information_Dimension ###### 
# Adding PK and reordering columns
Order_Information_Dimension = order_info_aux_order_info[['Order ID', 'Ship Mode', 'Order Priority', 'Returned Indicator']]
Order_Information_Dimension["Order Key"] = pd.factorize(order_info_aux_order_info['Order ID'])[0] + 1
Order_Information_Dimension = Order_Information_Dimension.reindex(columns=['Order Key', 'Order ID', 'Returned Indicator', 'Ship Mode', 'Order Priority']).copy()


                                            ###### Seller_Dimension ###### 
# Adding PK and reordering columns
Seller_Dimension = seller_df[['Seller City', 'Seller State', 'Seller Country', 'Seller Region', 'Seller Market', 'Seller Name']]
Seller_Dimension["Seller Key"] = pd.factorize(seller_df['Seller City'])[0] + 1
Seller_Dimension = Seller_Dimension.reindex(columns=['Seller Key', 'Seller Name', 'Seller Market', 'Seller Region', 'Seller Country', 'Seller State', 'Seller City']).copy()


                                            ###### Customer_Dimension ###### 
# Create the Customer Dimension DataFrame
Customer_Dimension = customers_USA[['Customer ID', 'Customer Name', 'Segment', 'State', 'Region', 'City', 'Postal Code']]
# Generate unique numerical identifiers for each customer
Customer_Dimension['Customer Key'] = pd.factorize(Customer_Dimension['Customer ID'] + '_' + Customer_Dimension['City'])[0] + 1
# Reorder the columns with 'Customer Key' as the first column
Customer_Dimension = Customer_Dimension.reindex(columns=['Customer Key', 'Customer ID', 'Customer Name', 'Segment', 'State', 'Region', 'City', 'Postal Code']).copy()
# Check if 'State Key Customer' column already exists
if 'State_Key_Customer' not in Customer_Dimension.columns:
    # Merge GDP_dimension and customer_dimension on 'State' column
    Customer_Dimension = Customer_Dimension.merge(GDP_Dimension[['State', 'State_Key_GDP']], left_on='State', right_on='State', how='left')
    # Add a unique constraint to the State_Key column in Customer_Dimension
Customer_Dimension.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
Customer_Dimension.rename(columns={'State_Key_GDP': 'State_Key_Customer'}, inplace=True)
Customer_Dimension = Customer_Dimension.drop_duplicates(subset=['Customer_Key'])
# Remove rows with specific customer names
values_to_drop = ['Tom Zandusky', 'Cari MacIntyre', 'Kai Rey']
Customer_Dimension = Customer_Dimension[~Customer_Dimension['Customer_Name'].isin(values_to_drop)]


                                        ###### Customer_Dimension ###### 
# Read the orders data from Excel file
orders = pd.read_excel('../Phase01/data/Raw_Tables/orders_RAW.xlsx')
# Remove unnecessary quotes from the City column
orders['City'] = orders['City'].str.replace('[\'\"]', '')
# Merge orders and product dimension based on Product ID
facts = pd.merge(orders, Product_Dimension[['Product_ID', 'Product_Key']], left_on='Product ID', right_on='Product_ID', how='left')
# Drop redundant column
facts.drop(['Product ID', 'Product Name', 'Category', 'Sub-Category', 'Product_ID', 'Discount', 'Postal Code'], axis=1, inplace=True)
# Map Ship Date to Ship Date Key
ship_date_mapping = Ship_Date_Dimension.set_index('Ship_Full_Date_Description')['Ship_Date_Key'].to_dict()
facts['Ship_Date_Key'] = facts['Ship Date'].map(ship_date_mapping)
# Map Order Date to Order Date Key
order_date_mapping = Order_Date_Dimension.set_index('Order_Full_Date_Description')['Order_Date_Key'].to_dict()
facts['Order_Date_Key'] = facts['Order Date'].map(order_date_mapping)
# Merge with Seller Dimension based on City and Seller City columns
facts = facts.merge(Seller_Dimension, left_on='City', right_on='Seller City', how='left')
# Drop unnecessary columns from Seller Dimension
facts.drop(['Seller Name', 'Seller Market', 'Seller Region', 'Seller Country', 'Seller State', 'Seller City'], axis=1, inplace=True)
# Drop unnecessary columns from facts
facts.drop(['City', 'State', 'Country', 'Region', 'Market'], axis=1, inplace=True)
# Remove rows with specific customer names
values_to_drop = ['Tom Zandusky', 'Cari MacIntyre', 'Kai Rey']
facts = facts[~facts['Customer Name'].isin(values_to_drop)]
# Map Customer Name to Customer Key
customer_mapping = Customer_Dimension.set_index('Customer_Name')['Customer_Key'].to_dict()
facts['Customer_Key'] = facts['Customer Name'].map(customer_mapping)
# Map Order ID to Order Key
order_mapping = Order_Information_Dimension.set_index('Order ID')['Order Key'].to_dict()
facts['Order_Key'] = facts['Order ID'].map(order_mapping)
# Drop unnecessary columns
facts.drop(['Customer ID', 'Customer Name', 'Segment', 'Order ID', 'Row ID', 'Ship Mode', 'Order Priority', 'Order Date', 'Ship Date'], axis=1, inplace=True)
# Add Transaction Key column
facts['Transaction_Key'] = range(1, len(facts) + 1)
# Reorder columns
desired_order = ['Transaction_Key', 'Product_Key', 'Customer_Key', 'Order_Key', 'Order_Date_Key', 'Ship_Date_Key', 'Seller Key', 'Sales', 'Quantity', 'Profit', 'Shipping Cost']
Facts_Table = facts[desired_order]
Facts_Table.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
Facts_Table = Facts_Table.drop_duplicates(subset=['Product_Key', 'Customer_Key', 'Order_Key', 'Order_Date_Key', 'Ship_Date_Key', 'Seller_Key'])

end = time.time()
print("Dims and Fact Table creation took: ", end - start, " seconds")


                    ############################# Loading #############################


start = time.time()
## Establish Connection                   
conn = pg.connect(host="appserver-01.alunos.di.fc.ul.pt",database="ipai01", user="ipai01", password='ipai02')
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS Ship_Date_Dimension CASCADE;")
cursor.execute("DROP TABLE IF EXISTS Order_Date_Dimension CASCADE;")
cursor.execute("DROP TABLE IF EXISTS Product_Dimension CASCADE;")
cursor.execute("DROP TABLE IF EXISTS Order_Information_Dimension CASCADE;")
cursor.execute("DROP TABLE IF EXISTS Seller_Dimension CASCADE;")
cursor.execute("DROP TABLE IF EXISTS Facts_Table CASCADE;")
cursor.execute("DROP TABLE IF EXISTS Customer_Dimension CASCADE;")

conn.commit()
                                    ###### Holiday_Dimension ###### 
# Check transaction status and perform rollback if necessary
if conn.status == pg.extensions.STATUS_IN_TRANSACTION:
    conn.rollback()

# Create SQL Table in DB
sql_Holiday_Dimension = """


CREATE TABLE Holiday_Dimension (
  Full_Holiday_Date TIMESTAMP NULL,
  Holiday_Key  NUMERIC(9,0),
  Holiday_Name VARCHAR(100) NOT NULL,
  Year_Holiday NUMERIC(4,0) NOT NULL,
  Month_Holiday NUMERIC(2,0) NOT NULL,
  Day_Month_Holiday NUMERIC(2,0) NOT NULL,
  Day_Week_Holiday VARCHAR(100) NOT NULL,
  
--
  PRIMARY KEY (Holiday_Key),
--
  CHECK (Holiday_Key > 0)
);
"""
cursor.execute("DROP table IF EXISTS Holiday_Dimension;")
cursor.execute(sql_Holiday_Dimension)
conn.commit()

# Load data into DB
Holiday_Dimension_list = Holiday_Dimension.to_numpy().tolist()

sql_holiday = "INSERT INTO Holiday_Dimension(Full_Holiday_Date, Holiday_Key, Holiday_Name, Year_Holiday, Month_Holiday, Day_Month_Holiday, Day_Week_Holiday) VALUES(%s, %s, %s, %s, %s, %s, %s)"

cursor.executemany(sql_holiday, Holiday_Dimension_list)
conn.commit()


                                    ###### GDP_Dimension ######
# Check transaction status and perform rollback if necessary
if conn.status == pg.extensions.STATUS_IN_TRANSACTION:
    conn.rollback()

# Create SQL Table in DB
sql_GDP_Dimension = """

CREATE TABLE GDP_Dimension (
  State_Key_GDP NUMERIC(9,0),
  State VARCHAR(100) NOT NULL,
  Average_GDP_2012_billions FLOAT(2) NOT NULL,
  Average_GDP_2013_billions FLOAT(2) NOT NULL,
  Average_GDP_2014_billions FLOAT(2) NOT NULL,
  Average_GDP_2015_billions FLOAT(2) NOT NULL,
  
--
  PRIMARY KEY (State_Key_GDP),
--
  CHECK (State_Key_GDP > 0)
);
"""
# Drop the GDP_Dimension table and its dependent objects
cursor.execute("DROP TABLE IF EXISTS Customer_Dimension CASCADE;")
cursor.execute("DROP TABLE IF EXISTS GDP_Dimension CASCADE;")

conn.commit()
cursor.execute(sql_GDP_Dimension)
conn.commit()

# Load data into DB
GDP_Dimension_list = GDP_Dimension.to_numpy().tolist()

sql_GDP = "INSERT INTO GDP_Dimension(State_Key_GDP, State, Average_GDP_2012_billions, Average_GDP_2013_billions, Average_GDP_2014_billions, Average_GDP_2015_billions) VALUES(%s, %s, %s, %s, %s, %s)"

cursor.executemany(sql_GDP, GDP_Dimension_list)
conn.commit()

                                    ###### Ship_Date_Dimension ######
# Check transaction status and perform rollback if necessary
if conn.status == pg.extensions.STATUS_IN_TRANSACTION:
    conn.rollback()
    
# Create SQL Table in DB
sql_Ship_Date_Dimension = """

CREATE TABLE Ship_Date_Dimension (
  Ship_Full_Date_Description TIMESTAMP NOT NULL,
  Ship_Year NUMERIC(4,0) NOT NULL,
  Ship_Season VARCHAR(100) NOT NULL,
  Ship_Semester NUMERIC(4,0) NOT NULL,
  Ship_Month_Number_Year NUMERIC(2,0) NOT NULL,
  Ship_Week_Number_Year NUMERIC(4,0) NOT NULL,
  Ship_Day_Number_Month NUMERIC(2,0) NOT NULL,
  Ship_Day_Number_Week NUMERIC(1,0) NOT NULL,
  Ship_Day_Name_Week VARCHAR(100) NOT NULL,
  Weekend_Indicator VARCHAR(100) NOT NULL,
  Holiday_Key NUMERIC(3,0) NOT NULL,  
  Holiday_Indicator VARCHAR(100) NOT NULL,
  Ship_Date_Key  NUMERIC(9,0),
  
 --
  PRIMARY KEY (Ship_Date_Key),
  FOREIGN KEY (Holiday_Key) REFERENCES Holiday_Dimension(Holiday_Key),
--
  CHECK (Ship_Date_Key > 0)
);
  
"""
cursor.execute("DROP table IF EXISTS Ship_Date_Dimension;")
cursor.execute(sql_Ship_Date_Dimension)
conn.commit()
# Load data into DB
Ship_Date_Dimension_list = Ship_Date_Dimension.to_numpy().tolist()

sql_Ship_Date = "INSERT INTO Ship_Date_Dimension(Ship_Full_Date_Description, Ship_Year, Ship_Season, Ship_Semester, Ship_Month_Number_Year, Ship_Week_Number_Year, Ship_Day_Number_Month, Ship_Day_Number_Week, Ship_Day_Name_Week, Weekend_Indicator, Holiday_Key, Holiday_Indicator, Ship_Date_Key) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

cursor.executemany(sql_Ship_Date, Ship_Date_Dimension_list)
conn.commit()


                                    ###### Order_Date_Dimension ######
# Check transaction status and perform rollback if necessary
if conn.status == pg.extensions.STATUS_IN_TRANSACTION:
    conn.rollback()

# Create SQL Table in DB
sql_Order_Date_Dimension = """

CREATE TABLE Order_Date_Dimension (
  Order_Full_Date_Description TIMESTAMP NOT NULL,
  Order_Year NUMERIC(4,0) NOT NULL,
  Order_Season VARCHAR(100) NOT NULL,
  Order_Semester NUMERIC(4,0) NOT NULL,
  Order_Month_Number_Year NUMERIC(2,0) NOT NULL,
  Order_Week_Number_Year NUMERIC(4,0) NOT NULL,
  Order_Day_Number_Month NUMERIC(2,0) NOT NULL,
  Order_Day_Number_Week NUMERIC(1,0) NOT NULL,
  Order_Day_Name_Week VARCHAR(100) NOT NULL,
  Weekend_Indicator VARCHAR(100) NOT NULL,
  Holiday_Key NUMERIC(3,0) NOT NULL,  
  Holiday_Indicator VARCHAR(100) NOT NULL,
  Order_Date_Key  NUMERIC(9,0),
  
 --
  PRIMARY KEY (Order_Date_Key),
  FOREIGN KEY (Holiday_Key) REFERENCES Holiday_Dimension(Holiday_Key),
--
  CHECK (Order_Date_Key > 0)
);
  
"""
cursor.execute("DROP table IF EXISTS Order_Date_Dimension;")
cursor.execute(sql_Order_Date_Dimension)
conn.commit()

# Load data into DB
Order_Date_Dimension_list = Order_Date_Dimension.to_numpy().tolist()

sql_Order_Date = "INSERT INTO Order_Date_Dimension(Order_Full_Date_Description, Order_Year, Order_Season, Order_Semester, Order_Month_Number_Year, Order_Week_Number_Year, Order_Day_Number_Month, Order_Day_Number_Week, Order_Day_Name_Week, Weekend_Indicator, Holiday_Key, Holiday_Indicator, Order_Date_Key) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

cursor.executemany(sql_Order_Date, Order_Date_Dimension_list)
conn.commit()

                                        ###### Product_Dimension ######
# Check transaction status and perform rollback if necessary
if conn.status == pg.extensions.STATUS_IN_TRANSACTION:
    conn.rollback()

# Check transaction status and perform rollback if necessary
if conn.status == pg.extensions.STATUS_IN_TRANSACTION:
    conn.rollback()

# Create SQL Table in DB
sql_Product_Dimension = """


CREATE TABLE Product_Dimension (
  Product_ID VARCHAR(500) NOT NULL,
  Product_Key  NUMERIC(9,0),
  Product_Name VARCHAR(500) NOT NULL,
  Category VARCHAR(500) NOT NULL,
  Sub_Category VARCHAR(500) NOT NULL,
  
--
  PRIMARY KEY (Product_Key),
--
  CHECK (Product_Key > 0)
);
"""
cursor.execute("DROP table IF EXISTS Product_Dimension;")
cursor.execute(sql_Product_Dimension)
conn.commit()

# Load data into DB
Product_Dimension_list = Product_Dimension.to_numpy().tolist()

sql_Ship_Date = "INSERT INTO Product_Dimension(Product_ID, Product_Key, Product_Name, Category, Sub_Category) VALUES (%s, %s, %s, %s, %s)"

cursor.executemany(sql_Ship_Date, Product_Dimension_list)
conn.commit()

                                        ###### Order_Information_Dimension ######
# Creating table

if conn.status == pg.extensions.STATUS_IN_TRANSACTION:
    conn.rollback()

sql_Order_Information_Dimension = """

CREATE TABLE Order_Information_Dimension (
  Order_Key  NUMERIC(9,0),
  Order_ID VARCHAR(100) NOT NULL,
  Returned_Indicator VARCHAR(100) NOT NULL,
  Ship_Mode VARCHAR(100) NOT NULL,
  Order_Priority VARCHAR(100) NOT NULL,
  
--
  PRIMARY KEY (Order_Key),
--
  CHECK (Order_Key > 0)
);
"""
cursor.execute("DROP table IF EXISTS Order_Information_Dimension;")
cursor.execute(sql_Order_Information_Dimension)
conn.commit()

# Inserting values

Order_Information_Dimension_list = Order_Information_Dimension.to_numpy().tolist()

sql_Order_Information = "INSERT INTO Order_Information_Dimension(Order_Key, Order_ID, Returned_Indicator, Ship_Mode, Order_Priority) VALUES(%s, %s, %s, %s, %s)"

cursor.executemany(sql_Order_Information, Order_Information_Dimension_list)
conn.commit()



                                ###### Seller_Dimension ######
# Creating table

if conn.status == pg.extensions.STATUS_IN_TRANSACTION:
    conn.rollback()

sql_Seller_Dimension = """

CREATE TABLE Seller_Dimension (
  Seller_Key  NUMERIC(9,0),
  Seller_Name VARCHAR(100) NOT NULL,
  Seller_Market VARCHAR(100) NOT NULL,
  Seller_Region VARCHAR(100) NOT NULL,
  Seller_Country VARCHAR(100) NOT NULL,
  Seller_State VARCHAR(100) NOT NULL,
  Seller_City VARCHAR(100) NOT NULL,
  
--
  PRIMARY KEY (Seller_Key),
--
  CHECK (Seller_Key > 0)
);
"""
cursor.execute("DROP table IF EXISTS Seller_Dimension;")
cursor.execute(sql_Seller_Dimension)
cursor.execute("TRUNCATE TABLE Seller_Dimension;")
conn.commit()

# Inserting values

Seller_Dimension_list = Seller_Dimension.to_numpy().tolist()

sql_Seller_Dimension = "INSERT INTO Seller_Dimension(Seller_Key, Seller_Name, Seller_Market, Seller_Region, Seller_Country, Seller_State, Seller_City) VALUES(%s, %s, %s, %s, %s, %s, %s)"

cursor.executemany(sql_Seller_Dimension, Seller_Dimension_list)
conn.commit()


                                    ###### Customer_Dimension ######
# Check transaction status and perform rollback if necessary
if conn.status == pg.extensions.STATUS_IN_TRANSACTION:
    conn.rollback()

# Create SQL Table in DB
sql_Customer_Dimension = """

CREATE TABLE Customer_Dimension (
  Customer_Key NUMERIC(9,0),
  Customer_ID VARCHAR(100) NOT NULL,
  Customer_Name VARCHAR(100) NOT NULL,
  Segment VARCHAR(100) NOT NULL,
  State VARCHAR(100) NOT NULL,
  Region VARCHAR(100) NOT NULL,
  City VARCHAR(100) NOT NULL,
  Postal_Code VARCHAR(100) NOT NULL,
  State_Key_Customer NUMERIC(9,0) NOT NULL,
  
--
  PRIMARY KEY (Customer_Key),
  FOREIGN KEY (State_Key_Customer) REFERENCES GDP_Dimension(State_Key_GDP),

--
  CHECK (Customer_Key > 0)
);
"""
cursor.execute("DROP table IF EXISTS Customer_Dimension;")
cursor.execute(sql_Customer_Dimension)
conn.commit()

# Load data into DB
Customer_Dimension_list = Customer_Dimension.to_numpy().tolist()

sql_Customer = "INSERT INTO Customer_Dimension(Customer_Key, Customer_ID, Customer_Name, Segment, State, Region, City, Postal_Code, State_Key_Customer) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"

cursor.executemany(sql_Customer, Customer_Dimension_list)
conn.commit()

                                    ###### Facts_Table ######
                                    
# Check transaction status and perform rollback if necessary
if conn.status == pg.extensions.STATUS_IN_TRANSACTION:
    conn.rollback()

# Create the facts table using the template
sql_facts_table = """
    CREATE TABLE Facts_Table (
        Transaction_Key NUMERIC(9,0),
        Product_Key NUMERIC(9,0),
        Customer_Key NUMERIC(9,0),
        Order_Key NUMERIC(9,0),
        Order_Date_Key NUMERIC(9,0),
        Ship_Date_Key NUMERIC(9,0),
        Seller_Key NUMERIC(9,0),
        Sales NUMERIC(9,0),
        Quantity NUMERIC(9,0),
        Profit NUMERIC(9,0),
        Shipping_Cost NUMERIC(9,0),
    --
        PRIMARY KEY (Product_Key, Customer_Key, Order_Key, Order_Date_Key, Ship_Date_Key, Seller_Key),
    --
        FOREIGN KEY (Product_Key) REFERENCES Product_Dimension(Product_Key),
        FOREIGN KEY (Customer_Key) REFERENCES Customer_Dimension(Customer_Key),
        FOREIGN KEY (Order_Key) REFERENCES Order_Information_Dimension(Order_Key),
        FOREIGN KEY (Order_Date_Key) REFERENCES Order_Date_Dimension(Order_Date_Key),
        FOREIGN KEY (Ship_Date_Key) REFERENCES Ship_Date_Dimension(Ship_Date_Key),
        FOREIGN KEY (Seller_Key) REFERENCES Seller_Dimension(Seller_Key),
    --
        CHECK (Product_Key > 0),
        CHECK (Customer_Key > 0),
        CHECK (Order_Key > 0),
        CHECK (Order_Date_Key > 0),
        CHECK (Ship_Date_Key > 0),
        CHECK (Seller_Key > 0)
    );
"""

cursor.execute("DROP TABLE IF EXISTS Facts_Table;")
cursor.execute(sql_facts_table)
conn.commit()

# Load data into DB
Facts_Table_list = Facts_Table.to_numpy().tolist()

sql_facts = """
INSERT INTO Facts_Table(Transaction_Key, Product_Key, Customer_Key, Order_Key, Order_Date_Key, Ship_Date_Key, Seller_Key, Sales, Quantity, Profit, Shipping_Cost)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

cursor.executemany(sql_facts, Facts_Table_list)
conn.commit()


end = time.time()
print("Loading took: ", end - start, " seconds")
end_total = time.time()
print("Total ETL took: ", end_total - start_total, " seconds")