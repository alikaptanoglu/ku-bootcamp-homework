

```python
import pandas as pd
from ipy_table import *
from babel.numbers import format_currency


def format_ipy_table(row_count=0):
    #choose basic, basic_left, or basic_both
    apply_theme('basic')
    #white background
    set_global_style(color='White')
    #bold all cells in the first column
    if row_count > 0:
        for r in range(0, row_count - 1):
            set_cell_style(r, 0, bold=True)
        
    
#read JSON into dataframe
file_1 = 'resources/purchase_data.json'
df_1 = pd.read_json(file_1)

#there are duplicates in Price per one Item ID
#there are duplicates in SN by gender
#the output turns out fairly complex and it took me much time to cross-validate the results
#...so I'm saving it for later work if I have extra time :)
#file_2 = 'resources/purchase_data_2.json'
#df_2 = pd.read_json(file_2)

#combine dataframes
df_combined = df_1


#due to a potential duplicates in SN in different genders...
#group by gender and SN, then count unique values
df_unique_sn = df_combined.groupby(["Gender", "SN"]).nunique().sum()


#PLAYER COUNT
#number of unique players
unique_players = df_unique_sn["SN"]
#display
total_players = [["Total Players"], [unique_players]]
table_player_count = make_table(total_players)
format_ipy_table()

#PURCHASING ANALYSIS
#number of unique items
unique_items = df_combined["Item ID"].nunique()
#average purchase price
average_price = df_combined["Price"].mean()
#number of items purchased (same as number of rows)
items_purchased = df_combined["Item ID"].count()
#total revenue, assuming 1 count of purchased item on each row
total_revenue = df_combined["Price"].sum()

purchasing_analysis = [["Number of Unique Items", "Average Price", 
                        "Number of Purchases", "Total Revenue"], 
                       [unique_items, format_currency(average_price, 'USD', locale='en_US'), 
                        items_purchased, format_currency(total_revenue, 'USD', locale='en_US')]]
table_purchasing_analysis = make_table(purchasing_analysis)
format_ipy_table()


#GENDER DEMOGRAPHICS
df_gender_count = df_combined.groupby(["Gender"]).nunique()
for i, row in df_gender_count.iterrows():
    if (i == "Female"):
        #percentage and count of female players
        female_count = row["SN"] 
        female_count_percent = "{:.2%}".format(female_count / unique_players)
    elif (i == "Male"):
        #percentage and count of male players
        male_count = row["SN"] 
        male_count_percent = "{:.2%}".format(male_count / unique_players)
    else:
        #percentage and count of other players
        other_count = row["SN"] 
        other_count_percent = "{:.2%}".format(1.0 - ((female_count + male_count) / unique_players)) 

gender_demo_analysis = [["", "Percentage of Players", "Total Count"], 
                        ["Female", female_count_percent, female_count], 
                        ["Male", male_count_percent, male_count],
                        ["Other / Non-Disclosed", other_count_percent, other_count]]

table_gender_demo_analysis = make_table(gender_demo_analysis)
format_ipy_table(5)

        
#PURCHASING ANALYSIS (gender)
#Purchase Count
gender_purchase_count = df_combined.groupby(["Gender"])["Item ID"].count()
#Average Purchase Price
gender_average_price = df_combined.groupby(["Gender"])["Price"].mean()
#Total Purchase Value
gender_total_purchase = df_combined.groupby(["Gender"])["Price"].sum()
#Total Purchase Value / Unique Player Count - calculated from the above data

gender_purchase_analysis = [["", "Purchase Count", "Average Purchase Price", "Total Purchase Value", "Normalized Totals"]]

for i, row in gender_purchase_count.iteritems():
    each_row = []
    each_row.append(i)
    each_row.append(gender_purchase_count.loc(i)[i])
    each_row.append(format_currency(gender_average_price.loc(i)[i], 'USD', locale='en_US'))
    each_row.append(format_currency(gender_total_purchase.loc(i)[i], 'USD', locale='en_US'))
    
    unique_player_count = other_count;
    if i == "Female":
        unique_player_count = female_count
    elif i == "Male":
        unique_player_count = male_count
        
    each_row.append(format_currency((gender_total_purchase.loc(i)[i] / unique_player_count), 'USD', locale='en_US'))
    
    gender_purchase_analysis.append(each_row)

table_gender_purchase_analysis = make_table(gender_purchase_analysis)
format_ipy_table(5)



#AGE DEMOGRAPHICS (broken into bins of 4 years)
start = df_combined["Age"].min()
end = df_combined["Age"].max()
bins = [start]
bin_names = [str(start) + "-" + str(start + 3)]
while (start <= end):
    start = start + 4
    bins.append(start)
    if (start <= end):
        bin_names.append(str(start) + "-" + str(start + 3))
        
df_combined["Age Group"] = pd.cut(df_combined["Age"], bins, labels=bin_names)

#age group count
age_group = df_combined.groupby(["Age Group"])["SN"].nunique()
age_group_count = age_group.sum()
age_group_count_analysis = [["", "Percentage of Players", "Total Count"]]
for i, row in age_group.iteritems():
    each_row = []
    each_row.append(i)
    each_row.append("{:.2%}".format(row.astype(int) / age_group_count))
    each_row.append(row)
    
    age_group_count_analysis.append(each_row)

table_age_group_count_analysis = make_table(age_group_count_analysis)
format_ipy_table(12)


#Purchase Count
age_group_purchase_count = df_combined.groupby(["Age Group"])["Item ID"].count()
#Average Purchase Price
age_group_average_purchase_price = df_combined.groupby(["Age Group"])["Price"].mean()
#Total Purchase Value
age_group_total_purchase = df_combined.groupby(["Age Group"])["Price"].sum()
#Total Purchase Value / Unique Player Count - calculated from the above values

age_group_purchase_analysis = [["", "Purchase Count", "Average Purchase Price", "Total Purchase Value", "Normalized Totals"]]
for i, row in age_group.iteritems():
    each_row = []
    each_row.append(i)
    each_row.append(age_group_purchase_count.loc(i)[i])
    each_row.append(format_currency(age_group_average_purchase_price.loc(i)[i], 'USD', locale='en_US'))
    each_row.append(format_currency(age_group_total_purchase.loc(i)[i], 'USD', locale='en_US'))
    each_row.append(format_currency((age_group_total_purchase.loc(i)[i] / age_group.loc(i)[i]), 'USD', locale='en_US'))
    
    age_group_purchase_analysis.append(each_row)
    
table_age_group_purchase_analysis = make_table(age_group_purchase_analysis)
format_ipy_table(12)



#LIST 5 TOP SPENDERS: SN, Purchase Count, Average Purchase Price, Total Purchase Value
#Purchase Count
spenders_purchase_count = df_combined.groupby(["SN"])["Item ID"].count()
#Average Purchase Price
spenders_average_purchase_price = df_combined.groupby(["SN"])["Price"].mean()
#Total Purchase Value
spenders_total_purchase = df_combined.groupby(["SN"])["Price"].sum()
top_spenders = spenders_total_purchase.sort_values(ascending=False)
top_spenders.head()

top_five_spenders_analysis = [["SN", "Purchase Count", "Average Purchase Price", "Total Purchase Value"]]

start_count = 0
                              
for i, row in top_spenders.iteritems():
    if start_count > 4:
        break
    each_row = []    
    each_row.append(i)
    each_row.append(spenders_purchase_count.loc(i)[i])
    each_row.append(format_currency(spenders_average_purchase_price.loc(i)[i], 'USD', locale='en_US'))
    each_row.append(format_currency(row, 'USD', locale='en_US'))

    top_five_spenders_analysis.append(each_row)
    
    start_count = start_count + 1

table_top_spenders_analysis = make_table(top_five_spenders_analysis)
format_ipy_table()


#LIST 5 MOST POPULAR ITEMS (by purchase count): Item ID, Item Name, Purchase Count, Item Price, Total Purchase Value
#Total Purchase Value
popular_total_purchase = df_combined.groupby(["Item ID"])["Price"].sum()
#Purchase Count
popular_purchase_count = df_combined.groupby(["Item ID"])["Item Name"].count()
popular = popular_purchase_count.sort_values(ascending=False)

popular_analysis = [["Item ID", "Item Name", "Purchase Count", "Item Price", "Total Purchase Value"]]

start_count = 0
                              
for i, row in popular.iteritems():
    if start_count > 4:
        break

    find = df_combined.loc([df_combined["Item ID"] == i])

    each_row = []
    each_row.append(i)
    each_row.append(find[i]["Item Name"])
    each_row.append(row)
    each_row.append(format_currency(find[i]["Price"], 'USD', locale='en_US'))
    each_row.append(format_currency(popular_total_purchase.loc(i)[i], 'USD', locale='en_US'))

    popular_analysis.append(each_row)
    
    start_count = start_count + 1

table_popular_analysis = make_table(popular_analysis)
format_ipy_table()


#LIST 5 MOST PROFITABLE ITEMS (by total purchase value): Item ID, Item Name, Purchase Count, Item Price, Total Purchase Value
#Purchase Count
profitable_purchase_count = df_combined.groupby(["Item ID"])["Item Name"].count()
#Total Purchase Value
profitable_total_purchase = df_combined.groupby(["Item ID"])["Price"].sum()
profitable = profitable_total_purchase.sort_values(ascending=False)

profitable_analysis = [["Item ID", "Item Name", "Purchase Count", "Item Price", "Total Purchase Value"]]

start_count = 0
                              
for i, row in profitable.iteritems():
    if start_count > 4:
        break

    find = df_combined.loc([df_combined["Item ID"] == i])

    each_row = []
    each_row.append(i)
    each_row.append(find[i]["Item Name"])
    each_row.append(profitable_purchase_count.loc(i)[i])
    each_row.append(format_currency(find[i]["Price"], 'USD', locale='en_US'))
    each_row.append(format_currency(row, 'USD', locale='en_US'))

    profitable_analysis.append(each_row)
    
    start_count = start_count + 1

table_profitable_popular_analysis = make_table(profitable_analysis)
format_ipy_table()






```


```python
table_player_count

```




<table border="1" cellpadding="3" cellspacing="0"  style="border:black; border-collapse:collapse;"><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Total&nbsp;Players</b></td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">573</td></tr></table>




```python
table_purchasing_analysis

```




<table border="1" cellpadding="3" cellspacing="0"  style="border:black; border-collapse:collapse;"><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Number&nbsp;of&nbsp;Unique&nbsp;Items</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Average&nbsp;Price</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Number&nbsp;of&nbsp;Purchases</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Total&nbsp;Revenue</b></td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">183</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.93</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">780</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2,286.33</td></tr></table>




```python
table_gender_demo_analysis

```




<table border="1" cellpadding="3" cellspacing="0"  style="border:black; border-collapse:collapse;"><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b></b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Percentage&nbsp;of&nbsp;Players</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Total&nbsp;Count</b></td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Female</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">17.45%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">100</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Male</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">81.15%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">465</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Other&nbsp;/&nbsp;Non-Disclosed</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">1.40%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">8</td></tr></table>




```python
table_gender_purchase_analysis

```




<table border="1" cellpadding="3" cellspacing="0"  style="border:black; border-collapse:collapse;"><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b></b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Purchase&nbsp;Count</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Average&nbsp;Purchase&nbsp;Price</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Total&nbsp;Purchase&nbsp;Value</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Normalized&nbsp;Totals</b></td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Female</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">136</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.82</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$382.91</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.83</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Male</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">633</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.95</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$1,867.68</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.02</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Other&nbsp;/&nbsp;Non-Disclosed</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">11</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.25</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$35.74</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.47</td></tr></table>




```python
table_age_group_count_analysis
            
```




<table border="1" cellpadding="3" cellspacing="0"  style="border:black; border-collapse:collapse;"><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b></b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Percentage&nbsp;of&nbsp;Players</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Total&nbsp;Count</b></td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>7-10</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">2.85%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">16</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>11-14</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">8.72%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">49</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>15-18</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">11.74%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">66</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>19-22</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">37.19%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">209</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>23-26</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">21.53%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">121</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>27-30</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">6.94%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">39</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>31-34</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">6.05%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">34</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>35-38</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">3.02%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">17</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>39-42</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">1.78%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">10</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>43-46</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">0.18%</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">1</td></tr></table>




```python
table_age_group_purchase_analysis

```




<table border="1" cellpadding="3" cellspacing="0"  style="border:black; border-collapse:collapse;"><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b></b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Purchase&nbsp;Count</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Average&nbsp;Purchase&nbsp;Price</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Total&nbsp;Purchase&nbsp;Value</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Normalized&nbsp;Totals</b></td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>7-10</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">22</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.09</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$67.91</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.24</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>11-14</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">69</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.86</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$197.39</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.03</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>15-18</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">86</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.86</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$246.06</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.73</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>19-22</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">266</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.88</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$765.31</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.66</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>23-26</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">169</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.02</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$510.02</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.22</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>27-30</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">60</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.96</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$177.40</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.55</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>31-34</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">42</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.11</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$130.64</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.84</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>35-38</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">30</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.75</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$82.38</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.85</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>39-42</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">16</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.19</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$51.03</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$5.10</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>43-46</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">1</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.72</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.72</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.72</td></tr></table>




```python
table_top_spenders_analysis

```




<table border="1" cellpadding="3" cellspacing="0"  style="border:black; border-collapse:collapse;"><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>SN</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Purchase&nbsp;Count</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Average&nbsp;Purchase&nbsp;Price</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Total&nbsp;Purchase&nbsp;Value</b></td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Undirrala66</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">5</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.41</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$17.06</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Saedue76</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">4</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.39</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$13.56</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Mindimnya67</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">4</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.18</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$12.74</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Haellysu29</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">3</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.24</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$12.73</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Eoda93</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">3</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$3.86</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$11.58</td></tr></table>




```python
table_popular_analysis

```




<table border="1" cellpadding="3" cellspacing="0"  style="border:black; border-collapse:collapse;"><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Item&nbsp;ID</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Item&nbsp;Name</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Purchase&nbsp;Count</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Item&nbsp;Price</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Total&nbsp;Purchase&nbsp;Value</b></td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">84</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Thorn,&nbsp;Satchel&nbsp;of&nbsp;Dark&nbsp;Souls</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">11</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.51</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$24.53</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">39</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Stormfury&nbsp;Mace</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">11</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$1.27</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$25.85</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">31</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Shadow&nbsp;Strike,&nbsp;Glory&nbsp;of&nbsp;Ending&nbsp;Hope</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">9</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$1.93</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$18.63</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">34</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Alpha,&nbsp;Reach&nbsp;of&nbsp;Ending&nbsp;Hope</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">9</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$1.55</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$37.26</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">175</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Retribution&nbsp;Axe</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">9</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.14</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$11.16</td></tr></table>




```python
table_profitable_popular_analysis

```




<table border="1" cellpadding="3" cellspacing="0"  style="border:black; border-collapse:collapse;"><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Item&nbsp;ID</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Item&nbsp;Name</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Purchase&nbsp;Count</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Item&nbsp;Price</b></td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;"><b>Total&nbsp;Purchase&nbsp;Value</b></td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">34</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Alpha,&nbsp;Reach&nbsp;of&nbsp;Ending&nbsp;Hope</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">9</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$1.55</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$37.26</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">115</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Thorn,&nbsp;Conqueror&nbsp;of&nbsp;the&nbsp;Corrupted</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">7</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$2.04</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$29.75</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">32</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Rage,&nbsp;Legacy&nbsp;of&nbsp;the&nbsp;Lone&nbsp;Victor</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">6</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.32</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$29.70</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">103</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Mercy,&nbsp;Katana&nbsp;of&nbsp;Dismay</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">6</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.37</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$29.22</td></tr><tr><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">107</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">Spectral&nbsp;Diamond&nbsp;Doomblade</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">8</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$4.25</td><td  style="background-color:White;border-left: 1px solid;border-right: 1px solid;border-top: 1px solid;border-bottom: 1px solid;">$28.88</td></tr></table>



Observed trends:
    1. A majority of players are males (81+%).
    2. Males and age group 19-22 account for a majority of purchase count and purchase values.
       In other words, this game is popular and profitable with this age group and among males.
    3. Alpha, Reach of Ending Hope (#34) is both popular and profitable item in the top 5 popular 
       and profitable lists.

