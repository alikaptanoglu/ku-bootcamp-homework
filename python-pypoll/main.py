import os
import csv
import pandas as pd
import re

#locate CSV files
budget_1_csv = os.path.join('resources', 'budget_data_1.csv')
budget_2_csv = os.path.join('resources', 'budget_data_2.csv')

#create dataframes
bg_1 = pd.read_csv(budget_1_csv)
df_1 = pd.DataFrame(bg_1)
df_1['Date'], df_1['Revenue'] = zip(*df_1['Date,Revenue'].apply(lambda x: x.split(',', 1)))
df_1 = df_1.drop('Date,Revenue', axis=1)

bg_2 = pd.read_csv(budget_2_csv)
df_2 = pd.DataFrame(bg_2)
df_2['Date'], df_2['Revenue'] = zip(*df_2['Date,Revenue'].apply(lambda x: x.split(',', 1)))
df_2 = df_2.drop('Date,Revenue', axis=1)

#reformat date to MMM-yy, dropping the century
pattern = "(\S+[-][1|2][0]\d{2})"
no_matched_1 = df_1[~df_1["Date"].str.contains(pattern, regex=True)]
matched_1 = df_1[df_1["Date"].str.contains(pattern, regex=True)]
if (len(matched_1) > 0):
    matched_1["Date"] = matched_1["Date"].replace('-20', '-')

no_matched_2 = df_2[~df_2["Date"].str.contains(pattern, regex=True)]
matched_2 = df_2[df_2["Date"].str.contains(pattern, regex=True)]
if (len(matched_2) > 0):
    matched_2["Date"] = matched_2["Date"].str.replace('-20', '-')

#combine the dataframes
if (len(no_matched_1) > 0):
    df_combined = no_matched_1
if (len(matched_1) > 0):
    df_combined = df_combined.append(matched_1)
if (len(no_matched_2) > 0):
    df_combined = df_combined.append(no_matched_2)
if (len(matched_2) > 0):
    df_combined = df_combined.append(matched_2)

#split column Date to Month and Year for sorting    
df_combined["Month"], df_combined["Year"] = zip(*df_combined["Date"].apply(lambda x: x.split('-', 1)))

#lookup list for Month name to number
month_look_up = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04',
                 'May':'05', 'Jun':'06', 'Jul':'07', 'Aug':'08', 
                 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}
#replace month name with number for sorting
df_combined["Month"] = df_combined["Month"].apply(lambda x: month_look_up[x])

#change data type of Revenue to Float    
df_combined["Revenue"] = df_combined["Revenue"].astype(float)

#number of non-unique months
total_nonunique_months = df_combined['Date'].count()
#number of unique months
total_unique_months = df_combined['Date'].nunique()
#total revenue
total_revenue = df_combined['Revenue'].sum()

#sort by year, month to calculate sum of revenue by month
df_sorted = df_combined.sort_values(["Year", "Month"], ascending=True)
revenue_by_month = df_sorted.groupby(["Year", "Month"]).sum()

#add Change column, calculate changes from month to month
revenue_by_month["Change"] = revenue_by_month["Revenue"]
previous_value = -1

for i, row in revenue_by_month.iterrows():
    if (previous_value == -1):
        #set first value change to zero
        row["Change"] = 0
        #set first previous value
        previous_value = revenue_by_month.loc[i,"Revenue"] 
    else:
        #calculate change from month to month
        row["Change"] = revenue_by_month.loc[i,"Revenue"] - previous_value
        #set new previous value
        previous_value = revenue_by_month.loc[i,"Revenue"] 

#average revenue change per unique month
average_revenue_change = revenue_by_month["Change"].mean()

#find the greatest increase and greatest decrease
max_change = revenue_by_month["Change"].max()
max_index = revenue_by_month.index[revenue_by_month['Change'] == max_change] 
max_row = revenue_by_month.loc[max_index] 

min_change = revenue_by_month["Change"].min()
min_index = revenue_by_month.index[revenue_by_month['Change'] == min_change] 
min_row = revenue_by_month.loc[min_index] 

max_date = ""
min_date = ""
for i, row in df_combined.iterrows():
    if ((len(max_date) == 0) and (row["Month"] == max_index[0][1]) and (row["Year"] == max_index[0][0])):
        max_date = row["Date"]
     
    if ((len(min_date) == 0) and (row["Month"] == min_index[0][1]) and (row["Year"] == min_index[0][0])):
        min_date = row["Date"]
    
    if ((len(max_date) > 0) and (len(min_date) > 0)):
        break

result = []
result.append("Financial Analysis")
result.append("------------------------------------------------")
result.append(''.join(("Total Months: ", total_nonunique_months.astype(str))))
result.append(''.join(("Total Revenue: $", str(int(total_revenue)))))
result.append(''.join(("Average Revenue Change: $", str(int(average_revenue_change)))))
result.append(''.join(("Greatest Increase in Revenue: ", max_date, " ($", str(int(max_change)), ")")))
result.append(''.join(("Greatest Decrease in Revenue: ", min_date, " ($", str(int(min_change)), ")")))
             
for each in result:
    print(each)                    


#write the result to .csv file
outputpath = os.path.join('resources', "py_bank.csv")

with open(outputpath, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=" ")
    all_rows = zip(result)
    for row in all_rows:
        csvwriter.writerow(row)
        