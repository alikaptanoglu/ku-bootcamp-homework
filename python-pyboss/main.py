import os
import csv
import re
import pandas as pd

#locate CSV files
election_1_csv = os.path.join('resources', 'election_data_1.csv')
election_2_csv = os.path.join('resources', 'election_data_2.csv')

#create dataframes
e_1 = pd.read_csv(election_1_csv)
df_1 = pd.DataFrame(e_1)

e_2 = pd.read_csv(election_2_csv)
df_2 = pd.DataFrame(e_2)

#combine the dataframes
df_combined = df_1.append(df_2)

#create new columns Vote, County, Candidate    
df_combined[['Voter ID', 'County', 'Candidate']] = df_combined['Voter ID,County,Candidate'].str.split(',', expand=True)

#total votes - do not need to use unique assuming each voter only votes once
total_votes = df_combined['Voter ID'].count()

#tally the votes
all_rows = pd.DataFrame(df_combined["Candidate"].value_counts())

#find the winner
winner = df_combined["Candidate"].max()

result = []
result.append("Election Result")
result.append("------------------------------------------------")
result.append(''.join(("Total Votes: ", total_votes.astype(str))))
result.append("------------------------------------------------")

for i, row in all_rows.iterrows():
    percent = (row["Candidate"] / total_votes)
    each = ''.join((i, ": ", "{:.1%}".format(percent), " (", str(row["Candidate"]), ")"))
    result.append(each)

result.append("------------------------------------------------")
result.append(''.join(("Winner: ", winner)))

for each in result:
    print(each)                    


#write the result to .csv file
outputpath = os.path.join('resources', "py_election.csv")

with open(outputpath, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=" ")
    all_rows = zip(result)
    for row in all_rows:
        csvwriter.writerow(row)
        