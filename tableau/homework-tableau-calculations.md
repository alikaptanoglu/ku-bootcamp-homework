

```python
import pymongo
import datetime
import os
import csv
import re
import math
import pandas as pd
import googlemaps
gmaps = googlemaps.Client(key='AIzaSyDTP5kawIfsPBqql2PtLElTahaS3sFdYgQ')

from bson.json_util import dumps
from bson.objectid import ObjectId
from dateutil import parser
from uszipcode import ZipcodeSearchEngine
search_uszipcode = ZipcodeSearchEngine()




# connect to Mongo client using PyMongo
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)
# create new client tableau
db = client.tableau
# get the table
bikedata = db.listings


```


```python
#1. count number of trips since inception
bikedata.count()
```




    19936122




```python
#2. Percentage of ridership grown by year and total from inception 

# count number of trips by year
ridership_pipeline = [
   {"$group": 
        {"_id": {"year": "$year"}, 
         "count": {"$sum": 1}
        }
   }
]

trip_count_by_year = list(bikedata.aggregate(ridership_pipeline))

# build columns for dataframe
year = []
count = []

# calculate the columns
for each in trip_count_by_year:
    year.append(each["_id"]["year"])
    count.append(each["count"])

# fill the dataframe
trip_df = pd.DataFrame()
trip_df["year"] = year
trip_df["count"] = count

# sort year in ascending order
trip_df = trip_df.sort_values("year")

# calculate ridership growth
previous = 0
diff = []
diff_percent = []
for i, row in trip_df.iterrows():
    # start with previous year count
    if previous == 0:
        previous = row["count"]
        
    diff_count = row["count"] - previous
    diff.append(diff_count)
    diff_percent.append("{:.2%}".format(diff_count / row["count"]))
    
    previous = row["count"]

trip_df["diff"] = diff
trip_df["%"] = diff_percent

trip_df

```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>count</th>
      <th>diff</th>
      <th>%</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>5</th>
      <td>2013</td>
      <td>5614776</td>
      <td>0</td>
      <td>0.00%</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2014</td>
      <td>8081207</td>
      <td>2466431</td>
      <td>30.52%</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2015</td>
      <td>5697621</td>
      <td>-2383586</td>
      <td>-41.83%</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2016</td>
      <td>247583</td>
      <td>-5450038</td>
      <td>-2201.30%</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2017</td>
      <td>294933</td>
      <td>47350</td>
      <td>16.05%</td>
    </tr>
    <tr>
      <th>0</th>
      <td>2018</td>
      <td>2</td>
      <td>-294931</td>
      <td>-14746550.00%</td>
    </tr>
  </tbody>
</table>
</div>




```python
#3. Proportional change between customer and subscriber by year 

# count number of customer and subscriber by year
subscription_pipeline = [
   {"$group": 
        {"_id": {"year": "$year", "subscription": "$usertype"}, 
         "count": {"$sum": 1}
        }
   }
]

subscription_count_by_year = list(bikedata.aggregate(subscription_pipeline))


# fill the dataframe
subscription_df = pd.DataFrame(columns=["year", "custcount", "subscount"])
subscription_df.set_index("year")

# calculate the columns
for each in subscription_count_by_year:
    data = {"year": each["_id"]["year"]}

    condition = subscription_df["year"] == each["_id"]["year"]
    found_row = subscription_df.loc[condition] 
    if len(found_row) == 0:
        # add new
        if each["_id"]["subscription"] == "Customer":
            data["custcount"] = each["count"]
        else:
            data["subscount"] = each["count"]
        subscription_df = subscription_df.append(data, ignore_index=True)
    else:
        # update this year
        if each["_id"]["subscription"] == "Customer":
            subscription_df.loc[condition, "custcount"] = each["count"]
        else:
            subscription_df.loc[condition, "subscount"] = each["count"]

# sort year in ascending order
subscription_df = subscription_df.sort_values("year")


# calculate subscription growth
cust_diff = []
subs_diff = []
cust_prev = 0
subs_prev = 0

for i, row in subscription_df.iterrows():
    # start with previous year count
    if cust_prev == 0:
        cust_prev = row["custcount"]
        subs_prev = row["subscount"]

    cust_diff.append(row["custcount"] - cust_prev)
    subs_diff.append(row["subscount"] - subs_prev)
    cust_prev = row["custcount"]
    subs_prev = row["subscount"]


subscription_df["custdiff"] = cust_diff
subscription_df["subsdiff"] = subs_diff
subscription_df["proportion"] = subscription_df["custcount"] / subscription_df["subscount"]

subscription_df



```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>custcount</th>
      <th>subscount</th>
      <th>custdiff</th>
      <th>subsdiff</th>
      <th>proportion</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4</th>
      <td>2013.0</td>
      <td>907249.0</td>
      <td>4707527.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.192723</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2014.0</td>
      <td>793483.0</td>
      <td>7287724.0</td>
      <td>-113766.0</td>
      <td>2580197.0</td>
      <td>0.108879</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2015.0</td>
      <td>791890.0</td>
      <td>4905731.0</td>
      <td>-1593.0</td>
      <td>-2381993.0</td>
      <td>0.161421</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2016.0</td>
      <td>15520.0</td>
      <td>231683.0</td>
      <td>-776370.0</td>
      <td>-4674048.0</td>
      <td>0.066988</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2017.0</td>
      <td>16031.0</td>
      <td>117.0</td>
      <td>511.0</td>
      <td>-231566.0</td>
      <td>137.017094</td>
    </tr>
  </tbody>
</table>
</div>




```python
#4a. List all bikes used during peak hours during Summer, Winter months 
# morning 6AM - 10AM, afternoon 3PM - 7PM

# search criteria for distinct bike during peak hours 
search_distinct = [
    {"$match": {"starthour": {"$in": [6, 7, 8, 9, 10, 15, 16, 17, 18, 19]},
                "season": {"$in": ["Summer", "Winter"]}}
    },
    {"$group": 
       {"_id": {"bikeid": "$bikeid"}} 
    }
]

peak_bikes = list(bikedata.aggregate(search_distinct))

# create new dataframe
peak_bike_df = pd.DataFrame(columns=["bikeid"])
peak_bike_df.set_index(["bikeid"])

for each in peak_bikes:
    data = {"bikeid": each["_id"]["bikeid"]}
    peak_bike_df = peak_bike_df.append(data, ignore_index=True)


# sort bikeid in ascending order
peak_bike_df = peak_bike_df.sort_values(["bikeid"])

peak_bike_df.head()
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>bikeid</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2932</th>
      <td>14529</td>
    </tr>
    <tr>
      <th>1922</th>
      <td>14530</td>
    </tr>
    <tr>
      <th>8542</th>
      <td>14531</td>
    </tr>
    <tr>
      <th>8726</th>
      <td>14532</td>
    </tr>
    <tr>
      <th>6728</th>
      <td>14533</td>
    </tr>
  </tbody>
</table>
</div>




```python
#4b. Peak hours during Summer, Winter months 

# search and count bikes in hours during Summer, Winter months
count_by_hour = [
    {"$match": 
       {"season": {"$in": ["Summer", "Winter"]}
       }
    },
    {"$group": 
       {"_id": {"season": "$season", "starthour": "$starthour"}, 
        "count": {"$sum": 1}
       }
    }
]

peak_hour = list(bikedata.aggregate(count_by_hour))

# create new dataframe
peak_hour_df = pd.DataFrame(columns=["season", "starthour", "count"])
peak_hour_df.set_index(["season", "starthour"])

for each in peak_hour:
    data = {"season": each["_id"]["season"], "starthour": each["_id"]["starthour"], "count": each["count"]}
    peak_hour_df = peak_hour_df.append(data, ignore_index=True)


# sort season, count in ascending order
peak_hour_df = peak_hour_df.sort_values(["season", "count"], ascending=False)

winter_hour_df = peak_hour_df.loc[peak_hour_df["season"] == "Winter"]
winter_hour_10_df = winter_hour_df.head(10)
winter = winter_hour_10_df["starthour"].tolist()

summer_hour_df = peak_hour_df.loc[peak_hour_df["season"] == "Summer"]
summer_hour_10_df = summer_hour_df.head(10)
summer = summer_hour_10_df["starthour"].tolist()

print("Summer ", summer)
print("Winter ", winter) 
    

```

    Summer  [13, 14, 15, 4, 12, 11, 5, 10, 9, 8]
    Winter  [13, 14, 4, 12, 5, 11, 15, 10, 9, 8]
    


```python
#5a. Popular starting stations 

# search and count bikes at starting stations
starting_station = [
    {"$group": 
       {"_id": {"startid": "$startstationid", "startname": "$startstationname", "startlatitude": "$startstationlatitude", "startlongitude": "$startstationlongitude"}, 
        "startcount": {"$sum": 1}
       }
    }
]

starting_stations = list(bikedata.aggregate(starting_station))

# create new dataframe
starting_df = pd.DataFrame(columns=["startid", "startname", "startcount", "startlatitude", "startlongitude", "zipcode"])
starting_df.set_index(["startid"])

for each in starting_stations:
    data = {"startid": each["_id"]["startid"], "startname": each["_id"]["startname"], "startcount": each["startcount"], "startlatitude": each["_id"]["startlatitude"], "startlongitude": each["_id"]["startlongitude"]}

    # find the closest zip code within 5 mile radius 
    try:
        found_zip = search_uszipcode.by_coordinate(data["startlatitude"], data["startlongitude"], radius=5, returns=1)
        data["zipcode"] = found_zip[0]['Zipcode']
    except:
        # find coordinate from the station name plus city and state
        geocode_found = gmaps.geocode(data["startname"] + ", New York City, NY")
        coordinate = geocode_found[0]["geometry"]["location"]
        # find zip code again
        found_zip_again = search_uszipcode.by_coordinate(coordinate["lat"], coordinate["lng"], radius=5, returns=1)
        data["zipcode"] = found_zip_again[0]['Zipcode']

    starting_df = starting_df.append(data, ignore_index=True)

# save to csv
starting_df.to_csv(os.path.join("","starting_stations.csv"))

starting_df = starting_df.sort_values(["startcount"], ascending=False)
starting_df.head(10)
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>startid</th>
      <th>startname</th>
      <th>startcount</th>
      <th>startlatitude</th>
      <th>startlongitude</th>
      <th>zipcode</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>104</th>
      <td>293</td>
      <td>Lafayette St &amp; E 8 St</td>
      <td>191125</td>
      <td>40.730287</td>
      <td>-73.990765</td>
      <td>10003</td>
    </tr>
    <tr>
      <th>230</th>
      <td>497</td>
      <td>E 17 St &amp; Broadway</td>
      <td>188552</td>
      <td>40.737050</td>
      <td>-73.990093</td>
      <td>10003</td>
    </tr>
    <tr>
      <th>527</th>
      <td>435</td>
      <td>W 21 St &amp; 6 Ave</td>
      <td>166234</td>
      <td>40.741740</td>
      <td>-73.994156</td>
      <td>10119</td>
    </tr>
    <tr>
      <th>477</th>
      <td>426</td>
      <td>West St &amp; Chambers St</td>
      <td>158104</td>
      <td>40.717548</td>
      <td>-74.013221</td>
      <td>10282</td>
    </tr>
    <tr>
      <th>532</th>
      <td>285</td>
      <td>Broadway &amp; E 14 St</td>
      <td>155723</td>
      <td>40.734546</td>
      <td>-73.990741</td>
      <td>10003</td>
    </tr>
    <tr>
      <th>101</th>
      <td>521</td>
      <td>8 Ave &amp; W 31 St</td>
      <td>155313</td>
      <td>40.750450</td>
      <td>-73.994811</td>
      <td>10119</td>
    </tr>
    <tr>
      <th>299</th>
      <td>402</td>
      <td>Broadway &amp; E 22 St</td>
      <td>140544</td>
      <td>40.740343</td>
      <td>-73.989551</td>
      <td>10010</td>
    </tr>
    <tr>
      <th>447</th>
      <td>490</td>
      <td>8 Ave &amp; W 33 St</td>
      <td>139060</td>
      <td>40.751551</td>
      <td>-73.993934</td>
      <td>10119</td>
    </tr>
    <tr>
      <th>465</th>
      <td>151</td>
      <td>Cleveland Pl &amp; Spring St</td>
      <td>138963</td>
      <td>40.721816</td>
      <td>-73.997203</td>
      <td>10012</td>
    </tr>
    <tr>
      <th>324</th>
      <td>382</td>
      <td>University Pl &amp; E 14 St</td>
      <td>136078</td>
      <td>40.734927</td>
      <td>-73.992005</td>
      <td>10003</td>
    </tr>
  </tbody>
</table>
</div>




```python
#5b. Popular starting stations over the years

# search and count bikes at starting stations
starting_station = [
    {"$group": 
       {"_id": {"startid": "$startstationid", "startname": "$startstationname", "startyear": "$year", "startlatitude": "$startstationlatitude", "startlongitude": "$startstationlongitude"}, 
        "startcount": {"$sum": 1}
       }
    }
]

starting_stations = list(bikedata.aggregate(starting_station))

# create new dataframe
starting_df = pd.DataFrame(columns=["startid", "startname", "startyear", "startcount", "startlatitude", "startlongitude", "zipcode"])
starting_df.set_index(["startid"])

for each in starting_stations:
    data = {"startid": each["_id"]["startid"], "startname": each["_id"]["startname"], "startyear": each["_id"]["startyear"], "startcount": each["startcount"], "startlatitude": each["_id"]["startlatitude"], "startlongitude": each["_id"]["startlongitude"]}

    # find the closest zip code within 5 mile radius 
    try:
        found_zip = search_uszipcode.by_coordinate(data["startlatitude"], data["startlongitude"], radius=5, returns=1)
        data["zipcode"] = found_zip[0]['Zipcode']
    except:
        # find coordinate from the station name plus city and state
        geocode_found = gmaps.geocode(data["startname"] + ", New York City, NY")
        coordinate = geocode_found[0]["geometry"]["location"]
        # find zip code again
        found_zip_again = search_uszipcode.by_coordinate(coordinate["lat"], coordinate["lng"], radius=5, returns=1)
        data["zipcode"] = found_zip_again[0]['Zipcode']

    starting_df = starting_df.append(data, ignore_index=True)

# save to csv
starting_df.to_csv(os.path.join("","starting_stations_by_year.csv"))


```


```python
#5c. Popular ending stations 

# search and count bikes at ending stations
ending_station = [
    {"$group": 
       {"_id": {"endid": "$endstationid", "endname": "$endstationname", "endlatitude": "$endstationlatitude", "endlongitude": "$endstationlongitude"}, 
        "endcount": {"$sum": 1}
       }
    }
]

ending_stations = list(bikedata.aggregate(ending_station))

# create new dataframe
ending_df = pd.DataFrame(columns=["endid", "endname", "endcount", "endlatitude", "endlongitude", "zipcode"])
ending_df.set_index(["endid"])

for each in ending_stations:
    data = {"endid": each["_id"]["endid"], "endname": each["_id"]["endname"], "endcount": each["endcount"], "endlatitude": each["_id"]["endlatitude"], "endlongitude": each["_id"]["endlongitude"]}

    # find the closest zip code within 5 mile radius 
    try:
        found_zip = search_uszipcode.by_coordinate(data["endlatitude"], data["endlongitude"], radius=5, returns=1)
        data["zipcode"] = found_zip[0]['Zipcode']
    except:
        # find coordinate from the station name plus city and state
        geocode_found = gmaps.geocode(data["endname"] + ", New York City, NY")
        coordinate = geocode_found[0]["geometry"]["location"]
        # find zip code again
        found_zip_again = search_uszipcode.by_coordinate(coordinate["lat"], coordinate["lng"], radius=5, returns=1)
        data["zipcode"] = found_zip_again[0]['Zipcode']

    ending_df = ending_df.append(data, ignore_index=True)

# save to csv
ending_df.to_csv(os.path.join("","ending_stations.csv"))

ending_df = ending_df.sort_values(["endcount"], ascending=False)
ending_df.head(10)
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>endid</th>
      <th>endname</th>
      <th>endcount</th>
      <th>endlatitude</th>
      <th>endlongitude</th>
      <th>zipcode</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>228</th>
      <td>497</td>
      <td>E 17 St &amp; Broadway</td>
      <td>200460</td>
      <td>40.737050</td>
      <td>-73.990093</td>
      <td>10003</td>
    </tr>
    <tr>
      <th>121</th>
      <td>293</td>
      <td>Lafayette St &amp; E 8 St</td>
      <td>187955</td>
      <td>40.730287</td>
      <td>-73.990765</td>
      <td>10003</td>
    </tr>
    <tr>
      <th>548</th>
      <td>435</td>
      <td>W 21 St &amp; 6 Ave</td>
      <td>167641</td>
      <td>40.741740</td>
      <td>-73.994156</td>
      <td>10119</td>
    </tr>
    <tr>
      <th>421</th>
      <td>426</td>
      <td>West St &amp; Chambers St</td>
      <td>160170</td>
      <td>40.717548</td>
      <td>-74.013221</td>
      <td>10282</td>
    </tr>
    <tr>
      <th>300</th>
      <td>285</td>
      <td>Broadway &amp; E 14 St</td>
      <td>153607</td>
      <td>40.734546</td>
      <td>-73.990741</td>
      <td>10003</td>
    </tr>
    <tr>
      <th>502</th>
      <td>382</td>
      <td>University Pl &amp; E 14 St</td>
      <td>141645</td>
      <td>40.734927</td>
      <td>-73.992005</td>
      <td>10003</td>
    </tr>
    <tr>
      <th>338</th>
      <td>402</td>
      <td>Broadway &amp; E 22 St</td>
      <td>141074</td>
      <td>40.740343</td>
      <td>-73.989551</td>
      <td>10010</td>
    </tr>
    <tr>
      <th>518</th>
      <td>151</td>
      <td>Cleveland Pl &amp; Spring St</td>
      <td>138790</td>
      <td>40.721816</td>
      <td>-73.997203</td>
      <td>10012</td>
    </tr>
    <tr>
      <th>149</th>
      <td>499</td>
      <td>Broadway &amp; W 60 St</td>
      <td>135952</td>
      <td>40.769155</td>
      <td>-73.981918</td>
      <td>10019</td>
    </tr>
    <tr>
      <th>371</th>
      <td>459</td>
      <td>W 20 St &amp; 11 Ave</td>
      <td>135708</td>
      <td>40.746745</td>
      <td>-74.007756</td>
      <td>10011</td>
    </tr>
  </tbody>
</table>
</div>




```python
#5d. Popular ending stations over the years 

# search and count bikes at ending stations
ending_station = [
    {"$group": 
       {"_id": {"endid": "$endstationid", "endname": "$endstationname", "endyear": "$year", "endlatitude": "$endstationlatitude", "endlongitude": "$endstationlongitude"}, 
        "endcount": {"$sum": 1}
       }
    }
]

ending_stations = list(bikedata.aggregate(ending_station))

# create new dataframe
ending_df = pd.DataFrame(columns=["endid", "endname", "endyear", "endcount", "endlatitude", "endlongitude", "zipcode"])
ending_df.set_index(["endid"])

for each in ending_stations:
    data = {"endid": each["_id"]["endid"], "endname": each["_id"]["endname"], "endyear": each["_id"]["endyear"], "endcount": each["endcount"], "endlatitude": each["_id"]["endlatitude"], "endlongitude": each["_id"]["endlongitude"]}

    # find the closest zip code within 5 mile radius 
    try:
        found_zip = search_uszipcode.by_coordinate(data["endlatitude"], data["endlongitude"], radius=5, returns=1)
        data["zipcode"] = found_zip[0]['Zipcode']
    except:
        # find coordinate from the station name plus city and state
        geocode_found = gmaps.geocode(data["endname"] + ", New York City, NY")
        coordinate = geocode_found[0]["geometry"]["location"]
        # find zip code again
        found_zip_again = search_uszipcode.by_coordinate(coordinate["lat"], coordinate["lng"], radius=5, returns=1)
        data["zipcode"] = found_zip_again[0]['Zipcode']

    ending_df = ending_df.append(data, ignore_index=True)

# save to csv
ending_df.to_csv(os.path.join("","ending_stations_by_year.csv"))


```


```python
#6. Gender breakdown of participants

# search and count bikes by gender
gender = [
    {"$group": 
       {"_id": {"gender": "$gender"}, 
        "count": {"$sum": 1}
       }
    }
]

genders = list(bikedata.aggregate(gender))

# create new dataframe
gender_df = pd.DataFrame(columns=["gender", "count"])
gender_df.set_index(["gender"])

for each in genders:
    data = {"gender": each["_id"]["gender"], "count": each["count"]}
    gender_df = gender_df.append(data, ignore_index=True)


gender_df
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>gender</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>female</td>
      <td>3985643</td>
    </tr>
    <tr>
      <th>1</th>
      <td>unknown</td>
      <td>2539313</td>
    </tr>
    <tr>
      <th>2</th>
      <td>male</td>
      <td>13411166</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7. Growth in female ridership  

# search and count bikes by gender
female = [
    {"$match": 
       {"gender": {"$eq": "female"}
       }
    },
    {"$group": 
       {"_id": {"year": "$year"}, 
        "count": {"$sum": 1}
       }
    }
]

females = list(bikedata.aggregate(female))

# create new dataframe
female_df = pd.DataFrame(columns=["year", "count"])
female_df.set_index(["year"])

for each in females:
    data = {"year": each["_id"]["year"], "count": each["count"]}
    female_df = female_df.append(data, ignore_index=True)

# sort year in ascending order
female_df = female_df.sort_values("year")


# calculate ridership growth
diff = []
prev = 0

for i, row in female_df.iterrows():
    # start with previous year count
    if prev == 0:
        prev = row["count"]

    diff.append(row["count"] - prev)
    prev = row["count"]


female_df["diff"] = diff
female_df["%"] = female_df["diff"] / female_df["count"]

female_df

```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>count</th>
      <th>diff</th>
      <th>%</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4</th>
      <td>2013</td>
      <td>1108080</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2014</td>
      <td>1650267</td>
      <td>542187</td>
      <td>0.328545</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2015</td>
      <td>1115516</td>
      <td>-534751</td>
      <td>-0.479375</td>
    </tr>
    <tr>
      <th>0</th>
      <td>2016</td>
      <td>50484</td>
      <td>-1065032</td>
      <td>-21.0964</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2017</td>
      <td>61296</td>
      <td>10812</td>
      <td>0.17639</td>
    </tr>
  </tbody>
</table>
</div>




```python
#8. Average trip duration change by age  

# search and average trip duration by age
duration = [
    {"$match": 
       {"birthyear": {"$gt": 0}
       }
    },
    {"$group": 
       {"_id": {"birthyear": "$birthyear"}, 
        "duration": {"$avg": "$tripduration"}
       }
    }
]

durations = list(bikedata.aggregate(duration))

current_year = datetime.datetime.now().year

# create new dataframe
duration_df = pd.DataFrame(columns=["age", "duration"])
duration_df.set_index(["age"])

# current year
datetime.datetime.now()
for each in durations:
    data = {"age": current_year - each["_id"]["birthyear"], "duration": int(round(each["duration"]))}
    duration_df = duration_df.append(data, ignore_index=True)

# sort age in ascending order
duration_df = duration_df.sort_values("age")

# calculate change in duration by age
diff = []
prev = 0

for i, row in duration_df.iterrows():
    # start with previous year count
    if prev == 0:
        prev = row["duration"]

    diff.append(row["duration"] - prev)
    prev = row["duration"]


duration_df["diff"] = diff
duration_df["%"] = duration_df["diff"] / duration_df["duration"]
duration_df 
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>age</th>
      <th>duration</th>
      <th>diff</th>
      <th>%</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>17</td>
      <td>1552</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>18</td>
      <td>1258</td>
      <td>-294</td>
      <td>-0.233704</td>
    </tr>
    <tr>
      <th>1</th>
      <td>19</td>
      <td>692</td>
      <td>-566</td>
      <td>-0.817919</td>
    </tr>
    <tr>
      <th>2</th>
      <td>20</td>
      <td>1145</td>
      <td>453</td>
      <td>0.395633</td>
    </tr>
    <tr>
      <th>15</th>
      <td>21</td>
      <td>762</td>
      <td>-383</td>
      <td>-0.502625</td>
    </tr>
    <tr>
      <th>41</th>
      <td>22</td>
      <td>819</td>
      <td>57</td>
      <td>0.0695971</td>
    </tr>
    <tr>
      <th>19</th>
      <td>23</td>
      <td>767</td>
      <td>-52</td>
      <td>-0.0677966</td>
    </tr>
    <tr>
      <th>81</th>
      <td>24</td>
      <td>651</td>
      <td>-116</td>
      <td>-0.178187</td>
    </tr>
    <tr>
      <th>35</th>
      <td>25</td>
      <td>689</td>
      <td>38</td>
      <td>0.0551524</td>
    </tr>
    <tr>
      <th>34</th>
      <td>26</td>
      <td>706</td>
      <td>17</td>
      <td>0.0240793</td>
    </tr>
    <tr>
      <th>79</th>
      <td>27</td>
      <td>744</td>
      <td>38</td>
      <td>0.0510753</td>
    </tr>
    <tr>
      <th>42</th>
      <td>28</td>
      <td>747</td>
      <td>3</td>
      <td>0.00401606</td>
    </tr>
    <tr>
      <th>69</th>
      <td>29</td>
      <td>755</td>
      <td>8</td>
      <td>0.010596</td>
    </tr>
    <tr>
      <th>86</th>
      <td>30</td>
      <td>764</td>
      <td>9</td>
      <td>0.0117801</td>
    </tr>
    <tr>
      <th>82</th>
      <td>31</td>
      <td>765</td>
      <td>1</td>
      <td>0.00130719</td>
    </tr>
    <tr>
      <th>49</th>
      <td>32</td>
      <td>757</td>
      <td>-8</td>
      <td>-0.010568</td>
    </tr>
    <tr>
      <th>58</th>
      <td>33</td>
      <td>760</td>
      <td>3</td>
      <td>0.00394737</td>
    </tr>
    <tr>
      <th>89</th>
      <td>34</td>
      <td>757</td>
      <td>-3</td>
      <td>-0.00396301</td>
    </tr>
    <tr>
      <th>88</th>
      <td>35</td>
      <td>764</td>
      <td>7</td>
      <td>0.0091623</td>
    </tr>
    <tr>
      <th>63</th>
      <td>36</td>
      <td>776</td>
      <td>12</td>
      <td>0.0154639</td>
    </tr>
    <tr>
      <th>78</th>
      <td>37</td>
      <td>763</td>
      <td>-13</td>
      <td>-0.017038</td>
    </tr>
    <tr>
      <th>80</th>
      <td>38</td>
      <td>762</td>
      <td>-1</td>
      <td>-0.00131234</td>
    </tr>
    <tr>
      <th>75</th>
      <td>39</td>
      <td>770</td>
      <td>8</td>
      <td>0.0103896</td>
    </tr>
    <tr>
      <th>47</th>
      <td>40</td>
      <td>773</td>
      <td>3</td>
      <td>0.00388098</td>
    </tr>
    <tr>
      <th>74</th>
      <td>41</td>
      <td>794</td>
      <td>21</td>
      <td>0.0264484</td>
    </tr>
    <tr>
      <th>54</th>
      <td>42</td>
      <td>770</td>
      <td>-24</td>
      <td>-0.0311688</td>
    </tr>
    <tr>
      <th>51</th>
      <td>43</td>
      <td>819</td>
      <td>49</td>
      <td>0.0598291</td>
    </tr>
    <tr>
      <th>52</th>
      <td>44</td>
      <td>780</td>
      <td>-39</td>
      <td>-0.05</td>
    </tr>
    <tr>
      <th>77</th>
      <td>45</td>
      <td>795</td>
      <td>15</td>
      <td>0.0188679</td>
    </tr>
    <tr>
      <th>53</th>
      <td>46</td>
      <td>776</td>
      <td>-19</td>
      <td>-0.0244845</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>22</th>
      <td>78</td>
      <td>776</td>
      <td>-91</td>
      <td>-0.117268</td>
    </tr>
    <tr>
      <th>23</th>
      <td>79</td>
      <td>983</td>
      <td>207</td>
      <td>0.21058</td>
    </tr>
    <tr>
      <th>13</th>
      <td>80</td>
      <td>1056</td>
      <td>73</td>
      <td>0.0691288</td>
    </tr>
    <tr>
      <th>20</th>
      <td>81</td>
      <td>845</td>
      <td>-211</td>
      <td>-0.249704</td>
    </tr>
    <tr>
      <th>33</th>
      <td>82</td>
      <td>937</td>
      <td>92</td>
      <td>0.0981857</td>
    </tr>
    <tr>
      <th>10</th>
      <td>83</td>
      <td>859</td>
      <td>-78</td>
      <td>-0.0908033</td>
    </tr>
    <tr>
      <th>40</th>
      <td>84</td>
      <td>592</td>
      <td>-267</td>
      <td>-0.451014</td>
    </tr>
    <tr>
      <th>14</th>
      <td>85</td>
      <td>619</td>
      <td>27</td>
      <td>0.0436187</td>
    </tr>
    <tr>
      <th>11</th>
      <td>86</td>
      <td>803</td>
      <td>184</td>
      <td>0.229141</td>
    </tr>
    <tr>
      <th>4</th>
      <td>87</td>
      <td>361</td>
      <td>-442</td>
      <td>-1.22438</td>
    </tr>
    <tr>
      <th>9</th>
      <td>88</td>
      <td>1126</td>
      <td>765</td>
      <td>0.679396</td>
    </tr>
    <tr>
      <th>6</th>
      <td>89</td>
      <td>1345</td>
      <td>219</td>
      <td>0.162825</td>
    </tr>
    <tr>
      <th>7</th>
      <td>91</td>
      <td>515</td>
      <td>-830</td>
      <td>-1.61165</td>
    </tr>
    <tr>
      <th>85</th>
      <td>92</td>
      <td>870</td>
      <td>355</td>
      <td>0.408046</td>
    </tr>
    <tr>
      <th>30</th>
      <td>93</td>
      <td>779</td>
      <td>-91</td>
      <td>-0.116816</td>
    </tr>
    <tr>
      <th>5</th>
      <td>94</td>
      <td>639</td>
      <td>-140</td>
      <td>-0.219092</td>
    </tr>
    <tr>
      <th>72</th>
      <td>95</td>
      <td>545</td>
      <td>-94</td>
      <td>-0.172477</td>
    </tr>
    <tr>
      <th>59</th>
      <td>96</td>
      <td>590</td>
      <td>45</td>
      <td>0.0762712</td>
    </tr>
    <tr>
      <th>84</th>
      <td>97</td>
      <td>570</td>
      <td>-20</td>
      <td>-0.0350877</td>
    </tr>
    <tr>
      <th>8</th>
      <td>98</td>
      <td>715</td>
      <td>145</td>
      <td>0.202797</td>
    </tr>
    <tr>
      <th>3</th>
      <td>101</td>
      <td>1015</td>
      <td>300</td>
      <td>0.295567</td>
    </tr>
    <tr>
      <th>64</th>
      <td>105</td>
      <td>1393</td>
      <td>378</td>
      <td>0.271357</td>
    </tr>
    <tr>
      <th>39</th>
      <td>108</td>
      <td>1193</td>
      <td>-200</td>
      <td>-0.167645</td>
    </tr>
    <tr>
      <th>28</th>
      <td>111</td>
      <td>781</td>
      <td>-412</td>
      <td>-0.527529</td>
    </tr>
    <tr>
      <th>17</th>
      <td>115</td>
      <td>835</td>
      <td>54</td>
      <td>0.0646707</td>
    </tr>
    <tr>
      <th>67</th>
      <td>117</td>
      <td>860</td>
      <td>25</td>
      <td>0.0290698</td>
    </tr>
    <tr>
      <th>65</th>
      <td>118</td>
      <td>1023</td>
      <td>163</td>
      <td>0.159335</td>
    </tr>
    <tr>
      <th>16</th>
      <td>119</td>
      <td>1195</td>
      <td>172</td>
      <td>0.143933</td>
    </tr>
    <tr>
      <th>66</th>
      <td>131</td>
      <td>566</td>
      <td>-629</td>
      <td>-1.11131</td>
    </tr>
    <tr>
      <th>44</th>
      <td>133</td>
      <td>1017</td>
      <td>451</td>
      <td>0.443461</td>
    </tr>
  </tbody>
</table>
<p>91 rows × 4 columns</p>
</div>




```python
#9. Average distance in miles that a bike is ridden   

# search and average distance in miles for each bike
distance = [
    {"$group": 
       {"_id": {"bikeid": "$bikeid"}, 
        "distance": {"$avg": "$distance"}
       }
    }
]

distances = list(bikedata.aggregate(distance))

# create new dataframe
distance_df = pd.DataFrame(columns=["bikeid", "distance"])
distance_df.set_index(["bikeid"])

for each in distances:
    data = {"bikeid": each["_id"]["bikeid"], "distance": each["distance"]}
    distance_df = distance_df.append(data, ignore_index=True)

# sort distance in descending order
distance_df = distance_df.sort_values(["distance"], ascending=False)

distance_df
distance_df["distance"].mean()

```




    6.570636729542504




```python
#10. List bikes that are due for repair or inspection
# On average, a bike is due for repair or inspection after 
# 500 miles - 2,500 miles - 6,000 miles

# sum distance in miles for each bike
repair = [
    {"$group": 
       {"_id": {"bikeid": "$bikeid"}, 
        "distance": {"$sum": "$distance"}
       }
    }
]

repairs = list(bikedata.aggregate(repair))

# create new dataframe
repair_df = pd.DataFrame(columns=["bikeid", "distance", "due"])
repair_df.set_index(["bikeid"])

for each in repairs:
    data = {"bikeid": each["_id"]["bikeid"], "distance": each["distance"]}
    
    # check to see if the bike is due for repair or inspection
    # 6,000 miles first, then 2,500 miles, 500 miles
    # 0 is for no inspection or repair
    # set the default
    
    if each["distance"] == 0:
        data["due"] = 0
    elif each["distance"] < 500:
        data["due"] = 0
    elif each["distance"] > 6000:
        data["due"] = 6000
    elif each["distance"] > 2500:
        data["due"] = 2500
    elif each["distance"] > 500:
        data["due"] = 500
            
    repair_df = repair_df.append(data, ignore_index=True)

# sort repair in descending order
repair_df = repair_df.sort_values(["due"], ascending=False)

repair_df = repair_df[repair_df["due"] > 0]
repair_df

```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>bikeid</th>
      <th>distance</th>
      <th>due</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>5210</th>
      <td>17324.0</td>
      <td>14552.913615</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4468</th>
      <td>19154.0</td>
      <td>25094.840648</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4478</th>
      <td>18772.0</td>
      <td>8694.983338</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4477</th>
      <td>14963.0</td>
      <td>24898.449879</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4476</th>
      <td>18177.0</td>
      <td>39420.871941</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4475</th>
      <td>17820.0</td>
      <td>12263.498758</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4474</th>
      <td>18450.0</td>
      <td>35131.289650</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>7539</th>
      <td>14799.0</td>
      <td>19233.448706</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4472</th>
      <td>16618.0</td>
      <td>23293.494334</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4471</th>
      <td>18323.0</td>
      <td>8157.440794</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4470</th>
      <td>19162.0</td>
      <td>25075.814818</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4469</th>
      <td>18083.0</td>
      <td>14010.277051</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4466</th>
      <td>17629.0</td>
      <td>9012.903171</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4511</th>
      <td>17048.0</td>
      <td>19650.370793</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4465</th>
      <td>19960.0</td>
      <td>14398.209935</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>7540</th>
      <td>15320.0</td>
      <td>19305.452083</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>7541</th>
      <td>15311.0</td>
      <td>29349.016791</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4462</th>
      <td>16435.0</td>
      <td>24511.220238</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4461</th>
      <td>16050.0</td>
      <td>13961.369242</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4460</th>
      <td>17981.0</td>
      <td>19214.175178</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4459</th>
      <td>18804.0</td>
      <td>19794.869850</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4458</th>
      <td>18779.0</td>
      <td>8553.810642</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4457</th>
      <td>15932.0</td>
      <td>19505.823779</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4456</th>
      <td>18742.0</td>
      <td>29781.464772</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4479</th>
      <td>17522.0</td>
      <td>19555.405817</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>7538</th>
      <td>16232.0</td>
      <td>19354.344854</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4481</th>
      <td>15719.0</td>
      <td>24943.744648</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4482</th>
      <td>17262.0</td>
      <td>14040.676652</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4508</th>
      <td>14638.0</td>
      <td>28716.802857</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>4507</th>
      <td>29510.0</td>
      <td>16435.523466</td>
      <td>6000.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>501</th>
      <td>24616.0</td>
      <td>639.371317</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>6820</th>
      <td>22172.0</td>
      <td>879.176826</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4301</th>
      <td>21526.0</td>
      <td>2246.623344</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>3528</th>
      <td>24713.0</td>
      <td>524.834892</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>500</th>
      <td>24572.0</td>
      <td>767.498706</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>499</th>
      <td>24608.0</td>
      <td>638.029510</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4312</th>
      <td>21491.0</td>
      <td>1948.149558</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4313</th>
      <td>24654.0</td>
      <td>505.173202</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4316</th>
      <td>21428.0</td>
      <td>609.377790</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4320</th>
      <td>22540.0</td>
      <td>694.283387</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4323</th>
      <td>21609.0</td>
      <td>2186.811349</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4327</th>
      <td>22176.0</td>
      <td>710.610523</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>3527</th>
      <td>21227.0</td>
      <td>2395.612977</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>1468</th>
      <td>22926.0</td>
      <td>618.201731</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>1469</th>
      <td>22930.0</td>
      <td>551.558792</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4328</th>
      <td>19811.0</td>
      <td>1020.144418</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>586</th>
      <td>24470.0</td>
      <td>631.802921</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>1470</th>
      <td>22201.0</td>
      <td>578.446790</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4266</th>
      <td>20705.0</td>
      <td>1568.617903</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4296</th>
      <td>22533.0</td>
      <td>740.532339</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>6913</th>
      <td>22612.0</td>
      <td>685.166629</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4270</th>
      <td>20720.0</td>
      <td>2224.233542</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>1433</th>
      <td>22981.0</td>
      <td>567.841204</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4277</th>
      <td>24542.0</td>
      <td>555.824475</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>1432</th>
      <td>22979.0</td>
      <td>698.554415</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>1431</th>
      <td>22985.0</td>
      <td>656.982538</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>505</th>
      <td>24660.0</td>
      <td>577.581044</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>4281</th>
      <td>21233.0</td>
      <td>2133.738518</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>506</th>
      <td>24634.0</td>
      <td>643.388832</td>
      <td>500.0</td>
    </tr>
    <tr>
      <th>507</th>
      <td>24663.0</td>
      <td>679.304341</td>
      <td>500.0</td>
    </tr>
  </tbody>
</table>
<p>8503 rows × 3 columns</p>
</div>




```python
5535.706064 % 1000

```




    535.706064




```python
#11. Utilization by bike ID each year
# utilization rate is calculated by adding all trip durations per bike
# compared to available time in a year 
# 365 * 24 * 60 * 60 seconds or 366 for a leap year
# = 31,536,000                  = 31,622,400


# sum trip duration in seconds for each bike
utilization = [
    {"$group": 
       {"_id": {"bikeid": "$bikeid", "year": "$year"}, 
        "duration": {"$sum": "$tripduration"}
       }
    }
]

utilizations = list(bikedata.aggregate(utilization))

# create new dataframe
utilization_df = pd.DataFrame(columns=["bikeid", "year", "duration", "utilization"])
utilization_df.set_index(["bikeid", "year"])

for each in utilizations:
    data = {"bikeid": each["_id"]["bikeid"], "year": each["_id"]["year"], "duration": each["duration"]}

    isLeapYear = (data["year"] % 4 == 0)
    if isLeapYear:
        data["utilization"] = data["duration"] / 31622400
    else:
        data["utilization"] = data["duration"] / 31536000
            
    utilization_df = utilization_df.append(data, ignore_index=True)

# sort utilization in descending order
utilization_df = utilization_df.sort_values(["utilization", "year"], ascending=False)

utilization_df_2013 = utilization_df[utilization_df["year"] == 2013]
utilization_df_2014 = utilization_df[utilization_df["year"] == 2014]
utilization_df_2015 = utilization_df[utilization_df["year"] == 2015]
utilization_df_2016 = utilization_df[utilization_df["year"] == 2016]
utilization_df_2017 = utilization_df[utilization_df["year"] == 2017]

```


```python
utilization_df_2013

```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>bikeid</th>
      <th>year</th>
      <th>duration</th>
      <th>utilization</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>20702</th>
      <td>16808.0</td>
      <td>2013.0</td>
      <td>6758988.0</td>
      <td>0.214326</td>
    </tr>
    <tr>
      <th>22515</th>
      <td>15259.0</td>
      <td>2013.0</td>
      <td>4686444.0</td>
      <td>0.148606</td>
    </tr>
    <tr>
      <th>15981</th>
      <td>17918.0</td>
      <td>2013.0</td>
      <td>3775054.0</td>
      <td>0.119706</td>
    </tr>
    <tr>
      <th>14919</th>
      <td>19866.0</td>
      <td>2013.0</td>
      <td>2927764.0</td>
      <td>0.092839</td>
    </tr>
    <tr>
      <th>15340</th>
      <td>19356.0</td>
      <td>2013.0</td>
      <td>2910331.0</td>
      <td>0.092286</td>
    </tr>
    <tr>
      <th>23430</th>
      <td>20354.0</td>
      <td>2013.0</td>
      <td>2825162.0</td>
      <td>0.089585</td>
    </tr>
    <tr>
      <th>14723</th>
      <td>17215.0</td>
      <td>2013.0</td>
      <td>2270742.0</td>
      <td>0.072005</td>
    </tr>
    <tr>
      <th>18432</th>
      <td>19704.0</td>
      <td>2013.0</td>
      <td>2236168.0</td>
      <td>0.070908</td>
    </tr>
    <tr>
      <th>13305</th>
      <td>17806.0</td>
      <td>2013.0</td>
      <td>2123786.0</td>
      <td>0.067345</td>
    </tr>
    <tr>
      <th>17798</th>
      <td>16083.0</td>
      <td>2013.0</td>
      <td>2105909.0</td>
      <td>0.066778</td>
    </tr>
    <tr>
      <th>15605</th>
      <td>17171.0</td>
      <td>2013.0</td>
      <td>2080335.0</td>
      <td>0.065967</td>
    </tr>
    <tr>
      <th>15861</th>
      <td>17917.0</td>
      <td>2013.0</td>
      <td>1898847.0</td>
      <td>0.060212</td>
    </tr>
    <tr>
      <th>19266</th>
      <td>19505.0</td>
      <td>2013.0</td>
      <td>1810549.0</td>
      <td>0.057412</td>
    </tr>
    <tr>
      <th>17746</th>
      <td>18152.0</td>
      <td>2013.0</td>
      <td>1762615.0</td>
      <td>0.055892</td>
    </tr>
    <tr>
      <th>22940</th>
      <td>17884.0</td>
      <td>2013.0</td>
      <td>1754416.0</td>
      <td>0.055632</td>
    </tr>
    <tr>
      <th>12847</th>
      <td>18131.0</td>
      <td>2013.0</td>
      <td>1706977.0</td>
      <td>0.054128</td>
    </tr>
    <tr>
      <th>23263</th>
      <td>14993.0</td>
      <td>2013.0</td>
      <td>1686385.0</td>
      <td>0.053475</td>
    </tr>
    <tr>
      <th>14225</th>
      <td>14535.0</td>
      <td>2013.0</td>
      <td>1618200.0</td>
      <td>0.051313</td>
    </tr>
    <tr>
      <th>20316</th>
      <td>18364.0</td>
      <td>2013.0</td>
      <td>1608213.0</td>
      <td>0.050996</td>
    </tr>
    <tr>
      <th>17980</th>
      <td>17846.0</td>
      <td>2013.0</td>
      <td>1604748.0</td>
      <td>0.050886</td>
    </tr>
    <tr>
      <th>12462</th>
      <td>15314.0</td>
      <td>2013.0</td>
      <td>1589671.0</td>
      <td>0.050408</td>
    </tr>
    <tr>
      <th>14604</th>
      <td>19755.0</td>
      <td>2013.0</td>
      <td>1557586.0</td>
      <td>0.049391</td>
    </tr>
    <tr>
      <th>16238</th>
      <td>15805.0</td>
      <td>2013.0</td>
      <td>1545219.0</td>
      <td>0.048999</td>
    </tr>
    <tr>
      <th>17660</th>
      <td>19307.0</td>
      <td>2013.0</td>
      <td>1543525.0</td>
      <td>0.048945</td>
    </tr>
    <tr>
      <th>17790</th>
      <td>17282.0</td>
      <td>2013.0</td>
      <td>1526893.0</td>
      <td>0.048417</td>
    </tr>
    <tr>
      <th>13236</th>
      <td>15948.0</td>
      <td>2013.0</td>
      <td>1524443.0</td>
      <td>0.048340</td>
    </tr>
    <tr>
      <th>13527</th>
      <td>16984.0</td>
      <td>2013.0</td>
      <td>1512384.0</td>
      <td>0.047957</td>
    </tr>
    <tr>
      <th>18319</th>
      <td>15322.0</td>
      <td>2013.0</td>
      <td>1509198.0</td>
      <td>0.047856</td>
    </tr>
    <tr>
      <th>21680</th>
      <td>19010.0</td>
      <td>2013.0</td>
      <td>1508931.0</td>
      <td>0.047848</td>
    </tr>
    <tr>
      <th>15810</th>
      <td>19261.0</td>
      <td>2013.0</td>
      <td>1507832.0</td>
      <td>0.047813</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>20592</th>
      <td>21271.0</td>
      <td>2013.0</td>
      <td>3559.0</td>
      <td>0.000113</td>
    </tr>
    <tr>
      <th>11639</th>
      <td>21307.0</td>
      <td>2013.0</td>
      <td>3268.0</td>
      <td>0.000104</td>
    </tr>
    <tr>
      <th>13295</th>
      <td>21128.0</td>
      <td>2013.0</td>
      <td>3075.0</td>
      <td>0.000098</td>
    </tr>
    <tr>
      <th>11800</th>
      <td>21155.0</td>
      <td>2013.0</td>
      <td>2966.0</td>
      <td>0.000094</td>
    </tr>
    <tr>
      <th>14897</th>
      <td>21324.0</td>
      <td>2013.0</td>
      <td>2900.0</td>
      <td>0.000092</td>
    </tr>
    <tr>
      <th>16597</th>
      <td>21066.0</td>
      <td>2013.0</td>
      <td>2812.0</td>
      <td>0.000089</td>
    </tr>
    <tr>
      <th>11635</th>
      <td>21328.0</td>
      <td>2013.0</td>
      <td>2736.0</td>
      <td>0.000087</td>
    </tr>
    <tr>
      <th>19593</th>
      <td>21286.0</td>
      <td>2013.0</td>
      <td>2427.0</td>
      <td>0.000077</td>
    </tr>
    <tr>
      <th>20031</th>
      <td>21326.0</td>
      <td>2013.0</td>
      <td>2381.0</td>
      <td>0.000076</td>
    </tr>
    <tr>
      <th>11689</th>
      <td>14689.0</td>
      <td>2013.0</td>
      <td>2290.0</td>
      <td>0.000073</td>
    </tr>
    <tr>
      <th>11650</th>
      <td>21315.0</td>
      <td>2013.0</td>
      <td>2272.0</td>
      <td>0.000072</td>
    </tr>
    <tr>
      <th>11641</th>
      <td>21321.0</td>
      <td>2013.0</td>
      <td>2211.0</td>
      <td>0.000070</td>
    </tr>
    <tr>
      <th>11637</th>
      <td>21305.0</td>
      <td>2013.0</td>
      <td>1939.0</td>
      <td>0.000061</td>
    </tr>
    <tr>
      <th>11646</th>
      <td>21295.0</td>
      <td>2013.0</td>
      <td>1919.0</td>
      <td>0.000061</td>
    </tr>
    <tr>
      <th>15298</th>
      <td>21333.0</td>
      <td>2013.0</td>
      <td>1660.0</td>
      <td>0.000053</td>
    </tr>
    <tr>
      <th>11659</th>
      <td>21304.0</td>
      <td>2013.0</td>
      <td>1522.0</td>
      <td>0.000048</td>
    </tr>
    <tr>
      <th>18930</th>
      <td>21313.0</td>
      <td>2013.0</td>
      <td>1462.0</td>
      <td>0.000046</td>
    </tr>
    <tr>
      <th>20888</th>
      <td>14724.0</td>
      <td>2013.0</td>
      <td>1453.0</td>
      <td>0.000046</td>
    </tr>
    <tr>
      <th>20568</th>
      <td>21256.0</td>
      <td>2013.0</td>
      <td>1319.0</td>
      <td>0.000042</td>
    </tr>
    <tr>
      <th>16337</th>
      <td>21298.0</td>
      <td>2013.0</td>
      <td>1151.0</td>
      <td>0.000036</td>
    </tr>
    <tr>
      <th>16239</th>
      <td>20927.0</td>
      <td>2013.0</td>
      <td>1011.0</td>
      <td>0.000032</td>
    </tr>
    <tr>
      <th>21862</th>
      <td>21293.0</td>
      <td>2013.0</td>
      <td>944.0</td>
      <td>0.000030</td>
    </tr>
    <tr>
      <th>11634</th>
      <td>21311.0</td>
      <td>2013.0</td>
      <td>810.0</td>
      <td>0.000026</td>
    </tr>
    <tr>
      <th>11707</th>
      <td>21287.0</td>
      <td>2013.0</td>
      <td>753.0</td>
      <td>0.000024</td>
    </tr>
    <tr>
      <th>13986</th>
      <td>21331.0</td>
      <td>2013.0</td>
      <td>723.0</td>
      <td>0.000023</td>
    </tr>
    <tr>
      <th>17338</th>
      <td>21078.0</td>
      <td>2013.0</td>
      <td>670.0</td>
      <td>0.000021</td>
    </tr>
    <tr>
      <th>12829</th>
      <td>20502.0</td>
      <td>2013.0</td>
      <td>556.0</td>
      <td>0.000018</td>
    </tr>
    <tr>
      <th>20878</th>
      <td>21172.0</td>
      <td>2013.0</td>
      <td>386.0</td>
      <td>0.000012</td>
    </tr>
    <tr>
      <th>19356</th>
      <td>21285.0</td>
      <td>2013.0</td>
      <td>359.0</td>
      <td>0.000011</td>
    </tr>
    <tr>
      <th>23582</th>
      <td>20504.0</td>
      <td>2013.0</td>
      <td>225.0</td>
      <td>0.000007</td>
    </tr>
  </tbody>
</table>
<p>6533 rows × 4 columns</p>
</div>




```python
utilization_df_2014
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>bikeid</th>
      <th>year</th>
      <th>duration</th>
      <th>utilization</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>9285</th>
      <td>15561.0</td>
      <td>2014.0</td>
      <td>6444760.0</td>
      <td>0.204362</td>
    </tr>
    <tr>
      <th>13390</th>
      <td>18380.0</td>
      <td>2014.0</td>
      <td>5638985.0</td>
      <td>0.178811</td>
    </tr>
    <tr>
      <th>14997</th>
      <td>14923.0</td>
      <td>2014.0</td>
      <td>5012275.0</td>
      <td>0.158938</td>
    </tr>
    <tr>
      <th>11731</th>
      <td>14726.0</td>
      <td>2014.0</td>
      <td>4338017.0</td>
      <td>0.137558</td>
    </tr>
    <tr>
      <th>7961</th>
      <td>19481.0</td>
      <td>2014.0</td>
      <td>3529662.0</td>
      <td>0.111925</td>
    </tr>
    <tr>
      <th>22451</th>
      <td>17902.0</td>
      <td>2014.0</td>
      <td>3508434.0</td>
      <td>0.111252</td>
    </tr>
    <tr>
      <th>7034</th>
      <td>15144.0</td>
      <td>2014.0</td>
      <td>3300765.0</td>
      <td>0.104667</td>
    </tr>
    <tr>
      <th>8349</th>
      <td>15132.0</td>
      <td>2014.0</td>
      <td>3027142.0</td>
      <td>0.095990</td>
    </tr>
    <tr>
      <th>12584</th>
      <td>19943.0</td>
      <td>2014.0</td>
      <td>2972051.0</td>
      <td>0.094243</td>
    </tr>
    <tr>
      <th>16771</th>
      <td>19997.0</td>
      <td>2014.0</td>
      <td>2859930.0</td>
      <td>0.090688</td>
    </tr>
    <tr>
      <th>11656</th>
      <td>15075.0</td>
      <td>2014.0</td>
      <td>2611101.0</td>
      <td>0.082797</td>
    </tr>
    <tr>
      <th>17141</th>
      <td>21237.0</td>
      <td>2014.0</td>
      <td>2498988.0</td>
      <td>0.079242</td>
    </tr>
    <tr>
      <th>18759</th>
      <td>20171.0</td>
      <td>2014.0</td>
      <td>2360880.0</td>
      <td>0.074863</td>
    </tr>
    <tr>
      <th>13989</th>
      <td>17980.0</td>
      <td>2014.0</td>
      <td>2348175.0</td>
      <td>0.074460</td>
    </tr>
    <tr>
      <th>6944</th>
      <td>17899.0</td>
      <td>2014.0</td>
      <td>2277075.0</td>
      <td>0.072206</td>
    </tr>
    <tr>
      <th>9401</th>
      <td>15294.0</td>
      <td>2014.0</td>
      <td>2228966.0</td>
      <td>0.070680</td>
    </tr>
    <tr>
      <th>10622</th>
      <td>21005.0</td>
      <td>2014.0</td>
      <td>2203705.0</td>
      <td>0.069879</td>
    </tr>
    <tr>
      <th>6220</th>
      <td>21624.0</td>
      <td>2014.0</td>
      <td>2185094.0</td>
      <td>0.069289</td>
    </tr>
    <tr>
      <th>15171</th>
      <td>21291.0</td>
      <td>2014.0</td>
      <td>2120053.0</td>
      <td>0.067226</td>
    </tr>
    <tr>
      <th>7383</th>
      <td>21376.0</td>
      <td>2014.0</td>
      <td>2116354.0</td>
      <td>0.067109</td>
    </tr>
    <tr>
      <th>11312</th>
      <td>15258.0</td>
      <td>2014.0</td>
      <td>2112579.0</td>
      <td>0.066989</td>
    </tr>
    <tr>
      <th>14348</th>
      <td>16698.0</td>
      <td>2014.0</td>
      <td>2051123.0</td>
      <td>0.065041</td>
    </tr>
    <tr>
      <th>8379</th>
      <td>21251.0</td>
      <td>2014.0</td>
      <td>2025718.0</td>
      <td>0.064235</td>
    </tr>
    <tr>
      <th>10963</th>
      <td>17777.0</td>
      <td>2014.0</td>
      <td>1998897.0</td>
      <td>0.063385</td>
    </tr>
    <tr>
      <th>10577</th>
      <td>19031.0</td>
      <td>2014.0</td>
      <td>1996337.0</td>
      <td>0.063303</td>
    </tr>
    <tr>
      <th>8951</th>
      <td>15679.0</td>
      <td>2014.0</td>
      <td>1994741.0</td>
      <td>0.063253</td>
    </tr>
    <tr>
      <th>14923</th>
      <td>21602.0</td>
      <td>2014.0</td>
      <td>1993388.0</td>
      <td>0.063210</td>
    </tr>
    <tr>
      <th>6219</th>
      <td>21627.0</td>
      <td>2014.0</td>
      <td>1911145.0</td>
      <td>0.060602</td>
    </tr>
    <tr>
      <th>11341</th>
      <td>14733.0</td>
      <td>2014.0</td>
      <td>1898104.0</td>
      <td>0.060188</td>
    </tr>
    <tr>
      <th>9129</th>
      <td>21089.0</td>
      <td>2014.0</td>
      <td>1871685.0</td>
      <td>0.059351</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>13359</th>
      <td>15345.0</td>
      <td>2014.0</td>
      <td>28197.0</td>
      <td>0.000894</td>
    </tr>
    <tr>
      <th>21618</th>
      <td>14735.0</td>
      <td>2014.0</td>
      <td>27287.0</td>
      <td>0.000865</td>
    </tr>
    <tr>
      <th>10538</th>
      <td>18294.0</td>
      <td>2014.0</td>
      <td>25544.0</td>
      <td>0.000810</td>
    </tr>
    <tr>
      <th>11693</th>
      <td>20776.0</td>
      <td>2014.0</td>
      <td>23753.0</td>
      <td>0.000753</td>
    </tr>
    <tr>
      <th>11316</th>
      <td>19844.0</td>
      <td>2014.0</td>
      <td>23520.0</td>
      <td>0.000746</td>
    </tr>
    <tr>
      <th>18709</th>
      <td>16602.0</td>
      <td>2014.0</td>
      <td>22485.0</td>
      <td>0.000713</td>
    </tr>
    <tr>
      <th>6992</th>
      <td>16622.0</td>
      <td>2014.0</td>
      <td>21560.0</td>
      <td>0.000684</td>
    </tr>
    <tr>
      <th>7639</th>
      <td>19462.0</td>
      <td>2014.0</td>
      <td>21326.0</td>
      <td>0.000676</td>
    </tr>
    <tr>
      <th>12390</th>
      <td>20206.0</td>
      <td>2014.0</td>
      <td>20940.0</td>
      <td>0.000664</td>
    </tr>
    <tr>
      <th>17343</th>
      <td>15507.0</td>
      <td>2014.0</td>
      <td>18046.0</td>
      <td>0.000572</td>
    </tr>
    <tr>
      <th>10771</th>
      <td>17468.0</td>
      <td>2014.0</td>
      <td>17015.0</td>
      <td>0.000540</td>
    </tr>
    <tr>
      <th>6039</th>
      <td>18703.0</td>
      <td>2014.0</td>
      <td>16910.0</td>
      <td>0.000536</td>
    </tr>
    <tr>
      <th>12027</th>
      <td>19887.0</td>
      <td>2014.0</td>
      <td>16751.0</td>
      <td>0.000531</td>
    </tr>
    <tr>
      <th>8354</th>
      <td>17752.0</td>
      <td>2014.0</td>
      <td>15729.0</td>
      <td>0.000499</td>
    </tr>
    <tr>
      <th>11601</th>
      <td>21472.0</td>
      <td>2014.0</td>
      <td>15212.0</td>
      <td>0.000482</td>
    </tr>
    <tr>
      <th>6040</th>
      <td>14661.0</td>
      <td>2014.0</td>
      <td>13582.0</td>
      <td>0.000431</td>
    </tr>
    <tr>
      <th>7268</th>
      <td>19241.0</td>
      <td>2014.0</td>
      <td>13579.0</td>
      <td>0.000431</td>
    </tr>
    <tr>
      <th>13437</th>
      <td>15017.0</td>
      <td>2014.0</td>
      <td>12961.0</td>
      <td>0.000411</td>
    </tr>
    <tr>
      <th>23023</th>
      <td>17660.0</td>
      <td>2014.0</td>
      <td>12560.0</td>
      <td>0.000398</td>
    </tr>
    <tr>
      <th>14107</th>
      <td>19510.0</td>
      <td>2014.0</td>
      <td>10424.0</td>
      <td>0.000331</td>
    </tr>
    <tr>
      <th>11069</th>
      <td>17744.0</td>
      <td>2014.0</td>
      <td>10183.0</td>
      <td>0.000323</td>
    </tr>
    <tr>
      <th>20668</th>
      <td>18052.0</td>
      <td>2014.0</td>
      <td>10001.0</td>
      <td>0.000317</td>
    </tr>
    <tr>
      <th>7153</th>
      <td>16912.0</td>
      <td>2014.0</td>
      <td>9563.0</td>
      <td>0.000303</td>
    </tr>
    <tr>
      <th>9962</th>
      <td>17184.0</td>
      <td>2014.0</td>
      <td>8383.0</td>
      <td>0.000266</td>
    </tr>
    <tr>
      <th>22842</th>
      <td>16745.0</td>
      <td>2014.0</td>
      <td>6179.0</td>
      <td>0.000196</td>
    </tr>
    <tr>
      <th>12305</th>
      <td>15284.0</td>
      <td>2014.0</td>
      <td>5972.0</td>
      <td>0.000189</td>
    </tr>
    <tr>
      <th>6083</th>
      <td>16297.0</td>
      <td>2014.0</td>
      <td>5251.0</td>
      <td>0.000167</td>
    </tr>
    <tr>
      <th>11898</th>
      <td>20148.0</td>
      <td>2014.0</td>
      <td>2671.0</td>
      <td>0.000085</td>
    </tr>
    <tr>
      <th>7857</th>
      <td>16559.0</td>
      <td>2014.0</td>
      <td>2171.0</td>
      <td>0.000069</td>
    </tr>
    <tr>
      <th>6409</th>
      <td>20250.0</td>
      <td>2014.0</td>
      <td>2141.0</td>
      <td>0.000068</td>
    </tr>
  </tbody>
</table>
<p>6811 rows × 4 columns</p>
</div>




```python
utilization_df_2015
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>bikeid</th>
      <th>year</th>
      <th>duration</th>
      <th>utilization</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>931</th>
      <td>24502.0</td>
      <td>2015.0</td>
      <td>20260211.0</td>
      <td>0.642447</td>
    </tr>
    <tr>
      <th>5467</th>
      <td>15230.0</td>
      <td>2015.0</td>
      <td>6322749.0</td>
      <td>0.200493</td>
    </tr>
    <tr>
      <th>19873</th>
      <td>24557.0</td>
      <td>2015.0</td>
      <td>6252306.0</td>
      <td>0.198259</td>
    </tr>
    <tr>
      <th>2669</th>
      <td>18572.0</td>
      <td>2015.0</td>
      <td>5727004.0</td>
      <td>0.181602</td>
    </tr>
    <tr>
      <th>19586</th>
      <td>24635.0</td>
      <td>2015.0</td>
      <td>5490886.0</td>
      <td>0.174115</td>
    </tr>
    <tr>
      <th>17625</th>
      <td>19043.0</td>
      <td>2015.0</td>
      <td>4393885.0</td>
      <td>0.139329</td>
    </tr>
    <tr>
      <th>4539</th>
      <td>22022.0</td>
      <td>2015.0</td>
      <td>3899412.0</td>
      <td>0.123650</td>
    </tr>
    <tr>
      <th>2569</th>
      <td>21296.0</td>
      <td>2015.0</td>
      <td>3589978.0</td>
      <td>0.113837</td>
    </tr>
    <tr>
      <th>4505</th>
      <td>14736.0</td>
      <td>2015.0</td>
      <td>3568162.0</td>
      <td>0.113146</td>
    </tr>
    <tr>
      <th>4211</th>
      <td>19779.0</td>
      <td>2015.0</td>
      <td>3543822.0</td>
      <td>0.112374</td>
    </tr>
    <tr>
      <th>7562</th>
      <td>20381.0</td>
      <td>2015.0</td>
      <td>3496599.0</td>
      <td>0.110876</td>
    </tr>
    <tr>
      <th>12706</th>
      <td>16893.0</td>
      <td>2015.0</td>
      <td>3136686.0</td>
      <td>0.099464</td>
    </tr>
    <tr>
      <th>9502</th>
      <td>16765.0</td>
      <td>2015.0</td>
      <td>3101640.0</td>
      <td>0.098352</td>
    </tr>
    <tr>
      <th>3068</th>
      <td>21164.0</td>
      <td>2015.0</td>
      <td>3084684.0</td>
      <td>0.097815</td>
    </tr>
    <tr>
      <th>14453</th>
      <td>18017.0</td>
      <td>2015.0</td>
      <td>2901195.0</td>
      <td>0.091996</td>
    </tr>
    <tr>
      <th>3829</th>
      <td>19432.0</td>
      <td>2015.0</td>
      <td>2884962.0</td>
      <td>0.091482</td>
    </tr>
    <tr>
      <th>5720</th>
      <td>15620.0</td>
      <td>2015.0</td>
      <td>2797292.0</td>
      <td>0.088702</td>
    </tr>
    <tr>
      <th>2363</th>
      <td>22269.0</td>
      <td>2015.0</td>
      <td>2770760.0</td>
      <td>0.087860</td>
    </tr>
    <tr>
      <th>6031</th>
      <td>16981.0</td>
      <td>2015.0</td>
      <td>2672908.0</td>
      <td>0.084757</td>
    </tr>
    <tr>
      <th>13560</th>
      <td>21107.0</td>
      <td>2015.0</td>
      <td>2662465.0</td>
      <td>0.084426</td>
    </tr>
    <tr>
      <th>3714</th>
      <td>15198.0</td>
      <td>2015.0</td>
      <td>2646508.0</td>
      <td>0.083920</td>
    </tr>
    <tr>
      <th>5996</th>
      <td>16298.0</td>
      <td>2015.0</td>
      <td>2616550.0</td>
      <td>0.082970</td>
    </tr>
    <tr>
      <th>7006</th>
      <td>17517.0</td>
      <td>2015.0</td>
      <td>2611709.0</td>
      <td>0.082817</td>
    </tr>
    <tr>
      <th>3388</th>
      <td>19860.0</td>
      <td>2015.0</td>
      <td>2610771.0</td>
      <td>0.082787</td>
    </tr>
    <tr>
      <th>4061</th>
      <td>15223.0</td>
      <td>2015.0</td>
      <td>2522295.0</td>
      <td>0.079981</td>
    </tr>
    <tr>
      <th>21843</th>
      <td>19193.0</td>
      <td>2015.0</td>
      <td>2448514.0</td>
      <td>0.077642</td>
    </tr>
    <tr>
      <th>5219</th>
      <td>20621.0</td>
      <td>2015.0</td>
      <td>2440020.0</td>
      <td>0.077373</td>
    </tr>
    <tr>
      <th>4159</th>
      <td>17790.0</td>
      <td>2015.0</td>
      <td>2320758.0</td>
      <td>0.073591</td>
    </tr>
    <tr>
      <th>16128</th>
      <td>18819.0</td>
      <td>2015.0</td>
      <td>2318259.0</td>
      <td>0.073512</td>
    </tr>
    <tr>
      <th>2959</th>
      <td>20721.0</td>
      <td>2015.0</td>
      <td>2305286.0</td>
      <td>0.073100</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1283</th>
      <td>23243.0</td>
      <td>2015.0</td>
      <td>2456.0</td>
      <td>0.000078</td>
    </tr>
    <tr>
      <th>20152</th>
      <td>23352.0</td>
      <td>2015.0</td>
      <td>2420.0</td>
      <td>0.000077</td>
    </tr>
    <tr>
      <th>17882</th>
      <td>22573.0</td>
      <td>2015.0</td>
      <td>2360.0</td>
      <td>0.000075</td>
    </tr>
    <tr>
      <th>21571</th>
      <td>24087.0</td>
      <td>2015.0</td>
      <td>2120.0</td>
      <td>0.000067</td>
    </tr>
    <tr>
      <th>1637</th>
      <td>22839.0</td>
      <td>2015.0</td>
      <td>2050.0</td>
      <td>0.000065</td>
    </tr>
    <tr>
      <th>22862</th>
      <td>24070.0</td>
      <td>2015.0</td>
      <td>2045.0</td>
      <td>0.000065</td>
    </tr>
    <tr>
      <th>3744</th>
      <td>21123.0</td>
      <td>2015.0</td>
      <td>1925.0</td>
      <td>0.000061</td>
    </tr>
    <tr>
      <th>2763</th>
      <td>19848.0</td>
      <td>2015.0</td>
      <td>1787.0</td>
      <td>0.000057</td>
    </tr>
    <tr>
      <th>1474</th>
      <td>24356.0</td>
      <td>2015.0</td>
      <td>1722.0</td>
      <td>0.000055</td>
    </tr>
    <tr>
      <th>1124</th>
      <td>23225.0</td>
      <td>2015.0</td>
      <td>1693.0</td>
      <td>0.000054</td>
    </tr>
    <tr>
      <th>4116</th>
      <td>23942.0</td>
      <td>2015.0</td>
      <td>1668.0</td>
      <td>0.000053</td>
    </tr>
    <tr>
      <th>5709</th>
      <td>14871.0</td>
      <td>2015.0</td>
      <td>1423.0</td>
      <td>0.000045</td>
    </tr>
    <tr>
      <th>2451</th>
      <td>21931.0</td>
      <td>2015.0</td>
      <td>1410.0</td>
      <td>0.000045</td>
    </tr>
    <tr>
      <th>6639</th>
      <td>21948.0</td>
      <td>2015.0</td>
      <td>1388.0</td>
      <td>0.000044</td>
    </tr>
    <tr>
      <th>1113</th>
      <td>24169.0</td>
      <td>2015.0</td>
      <td>1271.0</td>
      <td>0.000040</td>
    </tr>
    <tr>
      <th>2114</th>
      <td>22388.0</td>
      <td>2015.0</td>
      <td>1257.0</td>
      <td>0.000040</td>
    </tr>
    <tr>
      <th>2215</th>
      <td>22574.0</td>
      <td>2015.0</td>
      <td>1257.0</td>
      <td>0.000040</td>
    </tr>
    <tr>
      <th>1138</th>
      <td>23661.0</td>
      <td>2015.0</td>
      <td>1192.0</td>
      <td>0.000038</td>
    </tr>
    <tr>
      <th>2452</th>
      <td>21930.0</td>
      <td>2015.0</td>
      <td>955.0</td>
      <td>0.000030</td>
    </tr>
    <tr>
      <th>23072</th>
      <td>21758.0</td>
      <td>2015.0</td>
      <td>851.0</td>
      <td>0.000027</td>
    </tr>
    <tr>
      <th>7571</th>
      <td>21945.0</td>
      <td>2015.0</td>
      <td>794.0</td>
      <td>0.000025</td>
    </tr>
    <tr>
      <th>2373</th>
      <td>21942.0</td>
      <td>2015.0</td>
      <td>707.0</td>
      <td>0.000022</td>
    </tr>
    <tr>
      <th>2460</th>
      <td>24152.0</td>
      <td>2015.0</td>
      <td>666.0</td>
      <td>0.000021</td>
    </tr>
    <tr>
      <th>1066</th>
      <td>24568.0</td>
      <td>2015.0</td>
      <td>623.0</td>
      <td>0.000020</td>
    </tr>
    <tr>
      <th>19004</th>
      <td>24465.0</td>
      <td>2015.0</td>
      <td>616.0</td>
      <td>0.000020</td>
    </tr>
    <tr>
      <th>2341</th>
      <td>21943.0</td>
      <td>2015.0</td>
      <td>494.0</td>
      <td>0.000016</td>
    </tr>
    <tr>
      <th>1045</th>
      <td>24475.0</td>
      <td>2015.0</td>
      <td>331.0</td>
      <td>0.000010</td>
    </tr>
    <tr>
      <th>9611</th>
      <td>21947.0</td>
      <td>2015.0</td>
      <td>206.0</td>
      <td>0.000007</td>
    </tr>
    <tr>
      <th>1112</th>
      <td>17206.0</td>
      <td>2015.0</td>
      <td>182.0</td>
      <td>0.000006</td>
    </tr>
    <tr>
      <th>968</th>
      <td>24524.0</td>
      <td>2015.0</td>
      <td>78.0</td>
      <td>0.000002</td>
    </tr>
  </tbody>
</table>
<p>8519 rows × 4 columns</p>
</div>




```python
utilization_df_2016
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>bikeid</th>
      <th>year</th>
      <th>duration</th>
      <th>utilization</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>20171</th>
      <td>24519.0</td>
      <td>2016.0</td>
      <td>16720534.0</td>
      <td>0.528756</td>
    </tr>
    <tr>
      <th>15798</th>
      <td>24711.0</td>
      <td>2016.0</td>
      <td>5303956.0</td>
      <td>0.167728</td>
    </tr>
    <tr>
      <th>23214</th>
      <td>24664.0</td>
      <td>2016.0</td>
      <td>2730691.0</td>
      <td>0.086353</td>
    </tr>
    <tr>
      <th>21364</th>
      <td>24720.0</td>
      <td>2016.0</td>
      <td>2547839.0</td>
      <td>0.080571</td>
    </tr>
    <tr>
      <th>831</th>
      <td>24555.0</td>
      <td>2016.0</td>
      <td>2473179.0</td>
      <td>0.078210</td>
    </tr>
    <tr>
      <th>765</th>
      <td>24515.0</td>
      <td>2016.0</td>
      <td>2246669.0</td>
      <td>0.071047</td>
    </tr>
    <tr>
      <th>9700</th>
      <td>24629.0</td>
      <td>2016.0</td>
      <td>2087880.0</td>
      <td>0.066025</td>
    </tr>
    <tr>
      <th>21810</th>
      <td>24522.0</td>
      <td>2016.0</td>
      <td>2013856.0</td>
      <td>0.063684</td>
    </tr>
    <tr>
      <th>9313</th>
      <td>24556.0</td>
      <td>2016.0</td>
      <td>1603180.0</td>
      <td>0.050698</td>
    </tr>
    <tr>
      <th>851</th>
      <td>24441.0</td>
      <td>2016.0</td>
      <td>1444368.0</td>
      <td>0.045675</td>
    </tr>
    <tr>
      <th>718</th>
      <td>26192.0</td>
      <td>2016.0</td>
      <td>1417870.0</td>
      <td>0.044838</td>
    </tr>
    <tr>
      <th>906</th>
      <td>24429.0</td>
      <td>2016.0</td>
      <td>1235746.0</td>
      <td>0.039078</td>
    </tr>
    <tr>
      <th>720</th>
      <td>26151.0</td>
      <td>2016.0</td>
      <td>1229132.0</td>
      <td>0.038869</td>
    </tr>
    <tr>
      <th>7154</th>
      <td>24650.0</td>
      <td>2016.0</td>
      <td>1072161.0</td>
      <td>0.033905</td>
    </tr>
    <tr>
      <th>3617</th>
      <td>24433.0</td>
      <td>2016.0</td>
      <td>982043.0</td>
      <td>0.031055</td>
    </tr>
    <tr>
      <th>896</th>
      <td>24563.0</td>
      <td>2016.0</td>
      <td>893081.0</td>
      <td>0.028242</td>
    </tr>
    <tr>
      <th>691</th>
      <td>26182.0</td>
      <td>2016.0</td>
      <td>852974.0</td>
      <td>0.026974</td>
    </tr>
    <tr>
      <th>15953</th>
      <td>24638.0</td>
      <td>2016.0</td>
      <td>817716.0</td>
      <td>0.025859</td>
    </tr>
    <tr>
      <th>858</th>
      <td>24701.0</td>
      <td>2016.0</td>
      <td>770804.0</td>
      <td>0.024375</td>
    </tr>
    <tr>
      <th>795</th>
      <td>24572.0</td>
      <td>2016.0</td>
      <td>765965.0</td>
      <td>0.024222</td>
    </tr>
    <tr>
      <th>21747</th>
      <td>24477.0</td>
      <td>2016.0</td>
      <td>754470.0</td>
      <td>0.023859</td>
    </tr>
    <tr>
      <th>8382</th>
      <td>24616.0</td>
      <td>2016.0</td>
      <td>751203.0</td>
      <td>0.023755</td>
    </tr>
    <tr>
      <th>846</th>
      <td>24456.0</td>
      <td>2016.0</td>
      <td>737458.0</td>
      <td>0.023321</td>
    </tr>
    <tr>
      <th>908</th>
      <td>24573.0</td>
      <td>2016.0</td>
      <td>717742.0</td>
      <td>0.022697</td>
    </tr>
    <tr>
      <th>910</th>
      <td>24689.0</td>
      <td>2016.0</td>
      <td>713610.0</td>
      <td>0.022567</td>
    </tr>
    <tr>
      <th>870</th>
      <td>24599.0</td>
      <td>2016.0</td>
      <td>705876.0</td>
      <td>0.022322</td>
    </tr>
    <tr>
      <th>912</th>
      <td>24615.0</td>
      <td>2016.0</td>
      <td>702587.0</td>
      <td>0.022218</td>
    </tr>
    <tr>
      <th>879</th>
      <td>24558.0</td>
      <td>2016.0</td>
      <td>701169.0</td>
      <td>0.022173</td>
    </tr>
    <tr>
      <th>14131</th>
      <td>24709.0</td>
      <td>2016.0</td>
      <td>694340.0</td>
      <td>0.021957</td>
    </tr>
    <tr>
      <th>827</th>
      <td>24670.0</td>
      <td>2016.0</td>
      <td>690771.0</td>
      <td>0.021844</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>861</th>
      <td>24424.0</td>
      <td>2016.0</td>
      <td>3204.0</td>
      <td>0.000101</td>
    </tr>
    <tr>
      <th>726</th>
      <td>20972.0</td>
      <td>2016.0</td>
      <td>2745.0</td>
      <td>0.000087</td>
    </tr>
    <tr>
      <th>3168</th>
      <td>19276.0</td>
      <td>2016.0</td>
      <td>2715.0</td>
      <td>0.000086</td>
    </tr>
    <tr>
      <th>723</th>
      <td>21006.0</td>
      <td>2016.0</td>
      <td>2485.0</td>
      <td>0.000079</td>
    </tr>
    <tr>
      <th>770</th>
      <td>22537.0</td>
      <td>2016.0</td>
      <td>2376.0</td>
      <td>0.000075</td>
    </tr>
    <tr>
      <th>3093</th>
      <td>14705.0</td>
      <td>2016.0</td>
      <td>2372.0</td>
      <td>0.000075</td>
    </tr>
    <tr>
      <th>736</th>
      <td>19420.0</td>
      <td>2016.0</td>
      <td>1993.0</td>
      <td>0.000063</td>
    </tr>
    <tr>
      <th>621</th>
      <td>23234.0</td>
      <td>2016.0</td>
      <td>1946.0</td>
      <td>0.000062</td>
    </tr>
    <tr>
      <th>2731</th>
      <td>18812.0</td>
      <td>2016.0</td>
      <td>1938.0</td>
      <td>0.000061</td>
    </tr>
    <tr>
      <th>10572</th>
      <td>22608.0</td>
      <td>2016.0</td>
      <td>1907.0</td>
      <td>0.000060</td>
    </tr>
    <tr>
      <th>18923</th>
      <td>18605.0</td>
      <td>2016.0</td>
      <td>1840.0</td>
      <td>0.000058</td>
    </tr>
    <tr>
      <th>738</th>
      <td>21413.0</td>
      <td>2016.0</td>
      <td>1807.0</td>
      <td>0.000057</td>
    </tr>
    <tr>
      <th>7366</th>
      <td>18384.0</td>
      <td>2016.0</td>
      <td>1735.0</td>
      <td>0.000055</td>
    </tr>
    <tr>
      <th>622</th>
      <td>22292.0</td>
      <td>2016.0</td>
      <td>1676.0</td>
      <td>0.000053</td>
    </tr>
    <tr>
      <th>735</th>
      <td>22451.0</td>
      <td>2016.0</td>
      <td>1585.0</td>
      <td>0.000050</td>
    </tr>
    <tr>
      <th>9012</th>
      <td>21707.0</td>
      <td>2016.0</td>
      <td>1499.0</td>
      <td>0.000047</td>
    </tr>
    <tr>
      <th>730</th>
      <td>21557.0</td>
      <td>2016.0</td>
      <td>1336.0</td>
      <td>0.000042</td>
    </tr>
    <tr>
      <th>603</th>
      <td>18648.0</td>
      <td>2016.0</td>
      <td>1232.0</td>
      <td>0.000039</td>
    </tr>
    <tr>
      <th>611</th>
      <td>16021.0</td>
      <td>2016.0</td>
      <td>1028.0</td>
      <td>0.000033</td>
    </tr>
    <tr>
      <th>767</th>
      <td>22900.0</td>
      <td>2016.0</td>
      <td>998.0</td>
      <td>0.000032</td>
    </tr>
    <tr>
      <th>729</th>
      <td>25308.0</td>
      <td>2016.0</td>
      <td>964.0</td>
      <td>0.000030</td>
    </tr>
    <tr>
      <th>13801</th>
      <td>14970.0</td>
      <td>2016.0</td>
      <td>910.0</td>
      <td>0.000029</td>
    </tr>
    <tr>
      <th>19676</th>
      <td>23750.0</td>
      <td>2016.0</td>
      <td>818.0</td>
      <td>0.000026</td>
    </tr>
    <tr>
      <th>623</th>
      <td>18402.0</td>
      <td>2016.0</td>
      <td>804.0</td>
      <td>0.000025</td>
    </tr>
    <tr>
      <th>785</th>
      <td>17941.0</td>
      <td>2016.0</td>
      <td>471.0</td>
      <td>0.000015</td>
    </tr>
    <tr>
      <th>1052</th>
      <td>22350.0</td>
      <td>2016.0</td>
      <td>456.0</td>
      <td>0.000014</td>
    </tr>
    <tr>
      <th>728</th>
      <td>14872.0</td>
      <td>2016.0</td>
      <td>441.0</td>
      <td>0.000014</td>
    </tr>
    <tr>
      <th>9351</th>
      <td>17904.0</td>
      <td>2016.0</td>
      <td>289.0</td>
      <td>0.000009</td>
    </tr>
    <tr>
      <th>7394</th>
      <td>18062.0</td>
      <td>2016.0</td>
      <td>203.0</td>
      <td>0.000006</td>
    </tr>
    <tr>
      <th>776</th>
      <td>14632.0</td>
      <td>2016.0</td>
      <td>182.0</td>
      <td>0.000006</td>
    </tr>
  </tbody>
</table>
<p>566 rows × 4 columns</p>
</div>




```python
utilization_df_2017
```




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>bikeid</th>
      <th>year</th>
      <th>duration</th>
      <th>utilization</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>11107</th>
      <td>29242.0</td>
      <td>2017.0</td>
      <td>2533525.0</td>
      <td>0.080338</td>
    </tr>
    <tr>
      <th>2418</th>
      <td>29542.0</td>
      <td>2017.0</td>
      <td>2326312.0</td>
      <td>0.073767</td>
    </tr>
    <tr>
      <th>5227</th>
      <td>29522.0</td>
      <td>2017.0</td>
      <td>1957676.0</td>
      <td>0.062077</td>
    </tr>
    <tr>
      <th>9971</th>
      <td>29646.0</td>
      <td>2017.0</td>
      <td>1901246.0</td>
      <td>0.060288</td>
    </tr>
    <tr>
      <th>11126</th>
      <td>26273.0</td>
      <td>2017.0</td>
      <td>1775977.0</td>
      <td>0.056316</td>
    </tr>
    <tr>
      <th>304</th>
      <td>29434.0</td>
      <td>2017.0</td>
      <td>1649046.0</td>
      <td>0.052291</td>
    </tr>
    <tr>
      <th>1335</th>
      <td>29246.0</td>
      <td>2017.0</td>
      <td>1614171.0</td>
      <td>0.051185</td>
    </tr>
    <tr>
      <th>3518</th>
      <td>26168.0</td>
      <td>2017.0</td>
      <td>1547854.0</td>
      <td>0.049082</td>
    </tr>
    <tr>
      <th>2629</th>
      <td>29623.0</td>
      <td>2017.0</td>
      <td>1410071.0</td>
      <td>0.044713</td>
    </tr>
    <tr>
      <th>658</th>
      <td>29642.0</td>
      <td>2017.0</td>
      <td>1300974.0</td>
      <td>0.041254</td>
    </tr>
    <tr>
      <th>23538</th>
      <td>29488.0</td>
      <td>2017.0</td>
      <td>1267982.0</td>
      <td>0.040207</td>
    </tr>
    <tr>
      <th>321</th>
      <td>29472.0</td>
      <td>2017.0</td>
      <td>1230179.0</td>
      <td>0.039009</td>
    </tr>
    <tr>
      <th>448</th>
      <td>26172.0</td>
      <td>2017.0</td>
      <td>1188690.0</td>
      <td>0.037693</td>
    </tr>
    <tr>
      <th>20676</th>
      <td>26266.0</td>
      <td>2017.0</td>
      <td>1114573.0</td>
      <td>0.035343</td>
    </tr>
    <tr>
      <th>12483</th>
      <td>29299.0</td>
      <td>2017.0</td>
      <td>917293.0</td>
      <td>0.029087</td>
    </tr>
    <tr>
      <th>2810</th>
      <td>26224.0</td>
      <td>2017.0</td>
      <td>900886.0</td>
      <td>0.028567</td>
    </tr>
    <tr>
      <th>315</th>
      <td>29537.0</td>
      <td>2017.0</td>
      <td>892500.0</td>
      <td>0.028301</td>
    </tr>
    <tr>
      <th>425</th>
      <td>24420.0</td>
      <td>2017.0</td>
      <td>887809.0</td>
      <td>0.028152</td>
    </tr>
    <tr>
      <th>256</th>
      <td>29596.0</td>
      <td>2017.0</td>
      <td>863243.0</td>
      <td>0.027373</td>
    </tr>
    <tr>
      <th>511</th>
      <td>24516.0</td>
      <td>2017.0</td>
      <td>856022.0</td>
      <td>0.027144</td>
    </tr>
    <tr>
      <th>402</th>
      <td>26299.0</td>
      <td>2017.0</td>
      <td>731392.0</td>
      <td>0.023192</td>
    </tr>
    <tr>
      <th>411</th>
      <td>26310.0</td>
      <td>2017.0</td>
      <td>715033.0</td>
      <td>0.022674</td>
    </tr>
    <tr>
      <th>4799</th>
      <td>29600.0</td>
      <td>2017.0</td>
      <td>704869.0</td>
      <td>0.022351</td>
    </tr>
    <tr>
      <th>15</th>
      <td>31547.0</td>
      <td>2017.0</td>
      <td>675064.0</td>
      <td>0.021406</td>
    </tr>
    <tr>
      <th>507</th>
      <td>26269.0</td>
      <td>2017.0</td>
      <td>671988.0</td>
      <td>0.021309</td>
    </tr>
    <tr>
      <th>14287</th>
      <td>29580.0</td>
      <td>2017.0</td>
      <td>636469.0</td>
      <td>0.020182</td>
    </tr>
    <tr>
      <th>503</th>
      <td>26151.0</td>
      <td>2017.0</td>
      <td>626968.0</td>
      <td>0.019881</td>
    </tr>
    <tr>
      <th>427</th>
      <td>26284.0</td>
      <td>2017.0</td>
      <td>623731.0</td>
      <td>0.019778</td>
    </tr>
    <tr>
      <th>21995</th>
      <td>26229.0</td>
      <td>2017.0</td>
      <td>611260.0</td>
      <td>0.019383</td>
    </tr>
    <tr>
      <th>1597</th>
      <td>26170.0</td>
      <td>2017.0</td>
      <td>601447.0</td>
      <td>0.019072</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>186</th>
      <td>21412.0</td>
      <td>2017.0</td>
      <td>2008.0</td>
      <td>0.000064</td>
    </tr>
    <tr>
      <th>1654</th>
      <td>17619.0</td>
      <td>2017.0</td>
      <td>1857.0</td>
      <td>0.000059</td>
    </tr>
    <tr>
      <th>8375</th>
      <td>25150.0</td>
      <td>2017.0</td>
      <td>1769.0</td>
      <td>0.000056</td>
    </tr>
    <tr>
      <th>202</th>
      <td>27626.0</td>
      <td>2017.0</td>
      <td>1688.0</td>
      <td>0.000054</td>
    </tr>
    <tr>
      <th>6628</th>
      <td>31602.0</td>
      <td>2017.0</td>
      <td>1667.0</td>
      <td>0.000053</td>
    </tr>
    <tr>
      <th>198</th>
      <td>27574.0</td>
      <td>2017.0</td>
      <td>1403.0</td>
      <td>0.000044</td>
    </tr>
    <tr>
      <th>158</th>
      <td>18541.0</td>
      <td>2017.0</td>
      <td>1334.0</td>
      <td>0.000042</td>
    </tr>
    <tr>
      <th>12</th>
      <td>31129.0</td>
      <td>2017.0</td>
      <td>1235.0</td>
      <td>0.000039</td>
    </tr>
    <tr>
      <th>146</th>
      <td>18512.0</td>
      <td>2017.0</td>
      <td>1151.0</td>
      <td>0.000036</td>
    </tr>
    <tr>
      <th>10059</th>
      <td>25017.0</td>
      <td>2017.0</td>
      <td>1082.0</td>
      <td>0.000034</td>
    </tr>
    <tr>
      <th>141</th>
      <td>28982.0</td>
      <td>2017.0</td>
      <td>1067.0</td>
      <td>0.000034</td>
    </tr>
    <tr>
      <th>162</th>
      <td>14529.0</td>
      <td>2017.0</td>
      <td>1012.0</td>
      <td>0.000032</td>
    </tr>
    <tr>
      <th>7904</th>
      <td>24907.0</td>
      <td>2017.0</td>
      <td>906.0</td>
      <td>0.000029</td>
    </tr>
    <tr>
      <th>23217</th>
      <td>15271.0</td>
      <td>2017.0</td>
      <td>876.0</td>
      <td>0.000028</td>
    </tr>
    <tr>
      <th>3644</th>
      <td>20778.0</td>
      <td>2017.0</td>
      <td>848.0</td>
      <td>0.000027</td>
    </tr>
    <tr>
      <th>156</th>
      <td>27468.0</td>
      <td>2017.0</td>
      <td>843.0</td>
      <td>0.000027</td>
    </tr>
    <tr>
      <th>14352</th>
      <td>20867.0</td>
      <td>2017.0</td>
      <td>828.0</td>
      <td>0.000026</td>
    </tr>
    <tr>
      <th>6208</th>
      <td>15345.0</td>
      <td>2017.0</td>
      <td>789.0</td>
      <td>0.000025</td>
    </tr>
    <tr>
      <th>5836</th>
      <td>27499.0</td>
      <td>2017.0</td>
      <td>745.0</td>
      <td>0.000024</td>
    </tr>
    <tr>
      <th>4503</th>
      <td>21292.0</td>
      <td>2017.0</td>
      <td>722.0</td>
      <td>0.000023</td>
    </tr>
    <tr>
      <th>267</th>
      <td>17111.0</td>
      <td>2017.0</td>
      <td>696.0</td>
      <td>0.000022</td>
    </tr>
    <tr>
      <th>5089</th>
      <td>26146.0</td>
      <td>2017.0</td>
      <td>655.0</td>
      <td>0.000021</td>
    </tr>
    <tr>
      <th>20</th>
      <td>32346.0</td>
      <td>2017.0</td>
      <td>618.0</td>
      <td>0.000020</td>
    </tr>
    <tr>
      <th>15845</th>
      <td>28579.0</td>
      <td>2017.0</td>
      <td>545.0</td>
      <td>0.000017</td>
    </tr>
    <tr>
      <th>183</th>
      <td>20083.0</td>
      <td>2017.0</td>
      <td>535.0</td>
      <td>0.000017</td>
    </tr>
    <tr>
      <th>2864</th>
      <td>28804.0</td>
      <td>2017.0</td>
      <td>508.0</td>
      <td>0.000016</td>
    </tr>
    <tr>
      <th>15217</th>
      <td>19354.0</td>
      <td>2017.0</td>
      <td>346.0</td>
      <td>0.000011</td>
    </tr>
    <tr>
      <th>238</th>
      <td>18292.0</td>
      <td>2017.0</td>
      <td>263.0</td>
      <td>0.000008</td>
    </tr>
    <tr>
      <th>147</th>
      <td>18621.0</td>
      <td>2017.0</td>
      <td>214.0</td>
      <td>0.000007</td>
    </tr>
    <tr>
      <th>2</th>
      <td>19637.0</td>
      <td>2017.0</td>
      <td>167.0</td>
      <td>0.000005</td>
    </tr>
  </tbody>
</table>
<p>1201 rows × 4 columns</p>
</div>


