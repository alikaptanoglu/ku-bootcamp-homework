

```python
from bson.json_util import dumps
from dateutil import parser
from datetime import datetime, timedelta

import pytz
import pymongo
import os
import csv
import glob
import math




# connect to Mongo client using PyMongo
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)
# create new client tableau
db = client.tableau
# drop old table
db.listings.drop()
bikedata = db.listings

# read all csv files into an array
bikedata_files = glob.glob('*.csv') 

# set utc time, format
ny_timezone = "America/New_York"

```


```python
def get_utc_datetime(datetime_str):
    # parse naive datetime
    my_timestamp = parser.parse(datetime_str)
    # set timezone to America/New York
    ny_timestamp = my_timestamp.astimezone(pytz.timezone(ny_timezone))
    # convert America/New York timezone to UTC
    utc_timestamp = ny_timestamp.replace(tzinfo=pytz.utc)
    return utc_timestamp


# this is to stop mongodb from injecting the local hours instead of NY hours 
def get_hours(datetime_obj):
    new_timestamp = datetime_obj + timedelta(hours=-5)
    return new_timestamp.hour
    
    
# calculate distance in miles between sets of coordinates
# 3959 radius of circle in miles
# 6371 radius in kilometers
# 3959 * 5280 radius in feet
# 6371 * 1000 radius in meters
def get_miles(origin, destination):
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 3959  # miles

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return radius * c


```


```python
# data types for all years
data_types = ["int", "datetime", "datetime", "int", "string", "float", "float", "int", "string", "float", "float", "int", "string", "int", "string"]

# read the first file to build db column names
first_file = open(bikedata_files[0])
reader = csv.reader(first_file)

# get headers of the first file
headers = next(reader, None)

# build data types for the columns, column names
new_headers = []
for each in headers:
    # replace space with _ to build valid db column names
    each = each.replace(' ', '')
    each = each.replace('()', '')
    new_headers.append(each)
    
# set the year
year = 0
                       
# read each file to build db row
for each_file_name in bikedata_files:
    each_file = open(each_file_name)
    # start reading
    csvReader = csv.reader(each_file, delimiter=',')
    # get headers
    headers = next(csvReader, None)

    for row in csvReader:
        # build each row by setting one value for each header 
        each_trip = {}
        # restart header for each trip
        i = 0
        for each_header in new_headers:
            
            # set key value for each trip based on data type
            if data_types[i] == "int":
                try:
                    updated_value = int(row[i]) 
                except:
                    updated_value = 0
                    
            elif data_types[i] == "float": 
                try:
                    updated_value = float(row[i]) 
                except:
                    updated_value = 0.0

            elif data_types[i] == "datetime": 
                try:
                    updated_value = get_utc_datetime(row[i]) 
                except:
                    updated_value = calendar.timegm(datetime(year, 1, 1).utctimetuple())

            else:
                updated_value = row[i]
                
            each_trip[each_header] = updated_value
            
            # next header
            i = i + 1
        
        # year
        each_trip["year"] = each_trip["starttime"].year

        # hour in 24-hour format
        each_trip["starthour"] = get_hours(each_trip["starttime"])
        each_trip["stophour"] = get_hours(each_trip["stoptime"])
        

        # seasons in USA
        # assuming using the start_month to calculate this field
        months = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2]
        seasons = ["Spring", "Spring", "Spring", "Summer", "Summer", "Summer", "Fall", "Fall", "Fall", "Winter", "Winter", "Winter"]
        start_month = each_trip["starttime"].month
        for each in range(0, len(months)):
            if start_month == months[each]:
                each_trip["season"] = seasons[each]
                break

        # replace number with value of gender 
        gender_lookup = {"0":"unknown", "1":"male", "2":"female"}
        each_trip["gender"] = ' '.join(str(gender_lookup.get(x, x)) for x in each_trip["gender"].split())

        # set the year to the new year
        if each_trip["year"] != year:
            year = each_trip["year"]

        # distance between the start station to end station
        each_trip["distance"] = get_miles((each_trip["startstationlatitude"], each_trip["startstationlongitude"]), (each_trip["endstationlatitude"], each_trip["endstationlongitude"]))
        
        # insert a new row to the tableau db
        bikedata.insert_one(each_trip)


```


```python


```
