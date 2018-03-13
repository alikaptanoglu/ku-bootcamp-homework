

```python
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from warnings import filterwarnings
import pymysql
filterwarnings('ignore', category=pymysql.Warning)

import pandas as pd
import numpy as np
import csv
import os
import io



```


```python
#read measurements data from csv
measurement_file_name = os.path.join('resources', 'hawaii_measurements.csv')
measurement_file = pd.read_csv(measurement_file_name)
measurement_df = pd.DataFrame(measurement_file)

measurement_df[['station','date','prcp','tobs']] = measurement_df['station,date,prcp,tobs'].str.split(',', expand=True)
measurement_df = measurement_df.drop('station,date,prcp,tobs', axis=1)
measurement_df['prcp'] = measurement_df['prcp'].apply(lambda x: 0 if x == '' else x)
measurement_df['prcp'].fillna(0)

#save clean data
measurement_df.to_csv(os.path.join('resources','clean_hawaii_measurements.csv'))

measurement_df.head()

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
      <th>station</th>
      <th>date</th>
      <th>prcp</th>
      <th>tobs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>USC00519397</td>
      <td>2010-01-01</td>
      <td>0.08</td>
      <td>65</td>
    </tr>
    <tr>
      <th>1</th>
      <td>USC00519397</td>
      <td>2010-01-02</td>
      <td>0</td>
      <td>63</td>
    </tr>
    <tr>
      <th>2</th>
      <td>USC00519397</td>
      <td>2010-01-03</td>
      <td>0</td>
      <td>74</td>
    </tr>
    <tr>
      <th>3</th>
      <td>USC00519397</td>
      <td>2010-01-04</td>
      <td>0</td>
      <td>76</td>
    </tr>
    <tr>
      <th>4</th>
      <td>USC00519397</td>
      <td>2010-01-06</td>
      <td>0</td>
      <td>73</td>
    </tr>
  </tbody>
</table>
</div>




```python
#read stations data from csv
station_file_name = os.path.join("resources", "hawaii_stations.csv")
station_file = pd.read_csv(station_file_name, encoding='utf-8')

#create list of stations
new_rows = []

#rebuild list of stations as I have not figured out how to do it in a simpler way
#at least it works...
with open(station_file_name, 'r', newline='') as file:
    reader = csv.reader(file)
    #skip the header
    next(reader, None) 
    for row in reader:
        #find and replace the double quotes and comma in the string with *
        start_index = row[0].find('"')
        end_index = row[0][start_index + 1:].find('"') 
        orig = row[0][start_index:start_index + end_index + 2]
        upd = orig.replace('"', '')
        upd = upd.replace(',', '*')
        #split the new string 
        new_row = row[0].replace(orig,upd).split(',')
        #add new record to the list
        new_rows.append(new_row)
        
station_df = pd.DataFrame.from_records(new_rows,columns=['station','name','latitude','longitude','elevation'])
#restore the special character used
station_df['name'] = station_df['name'].str.replace('*',',') 

#save clean data
station_df.to_csv(os.path.join('resources','clean_hawaii_stations.csv'))

station_df

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
      <th>station</th>
      <th>name</th>
      <th>latitude</th>
      <th>longitude</th>
      <th>elevation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>USC00519397</td>
      <td>WAIKIKI 717.2, HI US</td>
      <td>21.2716</td>
      <td>-157.8168</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>USC00513117</td>
      <td>KANEOHE 838.1, HI US</td>
      <td>21.4234</td>
      <td>-157.8015</td>
      <td>14.6</td>
    </tr>
    <tr>
      <th>2</th>
      <td>USC00514830</td>
      <td>KUALOA RANCH HEADQUARTERS 886.9, HI US</td>
      <td>21.5213</td>
      <td>-157.8374</td>
      <td>7</td>
    </tr>
    <tr>
      <th>3</th>
      <td>USC00517948</td>
      <td>PEARL CITY, HI US</td>
      <td>21.3934</td>
      <td>-157.9751</td>
      <td>11.9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>USC00518838</td>
      <td>UPPER WAHIAWA 874.3, HI US</td>
      <td>21.4992</td>
      <td>-158.0111</td>
      <td>306.6</td>
    </tr>
    <tr>
      <th>5</th>
      <td>USC00519523</td>
      <td>WAIMANALO EXPERIMENTAL FARM, HI US</td>
      <td>21.33556</td>
      <td>-157.71139</td>
      <td>19.5</td>
    </tr>
    <tr>
      <th>6</th>
      <td>USC00519281</td>
      <td>WAIHEE 837.5, HI US</td>
      <td>21.45167</td>
      <td>-157.84889</td>
      <td>32.9</td>
    </tr>
    <tr>
      <th>7</th>
      <td>USC00511918</td>
      <td>HONOLULU OBSERVATORY 702.2, HI US</td>
      <td>21.3152</td>
      <td>-157.9992</td>
      <td>0.9</td>
    </tr>
    <tr>
      <th>8</th>
      <td>USC00516128</td>
      <td>MANOA LYON ARBO 785.2, HI US</td>
      <td>21.3331</td>
      <td>-157.8025</td>
      <td>152.4</td>
    </tr>
  </tbody>
</table>
</div>




```python
#create ORM class for each table
class Measurement(Base):
  __tablename__ = "measurements"
  station = Column(String(11),primary_key=True)
  date = Column(String(10),primary_key=True)
  prcp = Column(Float)
  tobs = Column(Float)

class Station(Base):
  __tablename__ = "stations"
  station = Column(String(11), primary_key=True)
  name = Column(String(255))
  latitude = Column(Float)
  longitude = Column(Float)
  elevation = Column(Integer)

```


```python
#create new database and connection
engine = create_engine("sqlite:///databases/hawaii.sqlite", echo=False)

#delete tables if exist
engine.execute("DROP TABLE IF EXISTS measurements")
engine.execute("DROP TABLE IF EXISTS stations")

Base.metadata.create_all(engine)

```


```python
#start new session
session = Session(bind=engine)

#populate table measurements with data
for each in measurement_df.iterrows():
    row = Measurement(station=each[1][0],date=each[1][1],prcp=each[1][2],tobs=each[1][3])
    session.add(row)
session.commit()
```


```python
#start new session
session = Session(bind=engine)

#populate table stations with data
for each in station_df.iterrows():
    row = Station(station=each[1][0],name=each[1][1],latitude=each[1][2],longitude=each[1][3],elevation=each[1][4])
    session.add(row)
session.commit()
```


```python
#verify table measurements
sql_query = """
    SELECT *
    FROM measurements
    ;
"""
pd.read_sql_query(sql_query, engine)

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
      <th>station</th>
      <th>date</th>
      <th>prcp</th>
      <th>tobs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>USC00519397</td>
      <td>2010-01-01</td>
      <td>0.08</td>
      <td>65.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>USC00519397</td>
      <td>2010-01-02</td>
      <td>0.00</td>
      <td>63.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>USC00519397</td>
      <td>2010-01-03</td>
      <td>0.00</td>
      <td>74.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>USC00519397</td>
      <td>2010-01-04</td>
      <td>0.00</td>
      <td>76.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>USC00519397</td>
      <td>2010-01-06</td>
      <td>0.00</td>
      <td>73.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>USC00519397</td>
      <td>2010-01-07</td>
      <td>0.06</td>
      <td>70.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>USC00519397</td>
      <td>2010-01-08</td>
      <td>0.00</td>
      <td>64.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>USC00519397</td>
      <td>2010-01-09</td>
      <td>0.00</td>
      <td>68.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>USC00519397</td>
      <td>2010-01-10</td>
      <td>0.00</td>
      <td>73.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>USC00519397</td>
      <td>2010-01-11</td>
      <td>0.01</td>
      <td>64.0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>USC00519397</td>
      <td>2010-01-12</td>
      <td>0.00</td>
      <td>61.0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>USC00519397</td>
      <td>2010-01-14</td>
      <td>0.00</td>
      <td>66.0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>USC00519397</td>
      <td>2010-01-15</td>
      <td>0.00</td>
      <td>65.0</td>
    </tr>
    <tr>
      <th>13</th>
      <td>USC00519397</td>
      <td>2010-01-16</td>
      <td>0.00</td>
      <td>68.0</td>
    </tr>
    <tr>
      <th>14</th>
      <td>USC00519397</td>
      <td>2010-01-17</td>
      <td>0.00</td>
      <td>64.0</td>
    </tr>
    <tr>
      <th>15</th>
      <td>USC00519397</td>
      <td>2010-01-18</td>
      <td>0.00</td>
      <td>72.0</td>
    </tr>
    <tr>
      <th>16</th>
      <td>USC00519397</td>
      <td>2010-01-19</td>
      <td>0.00</td>
      <td>66.0</td>
    </tr>
    <tr>
      <th>17</th>
      <td>USC00519397</td>
      <td>2010-01-20</td>
      <td>0.00</td>
      <td>66.0</td>
    </tr>
    <tr>
      <th>18</th>
      <td>USC00519397</td>
      <td>2010-01-21</td>
      <td>0.00</td>
      <td>69.0</td>
    </tr>
    <tr>
      <th>19</th>
      <td>USC00519397</td>
      <td>2010-01-22</td>
      <td>0.00</td>
      <td>67.0</td>
    </tr>
    <tr>
      <th>20</th>
      <td>USC00519397</td>
      <td>2010-01-23</td>
      <td>0.00</td>
      <td>67.0</td>
    </tr>
    <tr>
      <th>21</th>
      <td>USC00519397</td>
      <td>2010-01-24</td>
      <td>0.01</td>
      <td>71.0</td>
    </tr>
    <tr>
      <th>22</th>
      <td>USC00519397</td>
      <td>2010-01-25</td>
      <td>0.00</td>
      <td>67.0</td>
    </tr>
    <tr>
      <th>23</th>
      <td>USC00519397</td>
      <td>2010-01-26</td>
      <td>0.04</td>
      <td>76.0</td>
    </tr>
    <tr>
      <th>24</th>
      <td>USC00519397</td>
      <td>2010-01-27</td>
      <td>0.12</td>
      <td>68.0</td>
    </tr>
    <tr>
      <th>25</th>
      <td>USC00519397</td>
      <td>2010-01-28</td>
      <td>0.00</td>
      <td>72.0</td>
    </tr>
    <tr>
      <th>26</th>
      <td>USC00519397</td>
      <td>2010-01-30</td>
      <td>0.00</td>
      <td>70.0</td>
    </tr>
    <tr>
      <th>27</th>
      <td>USC00519397</td>
      <td>2010-01-31</td>
      <td>0.03</td>
      <td>67.0</td>
    </tr>
    <tr>
      <th>28</th>
      <td>USC00519397</td>
      <td>2010-02-01</td>
      <td>0.01</td>
      <td>66.0</td>
    </tr>
    <tr>
      <th>29</th>
      <td>USC00519397</td>
      <td>2010-02-03</td>
      <td>0.00</td>
      <td>67.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>19520</th>
      <td>USC00516128</td>
      <td>2017-07-24</td>
      <td>0.84</td>
      <td>77.0</td>
    </tr>
    <tr>
      <th>19521</th>
      <td>USC00516128</td>
      <td>2017-07-25</td>
      <td>0.30</td>
      <td>79.0</td>
    </tr>
    <tr>
      <th>19522</th>
      <td>USC00516128</td>
      <td>2017-07-26</td>
      <td>0.30</td>
      <td>73.0</td>
    </tr>
    <tr>
      <th>19523</th>
      <td>USC00516128</td>
      <td>2017-07-27</td>
      <td>0.00</td>
      <td>75.0</td>
    </tr>
    <tr>
      <th>19524</th>
      <td>USC00516128</td>
      <td>2017-07-28</td>
      <td>0.40</td>
      <td>73.0</td>
    </tr>
    <tr>
      <th>19525</th>
      <td>USC00516128</td>
      <td>2017-07-29</td>
      <td>0.30</td>
      <td>77.0</td>
    </tr>
    <tr>
      <th>19526</th>
      <td>USC00516128</td>
      <td>2017-07-30</td>
      <td>0.30</td>
      <td>79.0</td>
    </tr>
    <tr>
      <th>19527</th>
      <td>USC00516128</td>
      <td>2017-07-31</td>
      <td>0.00</td>
      <td>74.0</td>
    </tr>
    <tr>
      <th>19528</th>
      <td>USC00516128</td>
      <td>2017-08-01</td>
      <td>0.00</td>
      <td>72.0</td>
    </tr>
    <tr>
      <th>19529</th>
      <td>USC00516128</td>
      <td>2017-08-02</td>
      <td>0.25</td>
      <td>80.0</td>
    </tr>
    <tr>
      <th>19530</th>
      <td>USC00516128</td>
      <td>2017-08-03</td>
      <td>0.06</td>
      <td>76.0</td>
    </tr>
    <tr>
      <th>19531</th>
      <td>USC00516128</td>
      <td>2017-08-05</td>
      <td>0.00</td>
      <td>77.0</td>
    </tr>
    <tr>
      <th>19532</th>
      <td>USC00516128</td>
      <td>2017-08-06</td>
      <td>0.00</td>
      <td>79.0</td>
    </tr>
    <tr>
      <th>19533</th>
      <td>USC00516128</td>
      <td>2017-08-07</td>
      <td>0.05</td>
      <td>78.0</td>
    </tr>
    <tr>
      <th>19534</th>
      <td>USC00516128</td>
      <td>2017-08-08</td>
      <td>0.34</td>
      <td>74.0</td>
    </tr>
    <tr>
      <th>19535</th>
      <td>USC00516128</td>
      <td>2017-08-09</td>
      <td>0.15</td>
      <td>71.0</td>
    </tr>
    <tr>
      <th>19536</th>
      <td>USC00516128</td>
      <td>2017-08-10</td>
      <td>0.07</td>
      <td>75.0</td>
    </tr>
    <tr>
      <th>19537</th>
      <td>USC00516128</td>
      <td>2017-08-11</td>
      <td>0.00</td>
      <td>72.0</td>
    </tr>
    <tr>
      <th>19538</th>
      <td>USC00516128</td>
      <td>2017-08-12</td>
      <td>0.14</td>
      <td>74.0</td>
    </tr>
    <tr>
      <th>19539</th>
      <td>USC00516128</td>
      <td>2017-08-13</td>
      <td>0.00</td>
      <td>80.0</td>
    </tr>
    <tr>
      <th>19540</th>
      <td>USC00516128</td>
      <td>2017-08-14</td>
      <td>0.22</td>
      <td>79.0</td>
    </tr>
    <tr>
      <th>19541</th>
      <td>USC00516128</td>
      <td>2017-08-15</td>
      <td>0.42</td>
      <td>70.0</td>
    </tr>
    <tr>
      <th>19542</th>
      <td>USC00516128</td>
      <td>2017-08-16</td>
      <td>0.42</td>
      <td>71.0</td>
    </tr>
    <tr>
      <th>19543</th>
      <td>USC00516128</td>
      <td>2017-08-17</td>
      <td>0.13</td>
      <td>72.0</td>
    </tr>
    <tr>
      <th>19544</th>
      <td>USC00516128</td>
      <td>2017-08-18</td>
      <td>0.00</td>
      <td>76.0</td>
    </tr>
    <tr>
      <th>19545</th>
      <td>USC00516128</td>
      <td>2017-08-19</td>
      <td>0.09</td>
      <td>71.0</td>
    </tr>
    <tr>
      <th>19546</th>
      <td>USC00516128</td>
      <td>2017-08-20</td>
      <td>0.00</td>
      <td>78.0</td>
    </tr>
    <tr>
      <th>19547</th>
      <td>USC00516128</td>
      <td>2017-08-21</td>
      <td>0.56</td>
      <td>76.0</td>
    </tr>
    <tr>
      <th>19548</th>
      <td>USC00516128</td>
      <td>2017-08-22</td>
      <td>0.50</td>
      <td>76.0</td>
    </tr>
    <tr>
      <th>19549</th>
      <td>USC00516128</td>
      <td>2017-08-23</td>
      <td>0.45</td>
      <td>76.0</td>
    </tr>
  </tbody>
</table>
<p>19550 rows × 4 columns</p>
</div>




```python
#verify table measurements
sql_query = """
    SELECT *
    FROM stations
    ;
"""
pd.read_sql_query(sql_query, engine)

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
      <th>station</th>
      <th>name</th>
      <th>latitude</th>
      <th>longitude</th>
      <th>elevation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>USC00519397</td>
      <td>WAIKIKI 717.2, HI US</td>
      <td>21.27160</td>
      <td>-157.81680</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>USC00513117</td>
      <td>KANEOHE 838.1, HI US</td>
      <td>21.42340</td>
      <td>-157.80150</td>
      <td>14.6</td>
    </tr>
    <tr>
      <th>2</th>
      <td>USC00514830</td>
      <td>KUALOA RANCH HEADQUARTERS 886.9, HI US</td>
      <td>21.52130</td>
      <td>-157.83740</td>
      <td>7.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>USC00517948</td>
      <td>PEARL CITY, HI US</td>
      <td>21.39340</td>
      <td>-157.97510</td>
      <td>11.9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>USC00518838</td>
      <td>UPPER WAHIAWA 874.3, HI US</td>
      <td>21.49920</td>
      <td>-158.01110</td>
      <td>306.6</td>
    </tr>
    <tr>
      <th>5</th>
      <td>USC00519523</td>
      <td>WAIMANALO EXPERIMENTAL FARM, HI US</td>
      <td>21.33556</td>
      <td>-157.71139</td>
      <td>19.5</td>
    </tr>
    <tr>
      <th>6</th>
      <td>USC00519281</td>
      <td>WAIHEE 837.5, HI US</td>
      <td>21.45167</td>
      <td>-157.84889</td>
      <td>32.9</td>
    </tr>
    <tr>
      <th>7</th>
      <td>USC00511918</td>
      <td>HONOLULU OBSERVATORY 702.2, HI US</td>
      <td>21.31520</td>
      <td>-157.99920</td>
      <td>0.9</td>
    </tr>
    <tr>
      <th>8</th>
      <td>USC00516128</td>
      <td>MANOA LYON ARBO 785.2, HI US</td>
      <td>21.33310</td>
      <td>-157.80250</td>
      <td>152.4</td>
    </tr>
  </tbody>
</table>
</div>


