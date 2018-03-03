

```python
import pandas as pd
import os

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.automap import automap_base

from warnings import filterwarnings
import pymysql
filterwarnings('ignore', category=pymysql.Warning)


engine = create_engine('mysql+pymysql://root:LaborDay18@localhost/sakila') 
session = scoped_session(sessionmaker(bind=engine))
```


```python
#use reflection to map the classes
base = automap_base()
base.prepare(engine, reflect=True)
base.classes.keys()
```

    C:\Users\ng_th\Documents\software\Anaconda\lib\site-packages\sqlalchemy\dialects\mysql\reflection.py:170: SAWarning: Did not recognize type 'geometry' of column 'location'
      (type_, name))
    




    ['actor',
     'address',
     'city',
     'country',
     'category',
     'customer',
     'store',
     'staff',
     'film',
     'language',
     'film_actor',
     'film_category',
     'film_text',
     'inventory',
     'payment',
     'rental']




```python
#1a. display first name, last name of actors
sql_query = """
  SELECT first_name, last_name
  FROM actor
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
      <th>first_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>PENELOPE</td>
      <td>GUINESS</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NICK</td>
      <td>WAHLBERG</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ED</td>
      <td>CHASE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>JENNIFER</td>
      <td>DAVIS</td>
    </tr>
    <tr>
      <th>4</th>
      <td>JOHNNY</td>
      <td>LOLLOBRIGIDA</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BETTE</td>
      <td>NICHOLSON</td>
    </tr>
    <tr>
      <th>6</th>
      <td>GRACE</td>
      <td>MOSTEL</td>
    </tr>
    <tr>
      <th>7</th>
      <td>MATTHEW</td>
      <td>JOHANSSON</td>
    </tr>
    <tr>
      <th>8</th>
      <td>JOE</td>
      <td>SWANK</td>
    </tr>
    <tr>
      <th>9</th>
      <td>CHRISTIAN</td>
      <td>GABLE</td>
    </tr>
    <tr>
      <th>10</th>
      <td>ZERO</td>
      <td>CAGE</td>
    </tr>
    <tr>
      <th>11</th>
      <td>KARL</td>
      <td>BERRY</td>
    </tr>
    <tr>
      <th>12</th>
      <td>UMA</td>
      <td>WOOD</td>
    </tr>
    <tr>
      <th>13</th>
      <td>VIVIEN</td>
      <td>BERGEN</td>
    </tr>
    <tr>
      <th>14</th>
      <td>CUBA</td>
      <td>OLIVIER</td>
    </tr>
    <tr>
      <th>15</th>
      <td>FRED</td>
      <td>COSTNER</td>
    </tr>
    <tr>
      <th>16</th>
      <td>HELEN</td>
      <td>VOIGHT</td>
    </tr>
    <tr>
      <th>17</th>
      <td>DAN</td>
      <td>TORN</td>
    </tr>
    <tr>
      <th>18</th>
      <td>BOB</td>
      <td>FAWCETT</td>
    </tr>
    <tr>
      <th>19</th>
      <td>LUCILLE</td>
      <td>TRACY</td>
    </tr>
    <tr>
      <th>20</th>
      <td>KIRSTEN</td>
      <td>PALTROW</td>
    </tr>
    <tr>
      <th>21</th>
      <td>ELVIS</td>
      <td>MARX</td>
    </tr>
    <tr>
      <th>22</th>
      <td>SANDRA</td>
      <td>KILMER</td>
    </tr>
    <tr>
      <th>23</th>
      <td>CAMERON</td>
      <td>STREEP</td>
    </tr>
    <tr>
      <th>24</th>
      <td>KEVIN</td>
      <td>BLOOM</td>
    </tr>
    <tr>
      <th>25</th>
      <td>RIP</td>
      <td>CRAWFORD</td>
    </tr>
    <tr>
      <th>26</th>
      <td>JULIA</td>
      <td>MCQUEEN</td>
    </tr>
    <tr>
      <th>27</th>
      <td>WOODY</td>
      <td>HOFFMAN</td>
    </tr>
    <tr>
      <th>28</th>
      <td>ALEC</td>
      <td>WAYNE</td>
    </tr>
    <tr>
      <th>29</th>
      <td>SANDRA</td>
      <td>PECK</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>170</th>
      <td>OLYMPIA</td>
      <td>PFEIFFER</td>
    </tr>
    <tr>
      <th>171</th>
      <td>GROUCHO</td>
      <td>WILLIAMS</td>
    </tr>
    <tr>
      <th>172</th>
      <td>ALAN</td>
      <td>DREYFUSS</td>
    </tr>
    <tr>
      <th>173</th>
      <td>MICHAEL</td>
      <td>BENING</td>
    </tr>
    <tr>
      <th>174</th>
      <td>WILLIAM</td>
      <td>HACKMAN</td>
    </tr>
    <tr>
      <th>175</th>
      <td>JON</td>
      <td>CHASE</td>
    </tr>
    <tr>
      <th>176</th>
      <td>GENE</td>
      <td>MCKELLEN</td>
    </tr>
    <tr>
      <th>177</th>
      <td>LISA</td>
      <td>MONROE</td>
    </tr>
    <tr>
      <th>178</th>
      <td>ED</td>
      <td>GUINESS</td>
    </tr>
    <tr>
      <th>179</th>
      <td>JEFF</td>
      <td>SILVERSTONE</td>
    </tr>
    <tr>
      <th>180</th>
      <td>MATTHEW</td>
      <td>CARREY</td>
    </tr>
    <tr>
      <th>181</th>
      <td>DEBBIE</td>
      <td>AKROYD</td>
    </tr>
    <tr>
      <th>182</th>
      <td>RUSSELL</td>
      <td>CLOSE</td>
    </tr>
    <tr>
      <th>183</th>
      <td>HUMPHREY</td>
      <td>GARLAND</td>
    </tr>
    <tr>
      <th>184</th>
      <td>MICHAEL</td>
      <td>BOLGER</td>
    </tr>
    <tr>
      <th>185</th>
      <td>JULIA</td>
      <td>ZELLWEGER</td>
    </tr>
    <tr>
      <th>186</th>
      <td>RENEE</td>
      <td>BALL</td>
    </tr>
    <tr>
      <th>187</th>
      <td>ROCK</td>
      <td>DUKAKIS</td>
    </tr>
    <tr>
      <th>188</th>
      <td>CUBA</td>
      <td>BIRCH</td>
    </tr>
    <tr>
      <th>189</th>
      <td>AUDREY</td>
      <td>BAILEY</td>
    </tr>
    <tr>
      <th>190</th>
      <td>GREGORY</td>
      <td>GOODING</td>
    </tr>
    <tr>
      <th>191</th>
      <td>JOHN</td>
      <td>SUVARI</td>
    </tr>
    <tr>
      <th>192</th>
      <td>BURT</td>
      <td>TEMPLE</td>
    </tr>
    <tr>
      <th>193</th>
      <td>MERYL</td>
      <td>ALLEN</td>
    </tr>
    <tr>
      <th>194</th>
      <td>JAYNE</td>
      <td>SILVERSTONE</td>
    </tr>
    <tr>
      <th>195</th>
      <td>BELA</td>
      <td>WALKEN</td>
    </tr>
    <tr>
      <th>196</th>
      <td>REESE</td>
      <td>WEST</td>
    </tr>
    <tr>
      <th>197</th>
      <td>MARY</td>
      <td>KEITEL</td>
    </tr>
    <tr>
      <th>198</th>
      <td>JULIA</td>
      <td>FAWCETT</td>
    </tr>
    <tr>
      <th>199</th>
      <td>THORA</td>
      <td>TEMPLE</td>
    </tr>
  </tbody>
</table>
<p>200 rows × 2 columns</p>
</div>




```python
#1b. display first name, last name of actors in all caps and single column
sql_query = """
  SELECT UPPER(CONCAT(first_name, " ", last_name)) as "Actor Name"
  FROM actor
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
      <th>Actor Name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>PENELOPE GUINESS</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NICK WAHLBERG</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ED CHASE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>JENNIFER DAVIS</td>
    </tr>
    <tr>
      <th>4</th>
      <td>JOHNNY LOLLOBRIGIDA</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BETTE NICHOLSON</td>
    </tr>
    <tr>
      <th>6</th>
      <td>GRACE MOSTEL</td>
    </tr>
    <tr>
      <th>7</th>
      <td>MATTHEW JOHANSSON</td>
    </tr>
    <tr>
      <th>8</th>
      <td>JOE SWANK</td>
    </tr>
    <tr>
      <th>9</th>
      <td>CHRISTIAN GABLE</td>
    </tr>
    <tr>
      <th>10</th>
      <td>ZERO CAGE</td>
    </tr>
    <tr>
      <th>11</th>
      <td>KARL BERRY</td>
    </tr>
    <tr>
      <th>12</th>
      <td>UMA WOOD</td>
    </tr>
    <tr>
      <th>13</th>
      <td>VIVIEN BERGEN</td>
    </tr>
    <tr>
      <th>14</th>
      <td>CUBA OLIVIER</td>
    </tr>
    <tr>
      <th>15</th>
      <td>FRED COSTNER</td>
    </tr>
    <tr>
      <th>16</th>
      <td>HELEN VOIGHT</td>
    </tr>
    <tr>
      <th>17</th>
      <td>DAN TORN</td>
    </tr>
    <tr>
      <th>18</th>
      <td>BOB FAWCETT</td>
    </tr>
    <tr>
      <th>19</th>
      <td>LUCILLE TRACY</td>
    </tr>
    <tr>
      <th>20</th>
      <td>KIRSTEN PALTROW</td>
    </tr>
    <tr>
      <th>21</th>
      <td>ELVIS MARX</td>
    </tr>
    <tr>
      <th>22</th>
      <td>SANDRA KILMER</td>
    </tr>
    <tr>
      <th>23</th>
      <td>CAMERON STREEP</td>
    </tr>
    <tr>
      <th>24</th>
      <td>KEVIN BLOOM</td>
    </tr>
    <tr>
      <th>25</th>
      <td>RIP CRAWFORD</td>
    </tr>
    <tr>
      <th>26</th>
      <td>JULIA MCQUEEN</td>
    </tr>
    <tr>
      <th>27</th>
      <td>WOODY HOFFMAN</td>
    </tr>
    <tr>
      <th>28</th>
      <td>ALEC WAYNE</td>
    </tr>
    <tr>
      <th>29</th>
      <td>SANDRA PECK</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
    </tr>
    <tr>
      <th>170</th>
      <td>OLYMPIA PFEIFFER</td>
    </tr>
    <tr>
      <th>171</th>
      <td>GROUCHO WILLIAMS</td>
    </tr>
    <tr>
      <th>172</th>
      <td>ALAN DREYFUSS</td>
    </tr>
    <tr>
      <th>173</th>
      <td>MICHAEL BENING</td>
    </tr>
    <tr>
      <th>174</th>
      <td>WILLIAM HACKMAN</td>
    </tr>
    <tr>
      <th>175</th>
      <td>JON CHASE</td>
    </tr>
    <tr>
      <th>176</th>
      <td>GENE MCKELLEN</td>
    </tr>
    <tr>
      <th>177</th>
      <td>LISA MONROE</td>
    </tr>
    <tr>
      <th>178</th>
      <td>ED GUINESS</td>
    </tr>
    <tr>
      <th>179</th>
      <td>JEFF SILVERSTONE</td>
    </tr>
    <tr>
      <th>180</th>
      <td>MATTHEW CARREY</td>
    </tr>
    <tr>
      <th>181</th>
      <td>DEBBIE AKROYD</td>
    </tr>
    <tr>
      <th>182</th>
      <td>RUSSELL CLOSE</td>
    </tr>
    <tr>
      <th>183</th>
      <td>HUMPHREY GARLAND</td>
    </tr>
    <tr>
      <th>184</th>
      <td>MICHAEL BOLGER</td>
    </tr>
    <tr>
      <th>185</th>
      <td>JULIA ZELLWEGER</td>
    </tr>
    <tr>
      <th>186</th>
      <td>RENEE BALL</td>
    </tr>
    <tr>
      <th>187</th>
      <td>ROCK DUKAKIS</td>
    </tr>
    <tr>
      <th>188</th>
      <td>CUBA BIRCH</td>
    </tr>
    <tr>
      <th>189</th>
      <td>AUDREY BAILEY</td>
    </tr>
    <tr>
      <th>190</th>
      <td>GREGORY GOODING</td>
    </tr>
    <tr>
      <th>191</th>
      <td>JOHN SUVARI</td>
    </tr>
    <tr>
      <th>192</th>
      <td>BURT TEMPLE</td>
    </tr>
    <tr>
      <th>193</th>
      <td>MERYL ALLEN</td>
    </tr>
    <tr>
      <th>194</th>
      <td>JAYNE SILVERSTONE</td>
    </tr>
    <tr>
      <th>195</th>
      <td>BELA WALKEN</td>
    </tr>
    <tr>
      <th>196</th>
      <td>REESE WEST</td>
    </tr>
    <tr>
      <th>197</th>
      <td>MARY KEITEL</td>
    </tr>
    <tr>
      <th>198</th>
      <td>JULIA FAWCETT</td>
    </tr>
    <tr>
      <th>199</th>
      <td>THORA TEMPLE</td>
    </tr>
  </tbody>
</table>
<p>200 rows × 1 columns</p>
</div>




```python
#2a. find actor first name=Joe, get id, first name, and last name
sql_query = """
  SELECT actor_id, first_name, last_name
  FROM actor
  WHERE first_name = "Joe"
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
      <th>actor_id</th>
      <th>first_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>9</td>
      <td>JOE</td>
      <td>SWANK</td>
    </tr>
  </tbody>
</table>
</div>




```python
#2b. find all actor whose last names contain GEN
sql_query = """
  SELECT *
  FROM actor
  WHERE last_name like "%%GEN%%"
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
      <th>actor_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>last_update</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>14</td>
      <td>VIVIEN</td>
      <td>BERGEN</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>1</th>
      <td>41</td>
      <td>JODIE</td>
      <td>DEGENERES</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>2</th>
      <td>107</td>
      <td>GINA</td>
      <td>DEGENERES</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>3</th>
      <td>166</td>
      <td>NICK</td>
      <td>DEGENERES</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
  </tbody>
</table>
</div>




```python
#2c. find all actor whose last names contain LI
#    order by last_name, first_name
sql_query = """
  SELECT *
  FROM actor
  WHERE last_name like "%%LI%%"
  ORDER BY last_name, first_name  
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
      <th>actor_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>last_update</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>86</td>
      <td>GREG</td>
      <td>CHAPLIN</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>1</th>
      <td>82</td>
      <td>WOODY</td>
      <td>JOLIE</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>2</th>
      <td>34</td>
      <td>AUDREY</td>
      <td>OLIVIER</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>3</th>
      <td>15</td>
      <td>CUBA</td>
      <td>OLIVIER</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>4</th>
      <td>172</td>
      <td>GROUCHO</td>
      <td>WILLIAMS</td>
      <td>2018-03-02 20:58:51</td>
    </tr>
    <tr>
      <th>5</th>
      <td>137</td>
      <td>MORGAN</td>
      <td>WILLIAMS</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>6</th>
      <td>72</td>
      <td>SEAN</td>
      <td>WILLIAMS</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>7</th>
      <td>83</td>
      <td>BEN</td>
      <td>WILLIS</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>8</th>
      <td>96</td>
      <td>GENE</td>
      <td>WILLIS</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>9</th>
      <td>164</td>
      <td>HUMPHREY</td>
      <td>WILLIS</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
  </tbody>
</table>
</div>




```python
#2d. display country id, country of these three countries
sql_query = """
  SELECT country_id, country
  FROM country
  WHERE country in ("Afghanistan", "Bangladesh", "China")
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
      <th>country_id</th>
      <th>country</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Afghanistan</td>
    </tr>
    <tr>
      <th>1</th>
      <td>12</td>
      <td>Bangladesh</td>
    </tr>
    <tr>
      <th>2</th>
      <td>23</td>
      <td>China</td>
    </tr>
  </tbody>
</table>
</div>




```python
#3a1. remove column middle_name if exists
#engine.execute("ALTER TABLE actor DROP middle_name")

#3a. add column Middle Name between first_name and last_name
engine.execute("ALTER TABLE actor ADD COLUMN middle_name VARCHAR(50) AFTER first_name")


```




    <sqlalchemy.engine.result.ResultProxy at 0x1685678fb00>




```python
#check the new column
sql_query = """
   SELECT *
   FROM actor
   ;
"""
check = pd.read_sql_query(sql_query, engine)
check.head()
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
      <th>actor_id</th>
      <th>first_name</th>
      <th>middle_name</th>
      <th>last_name</th>
      <th>last_update</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>PENELOPE</td>
      <td>None</td>
      <td>GUINESS</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>NICK</td>
      <td>None</td>
      <td>WAHLBERG</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>ED</td>
      <td>None</td>
      <td>CHASE</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>JENNIFER</td>
      <td>None</td>
      <td>DAVIS</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>JOHNNY</td>
      <td>None</td>
      <td>LOLLOBRIGIDA</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
  </tbody>
</table>
</div>




```python
#check new column
sql_query = """
    SELECT *
    FROM actor
    ;
"""
check = pd.read_sql_query(sql_query, engine)
check.head()

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
      <th>actor_id</th>
      <th>first_name</th>
      <th>middle_name</th>
      <th>last_name</th>
      <th>last_update</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>PENELOPE</td>
      <td>None</td>
      <td>GUINESS</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>NICK</td>
      <td>None</td>
      <td>WAHLBERG</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>ED</td>
      <td>None</td>
      <td>CHASE</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>JENNIFER</td>
      <td>None</td>
      <td>DAVIS</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>JOHNNY</td>
      <td>None</td>
      <td>LOLLOBRIGIDA</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
  </tbody>
</table>
</div>




```python
#3b. change column middle_name to blobs
engine.execute("ALTER TABLE actor CHANGE middle_name middle_name BLOB")
```




    <sqlalchemy.engine.result.ResultProxy at 0x1685670db38>




```python
#3c. remove column middle_name
engine.execute("ALTER TABLE actor DROP middle_name")

```




    <sqlalchemy.engine.result.ResultProxy at 0x168567a16a0>




```python
#check the columns
sql_query = """
   SELECT *
   FROM actor
   ;
"""
check = pd.read_sql_query(sql_query, engine)
check.head()
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
      <th>actor_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>last_update</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>PENELOPE</td>
      <td>GUINESS</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>NICK</td>
      <td>WAHLBERG</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>ED</td>
      <td>CHASE</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>JENNIFER</td>
      <td>DAVIS</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>JOHNNY</td>
      <td>LOLLOBRIGIDA</td>
      <td>2006-02-15 04:34:33</td>
    </tr>
  </tbody>
</table>
</div>




```python
#4a. list actors by last name and count
Actor = base.classes.actor

counts = session.query(Actor.last_name, func.count()).group_by(Actor.last_name)
count_by_last_name = []
for each in counts:
    count_by_last_name.append({"Last Name": each[0],"Count": each[1]})
name_df = pd.DataFrame(count_by_last_name,columns=["Last Name", "Count"])
name_df
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
      <th>Last Name</th>
      <th>Count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AKROYD</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ALLEN</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ASTAIRE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BACALL</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>BAILEY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BALE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>BALL</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>BARRYMORE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>BASINGER</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>BENING</td>
      <td>2</td>
    </tr>
    <tr>
      <th>10</th>
      <td>BERGEN</td>
      <td>1</td>
    </tr>
    <tr>
      <th>11</th>
      <td>BERGMAN</td>
      <td>1</td>
    </tr>
    <tr>
      <th>12</th>
      <td>BERRY</td>
      <td>3</td>
    </tr>
    <tr>
      <th>13</th>
      <td>BIRCH</td>
      <td>1</td>
    </tr>
    <tr>
      <th>14</th>
      <td>BLOOM</td>
      <td>1</td>
    </tr>
    <tr>
      <th>15</th>
      <td>BOLGER</td>
      <td>2</td>
    </tr>
    <tr>
      <th>16</th>
      <td>BRIDGES</td>
      <td>1</td>
    </tr>
    <tr>
      <th>17</th>
      <td>BRODY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>18</th>
      <td>BULLOCK</td>
      <td>1</td>
    </tr>
    <tr>
      <th>19</th>
      <td>CAGE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>20</th>
      <td>CARREY</td>
      <td>1</td>
    </tr>
    <tr>
      <th>21</th>
      <td>CHAPLIN</td>
      <td>1</td>
    </tr>
    <tr>
      <th>22</th>
      <td>CHASE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>23</th>
      <td>CLOSE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>24</th>
      <td>COSTNER</td>
      <td>1</td>
    </tr>
    <tr>
      <th>25</th>
      <td>CRAWFORD</td>
      <td>2</td>
    </tr>
    <tr>
      <th>26</th>
      <td>CRONYN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>27</th>
      <td>CROWE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>28</th>
      <td>CRUISE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>29</th>
      <td>CRUZ</td>
      <td>1</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>91</th>
      <td>POSEY</td>
      <td>1</td>
    </tr>
    <tr>
      <th>92</th>
      <td>PRESLEY</td>
      <td>1</td>
    </tr>
    <tr>
      <th>93</th>
      <td>REYNOLDS</td>
      <td>1</td>
    </tr>
    <tr>
      <th>94</th>
      <td>RYDER</td>
      <td>1</td>
    </tr>
    <tr>
      <th>95</th>
      <td>SILVERSTONE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>96</th>
      <td>SINATRA</td>
      <td>1</td>
    </tr>
    <tr>
      <th>97</th>
      <td>SOBIESKI</td>
      <td>1</td>
    </tr>
    <tr>
      <th>98</th>
      <td>STALLONE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>99</th>
      <td>STREEP</td>
      <td>2</td>
    </tr>
    <tr>
      <th>100</th>
      <td>SUVARI</td>
      <td>1</td>
    </tr>
    <tr>
      <th>101</th>
      <td>SWANK</td>
      <td>1</td>
    </tr>
    <tr>
      <th>102</th>
      <td>TANDY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>103</th>
      <td>TAUTOU</td>
      <td>1</td>
    </tr>
    <tr>
      <th>104</th>
      <td>TEMPLE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>105</th>
      <td>TOMEI</td>
      <td>1</td>
    </tr>
    <tr>
      <th>106</th>
      <td>TORN</td>
      <td>3</td>
    </tr>
    <tr>
      <th>107</th>
      <td>TRACY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>108</th>
      <td>VOIGHT</td>
      <td>1</td>
    </tr>
    <tr>
      <th>109</th>
      <td>WAHLBERG</td>
      <td>2</td>
    </tr>
    <tr>
      <th>110</th>
      <td>WALKEN</td>
      <td>1</td>
    </tr>
    <tr>
      <th>111</th>
      <td>WAYNE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>112</th>
      <td>WEST</td>
      <td>2</td>
    </tr>
    <tr>
      <th>113</th>
      <td>WILLIAMS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>114</th>
      <td>WILLIS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>115</th>
      <td>WILSON</td>
      <td>1</td>
    </tr>
    <tr>
      <th>116</th>
      <td>WINSLET</td>
      <td>2</td>
    </tr>
    <tr>
      <th>117</th>
      <td>WITHERSPOON</td>
      <td>1</td>
    </tr>
    <tr>
      <th>118</th>
      <td>WOOD</td>
      <td>2</td>
    </tr>
    <tr>
      <th>119</th>
      <td>WRAY</td>
      <td>1</td>
    </tr>
    <tr>
      <th>120</th>
      <td>ZELLWEGER</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
<p>121 rows × 2 columns</p>
</div>




```python
#4b. list only more than 2 actors that share same last names
counts = session.query(Actor.last_name, func.count()).\
            group_by(Actor.last_name).having(func.count()>1)
count_by_last_name = []
for each in counts:
    count_by_last_name.append({"Last Name": each[0],"Count": each[1]})
name_df = pd.DataFrame(count_by_last_name,columns=["Last Name", "Count"])
name_df
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
      <th>Last Name</th>
      <th>Count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AKROYD</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ALLEN</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>BAILEY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BENING</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>BERRY</td>
      <td>3</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BOLGER</td>
      <td>2</td>
    </tr>
    <tr>
      <th>6</th>
      <td>BRODY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>CAGE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>8</th>
      <td>CHASE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>9</th>
      <td>CRAWFORD</td>
      <td>2</td>
    </tr>
    <tr>
      <th>10</th>
      <td>CRONYN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>11</th>
      <td>DAVIS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>12</th>
      <td>DEAN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>13</th>
      <td>DEE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>14</th>
      <td>DEGENERES</td>
      <td>3</td>
    </tr>
    <tr>
      <th>15</th>
      <td>DENCH</td>
      <td>2</td>
    </tr>
    <tr>
      <th>16</th>
      <td>DEPP</td>
      <td>2</td>
    </tr>
    <tr>
      <th>17</th>
      <td>DUKAKIS</td>
      <td>2</td>
    </tr>
    <tr>
      <th>18</th>
      <td>FAWCETT</td>
      <td>2</td>
    </tr>
    <tr>
      <th>19</th>
      <td>GARLAND</td>
      <td>3</td>
    </tr>
    <tr>
      <th>20</th>
      <td>GOODING</td>
      <td>2</td>
    </tr>
    <tr>
      <th>21</th>
      <td>GUINESS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>22</th>
      <td>HACKMAN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>23</th>
      <td>HARRIS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>24</th>
      <td>HOFFMAN</td>
      <td>3</td>
    </tr>
    <tr>
      <th>25</th>
      <td>HOPKINS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>26</th>
      <td>HOPPER</td>
      <td>2</td>
    </tr>
    <tr>
      <th>27</th>
      <td>JACKMAN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>28</th>
      <td>JOHANSSON</td>
      <td>3</td>
    </tr>
    <tr>
      <th>29</th>
      <td>KEITEL</td>
      <td>3</td>
    </tr>
    <tr>
      <th>30</th>
      <td>KILMER</td>
      <td>5</td>
    </tr>
    <tr>
      <th>31</th>
      <td>MCCONAUGHEY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>32</th>
      <td>MCKELLEN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>33</th>
      <td>MCQUEEN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>34</th>
      <td>MONROE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>35</th>
      <td>MOSTEL</td>
      <td>2</td>
    </tr>
    <tr>
      <th>36</th>
      <td>NEESON</td>
      <td>2</td>
    </tr>
    <tr>
      <th>37</th>
      <td>NOLTE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>38</th>
      <td>OLIVIER</td>
      <td>2</td>
    </tr>
    <tr>
      <th>39</th>
      <td>PALTROW</td>
      <td>2</td>
    </tr>
    <tr>
      <th>40</th>
      <td>PECK</td>
      <td>3</td>
    </tr>
    <tr>
      <th>41</th>
      <td>PENN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>42</th>
      <td>SILVERSTONE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>43</th>
      <td>STREEP</td>
      <td>2</td>
    </tr>
    <tr>
      <th>44</th>
      <td>TANDY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>45</th>
      <td>TEMPLE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>46</th>
      <td>TORN</td>
      <td>3</td>
    </tr>
    <tr>
      <th>47</th>
      <td>TRACY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>48</th>
      <td>WAHLBERG</td>
      <td>2</td>
    </tr>
    <tr>
      <th>49</th>
      <td>WEST</td>
      <td>2</td>
    </tr>
    <tr>
      <th>50</th>
      <td>WILLIAMS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>51</th>
      <td>WILLIS</td>
      <td>3</td>
    </tr>
    <tr>
      <th>52</th>
      <td>WINSLET</td>
      <td>2</td>
    </tr>
    <tr>
      <th>53</th>
      <td>WOOD</td>
      <td>2</td>
    </tr>
    <tr>
      <th>54</th>
      <td>ZELLWEGER</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>




```python
#4c. correct actor name from GROUCHO WILLIAMS to HARPO WILLIAMS
engine.execute("UPDATE actor SET first_name = 'HARPO' WHERE \
                first_name = 'GROUCHO' AND last_name = 'WILLIAMS'")


```




    <sqlalchemy.engine.result.ResultProxy at 0x168567f8278>




```python
#check the update
sql_query = """
   SELECT *
   FROM actor
   WHERE first_name = 'GROUCHO'
   ;
"""
check = pd.read_sql_query(sql_query, engine)
check.head()
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
      <th>actor_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>last_update</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
#4a1. find the actor_id of HARPO
sql_query = """
   SELECT actor_id
   FROM actor
   WHERE first_name = 'HARPO'
   ;
"""
check = pd.read_sql_query(sql_query, engine)
check
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
      <th>actor_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>172</td>
    </tr>
  </tbody>
</table>
</div>




```python
#4d. correct actor name from GROUCHO WILLIAMS to HARPO WILLIAMS
engine.execute("UPDATE actor SET first_name = 'GROUCHO' WHERE \
                actor_id = 172")
engine.execute("UPDATE actor SET first_name = 'MUCHO GROUCHO' WHERE \
                first_name = 'GROUCHO' AND actor_id <> 172")


```




    <sqlalchemy.engine.result.ResultProxy at 0x16856780be0>




```python
#check the update
sql_query = """
   SELECT *
   FROM actor
   WHERE first_name like '%%GROUCHO%%'
   ;
"""
check = pd.read_sql_query(sql_query, engine)
check
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
      <th>actor_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>last_update</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>78</td>
      <td>MUCHO GROUCHO</td>
      <td>SINATRA</td>
      <td>2018-03-01 18:26:12</td>
    </tr>
    <tr>
      <th>1</th>
      <td>106</td>
      <td>MUCHO GROUCHO</td>
      <td>DUNST</td>
      <td>2018-03-01 18:26:12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>172</td>
      <td>GROUCHO</td>
      <td>WILLIAMS</td>
      <td>2018-03-02 21:10:44</td>
    </tr>
  </tbody>
</table>
</div>




```python
#5a. show schema of table address
sql_query = """
    SELECT *
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'sakila' AND TABLE_NAME = 'address'
    ;
"""
schema = pd.read_sql_query(sql_query, engine)
schema
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
      <th>TABLE_CATALOG</th>
      <th>TABLE_SCHEMA</th>
      <th>TABLE_NAME</th>
      <th>COLUMN_NAME</th>
      <th>ORDINAL_POSITION</th>
      <th>COLUMN_DEFAULT</th>
      <th>IS_NULLABLE</th>
      <th>DATA_TYPE</th>
      <th>CHARACTER_MAXIMUM_LENGTH</th>
      <th>CHARACTER_OCTET_LENGTH</th>
      <th>...</th>
      <th>DATETIME_PRECISION</th>
      <th>CHARACTER_SET_NAME</th>
      <th>COLLATION_NAME</th>
      <th>COLUMN_TYPE</th>
      <th>COLUMN_KEY</th>
      <th>EXTRA</th>
      <th>PRIVILEGES</th>
      <th>COLUMN_COMMENT</th>
      <th>GENERATION_EXPRESSION</th>
      <th>SRS_ID</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>def</td>
      <td>sakila</td>
      <td>address</td>
      <td>address_id</td>
      <td>1</td>
      <td>None</td>
      <td>NO</td>
      <td>smallint</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>smallint(5) unsigned</td>
      <td>PRI</td>
      <td>auto_increment</td>
      <td>select,insert,update,references</td>
      <td></td>
      <td></td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>def</td>
      <td>sakila</td>
      <td>address</td>
      <td>address</td>
      <td>2</td>
      <td>None</td>
      <td>NO</td>
      <td>varchar</td>
      <td>50.0</td>
      <td>150.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>utf8</td>
      <td>utf8_general_ci</td>
      <td>varchar(50)</td>
      <td></td>
      <td></td>
      <td>select,insert,update,references</td>
      <td></td>
      <td></td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>def</td>
      <td>sakila</td>
      <td>address</td>
      <td>address2</td>
      <td>3</td>
      <td>None</td>
      <td>YES</td>
      <td>varchar</td>
      <td>50.0</td>
      <td>150.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>utf8</td>
      <td>utf8_general_ci</td>
      <td>varchar(50)</td>
      <td></td>
      <td></td>
      <td>select,insert,update,references</td>
      <td></td>
      <td></td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>def</td>
      <td>sakila</td>
      <td>address</td>
      <td>district</td>
      <td>4</td>
      <td>None</td>
      <td>NO</td>
      <td>varchar</td>
      <td>20.0</td>
      <td>60.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>utf8</td>
      <td>utf8_general_ci</td>
      <td>varchar(20)</td>
      <td></td>
      <td></td>
      <td>select,insert,update,references</td>
      <td></td>
      <td></td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>def</td>
      <td>sakila</td>
      <td>address</td>
      <td>city_id</td>
      <td>5</td>
      <td>None</td>
      <td>NO</td>
      <td>smallint</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>smallint(5) unsigned</td>
      <td>MUL</td>
      <td></td>
      <td>select,insert,update,references</td>
      <td></td>
      <td></td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>def</td>
      <td>sakila</td>
      <td>address</td>
      <td>postal_code</td>
      <td>6</td>
      <td>None</td>
      <td>YES</td>
      <td>varchar</td>
      <td>10.0</td>
      <td>30.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>utf8</td>
      <td>utf8_general_ci</td>
      <td>varchar(10)</td>
      <td></td>
      <td></td>
      <td>select,insert,update,references</td>
      <td></td>
      <td></td>
      <td>None</td>
    </tr>
    <tr>
      <th>6</th>
      <td>def</td>
      <td>sakila</td>
      <td>address</td>
      <td>phone</td>
      <td>7</td>
      <td>None</td>
      <td>NO</td>
      <td>varchar</td>
      <td>20.0</td>
      <td>60.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>utf8</td>
      <td>utf8_general_ci</td>
      <td>varchar(20)</td>
      <td></td>
      <td></td>
      <td>select,insert,update,references</td>
      <td></td>
      <td></td>
      <td>None</td>
    </tr>
    <tr>
      <th>7</th>
      <td>def</td>
      <td>sakila</td>
      <td>address</td>
      <td>location</td>
      <td>8</td>
      <td>None</td>
      <td>NO</td>
      <td>geometry</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>geometry</td>
      <td>MUL</td>
      <td></td>
      <td>select,insert,update,references</td>
      <td></td>
      <td></td>
      <td>None</td>
    </tr>
    <tr>
      <th>8</th>
      <td>def</td>
      <td>sakila</td>
      <td>address</td>
      <td>last_update</td>
      <td>9</td>
      <td>CURRENT_TIMESTAMP</td>
      <td>NO</td>
      <td>timestamp</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>0.0</td>
      <td>None</td>
      <td>None</td>
      <td>timestamp</td>
      <td></td>
      <td>on update CURRENT_TIMESTAMP</td>
      <td>select,insert,update,references</td>
      <td></td>
      <td></td>
      <td>None</td>
    </tr>
  </tbody>
</table>
<p>9 rows × 22 columns</p>
</div>




```python
#6a. display first name, last name, address of each staff member
sql_query = """
    SELECT s.first_name, 
           s.last_name,
           a.address
    FROM staff s 
    JOIN address a
    ON s.address_id = a.address_id
    ;
"""
staff = pd.read_sql_query(sql_query, engine)
staff


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
      <th>first_name</th>
      <th>last_name</th>
      <th>address</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mike</td>
      <td>Hillyer</td>
      <td>23 Workhaven Lane</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Jon</td>
      <td>Stephens</td>
      <td>1411 Lillydale Drive</td>
    </tr>
  </tbody>
</table>
</div>




```python
#6b. display total amount for each staff member in August 2005
sql_query = """
    SELECT s.staff_id, s.first_name, s.last_name, sum(p.amount) as total
    FROM staff s
    JOIN payment p
    ON s.staff_id = p.staff_id
    WHERE p.payment_date = str_to_date('082005', '%%m%%Y')
    GROUP BY s.staff_id
    ;
"""
total = pd.read_sql_query(sql_query, engine)
total

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
      <th>staff_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>total</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
#6c. display film, number of actors
sql_query = """
    SELECT f.title, count(fa.actor_id) as actor_count
    FROM film_actor fa
    INNER JOIN film f
    ON fa.film_id = f.film_id
    GROUP BY fa.film_id
    ;
"""
actors = pd.read_sql_query(sql_query, engine)
actors

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
      <th>title</th>
      <th>actor_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ACADEMY DINOSAUR</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ACE GOLDFINGER</td>
      <td>4</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ADAPTATION HOLES</td>
      <td>5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AFFAIR PREJUDICE</td>
      <td>5</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AFRICAN EGG</td>
      <td>5</td>
    </tr>
    <tr>
      <th>5</th>
      <td>AGENT TRUMAN</td>
      <td>7</td>
    </tr>
    <tr>
      <th>6</th>
      <td>AIRPLANE SIERRA</td>
      <td>5</td>
    </tr>
    <tr>
      <th>7</th>
      <td>AIRPORT POLLOCK</td>
      <td>4</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ALABAMA DEVIL</td>
      <td>9</td>
    </tr>
    <tr>
      <th>9</th>
      <td>ALADDIN CALENDAR</td>
      <td>8</td>
    </tr>
    <tr>
      <th>10</th>
      <td>ALAMO VIDEOTAPE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>11</th>
      <td>ALASKA PHANTOM</td>
      <td>7</td>
    </tr>
    <tr>
      <th>12</th>
      <td>ALI FOREVER</td>
      <td>5</td>
    </tr>
    <tr>
      <th>13</th>
      <td>ALICE FANTASIA</td>
      <td>4</td>
    </tr>
    <tr>
      <th>14</th>
      <td>ALIEN CENTER</td>
      <td>6</td>
    </tr>
    <tr>
      <th>15</th>
      <td>ALLEY EVOLUTION</td>
      <td>5</td>
    </tr>
    <tr>
      <th>16</th>
      <td>ALONE TRIP</td>
      <td>8</td>
    </tr>
    <tr>
      <th>17</th>
      <td>ALTER VICTORY</td>
      <td>4</td>
    </tr>
    <tr>
      <th>18</th>
      <td>AMADEUS HOLY</td>
      <td>6</td>
    </tr>
    <tr>
      <th>19</th>
      <td>AMELIE HELLFIGHTERS</td>
      <td>6</td>
    </tr>
    <tr>
      <th>20</th>
      <td>AMERICAN CIRCUS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>21</th>
      <td>AMISTAD MIDSUMMER</td>
      <td>4</td>
    </tr>
    <tr>
      <th>22</th>
      <td>ANACONDA CONFESSIONS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>23</th>
      <td>ANALYZE HOOSIERS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>24</th>
      <td>ANGELS LIFE</td>
      <td>9</td>
    </tr>
    <tr>
      <th>25</th>
      <td>ANNIE IDENTITY</td>
      <td>3</td>
    </tr>
    <tr>
      <th>26</th>
      <td>ANONYMOUS HUMAN</td>
      <td>9</td>
    </tr>
    <tr>
      <th>27</th>
      <td>ANTHEM LUKE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>28</th>
      <td>ANTITRUST TOMATOES</td>
      <td>7</td>
    </tr>
    <tr>
      <th>29</th>
      <td>ANYTHING SAVANNAH</td>
      <td>3</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>967</th>
      <td>WHALE BIKINI</td>
      <td>5</td>
    </tr>
    <tr>
      <th>968</th>
      <td>WHISPERER GIANT</td>
      <td>3</td>
    </tr>
    <tr>
      <th>969</th>
      <td>WIFE TURN</td>
      <td>6</td>
    </tr>
    <tr>
      <th>970</th>
      <td>WILD APOLLO</td>
      <td>4</td>
    </tr>
    <tr>
      <th>971</th>
      <td>WILLOW TRACY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>972</th>
      <td>WIND PHANTOM</td>
      <td>3</td>
    </tr>
    <tr>
      <th>973</th>
      <td>WINDOW SIDE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>974</th>
      <td>WISDOM WORKER</td>
      <td>7</td>
    </tr>
    <tr>
      <th>975</th>
      <td>WITCHES PANIC</td>
      <td>4</td>
    </tr>
    <tr>
      <th>976</th>
      <td>WIZARD COLDBLOODED</td>
      <td>9</td>
    </tr>
    <tr>
      <th>977</th>
      <td>WOLVES DESIRE</td>
      <td>6</td>
    </tr>
    <tr>
      <th>978</th>
      <td>WOMEN DORADO</td>
      <td>5</td>
    </tr>
    <tr>
      <th>979</th>
      <td>WON DARES</td>
      <td>5</td>
    </tr>
    <tr>
      <th>980</th>
      <td>WONDERFUL DROP</td>
      <td>4</td>
    </tr>
    <tr>
      <th>981</th>
      <td>WONDERLAND CHRISTMAS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>982</th>
      <td>WONKA SEA</td>
      <td>2</td>
    </tr>
    <tr>
      <th>983</th>
      <td>WORDS HUNTER</td>
      <td>6</td>
    </tr>
    <tr>
      <th>984</th>
      <td>WORKER TARZAN</td>
      <td>9</td>
    </tr>
    <tr>
      <th>985</th>
      <td>WORKING MICROCOSMOS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>986</th>
      <td>WORLD LEATHERNECKS</td>
      <td>8</td>
    </tr>
    <tr>
      <th>987</th>
      <td>WORST BANGER</td>
      <td>4</td>
    </tr>
    <tr>
      <th>988</th>
      <td>WRATH MILE</td>
      <td>4</td>
    </tr>
    <tr>
      <th>989</th>
      <td>WRONG BEHAVIOR</td>
      <td>9</td>
    </tr>
    <tr>
      <th>990</th>
      <td>WYOMING STORM</td>
      <td>6</td>
    </tr>
    <tr>
      <th>991</th>
      <td>YENTL IDAHO</td>
      <td>1</td>
    </tr>
    <tr>
      <th>992</th>
      <td>YOUNG LANGUAGE</td>
      <td>5</td>
    </tr>
    <tr>
      <th>993</th>
      <td>YOUTH KICK</td>
      <td>5</td>
    </tr>
    <tr>
      <th>994</th>
      <td>ZHIVAGO CORE</td>
      <td>6</td>
    </tr>
    <tr>
      <th>995</th>
      <td>ZOOLANDER FICTION</td>
      <td>5</td>
    </tr>
    <tr>
      <th>996</th>
      <td>ZORRO ARK</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
<p>997 rows × 2 columns</p>
</div>




```python
#6d. number of copies of film Hunchback Impossible in inventory
sql_query = """
    SELECT f.title, count(i.inventory_id)
    FROM film f
    JOIN inventory i
    ON f.film_id = i.film_id
    WHERE f.title = 'Hunchback Impossible'    
    ;
"""
film_count = pd.read_sql_query(sql_query, engine)
film_count

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
      <th>title</th>
      <th>count(i.inventory_id)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>HUNCHBACK IMPOSSIBLE</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>




```python
#6e. display customer first name, last name, total amount paid
sql_query = """
    SELECT c.first_name, c.last_name, sum(p.amount) as 'Total Amount Paid'
    FROM customer c
    JOIN payment p
    ON c.customer_id = p.customer_id
    GROUP BY c.customer_id
    ORDER BY c.last_name
    ;
"""
payment = pd.read_sql_query(sql_query, engine)
payment

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
      <th>first_name</th>
      <th>last_name</th>
      <th>Total Amount Paid</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>RAFAEL</td>
      <td>ABNEY</td>
      <td>97.79</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NATHANIEL</td>
      <td>ADAM</td>
      <td>133.72</td>
    </tr>
    <tr>
      <th>2</th>
      <td>KATHLEEN</td>
      <td>ADAMS</td>
      <td>92.73</td>
    </tr>
    <tr>
      <th>3</th>
      <td>DIANA</td>
      <td>ALEXANDER</td>
      <td>105.73</td>
    </tr>
    <tr>
      <th>4</th>
      <td>GORDON</td>
      <td>ALLARD</td>
      <td>160.68</td>
    </tr>
    <tr>
      <th>5</th>
      <td>SHIRLEY</td>
      <td>ALLEN</td>
      <td>126.69</td>
    </tr>
    <tr>
      <th>6</th>
      <td>CHARLENE</td>
      <td>ALVAREZ</td>
      <td>114.73</td>
    </tr>
    <tr>
      <th>7</th>
      <td>LISA</td>
      <td>ANDERSON</td>
      <td>106.76</td>
    </tr>
    <tr>
      <th>8</th>
      <td>JOSE</td>
      <td>ANDREW</td>
      <td>96.75</td>
    </tr>
    <tr>
      <th>9</th>
      <td>IDA</td>
      <td>ANDREWS</td>
      <td>76.77</td>
    </tr>
    <tr>
      <th>10</th>
      <td>OSCAR</td>
      <td>AQUINO</td>
      <td>99.80</td>
    </tr>
    <tr>
      <th>11</th>
      <td>HARRY</td>
      <td>ARCE</td>
      <td>157.65</td>
    </tr>
    <tr>
      <th>12</th>
      <td>JORDAN</td>
      <td>ARCHULETA</td>
      <td>132.70</td>
    </tr>
    <tr>
      <th>13</th>
      <td>MELANIE</td>
      <td>ARMSTRONG</td>
      <td>92.75</td>
    </tr>
    <tr>
      <th>14</th>
      <td>BEATRICE</td>
      <td>ARNOLD</td>
      <td>119.74</td>
    </tr>
    <tr>
      <th>15</th>
      <td>KENT</td>
      <td>ARSENAULT</td>
      <td>134.73</td>
    </tr>
    <tr>
      <th>16</th>
      <td>CARL</td>
      <td>ARTIS</td>
      <td>106.77</td>
    </tr>
    <tr>
      <th>17</th>
      <td>DARRYL</td>
      <td>ASHCRAFT</td>
      <td>76.77</td>
    </tr>
    <tr>
      <th>18</th>
      <td>TYRONE</td>
      <td>ASHER</td>
      <td>112.76</td>
    </tr>
    <tr>
      <th>19</th>
      <td>ALMA</td>
      <td>AUSTIN</td>
      <td>151.65</td>
    </tr>
    <tr>
      <th>20</th>
      <td>MILDRED</td>
      <td>BAILEY</td>
      <td>98.75</td>
    </tr>
    <tr>
      <th>21</th>
      <td>PAMELA</td>
      <td>BAKER</td>
      <td>95.77</td>
    </tr>
    <tr>
      <th>22</th>
      <td>MARTIN</td>
      <td>BALES</td>
      <td>103.73</td>
    </tr>
    <tr>
      <th>23</th>
      <td>EVERETT</td>
      <td>BANDA</td>
      <td>110.72</td>
    </tr>
    <tr>
      <th>24</th>
      <td>JESSIE</td>
      <td>BANKS</td>
      <td>91.74</td>
    </tr>
    <tr>
      <th>25</th>
      <td>CLAYTON</td>
      <td>BARBEE</td>
      <td>96.74</td>
    </tr>
    <tr>
      <th>26</th>
      <td>ANGEL</td>
      <td>BARCLAY</td>
      <td>115.68</td>
    </tr>
    <tr>
      <th>27</th>
      <td>NICHOLAS</td>
      <td>BARFIELD</td>
      <td>145.68</td>
    </tr>
    <tr>
      <th>28</th>
      <td>VICTOR</td>
      <td>BARKLEY</td>
      <td>91.76</td>
    </tr>
    <tr>
      <th>29</th>
      <td>RACHEL</td>
      <td>BARNES</td>
      <td>84.78</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>569</th>
      <td>THERESA</td>
      <td>WATSON</td>
      <td>99.70</td>
    </tr>
    <tr>
      <th>570</th>
      <td>SHELLY</td>
      <td>WATTS</td>
      <td>113.74</td>
    </tr>
    <tr>
      <th>571</th>
      <td>JAMIE</td>
      <td>WAUGH</td>
      <td>118.75</td>
    </tr>
    <tr>
      <th>572</th>
      <td>MIKE</td>
      <td>WAY</td>
      <td>166.65</td>
    </tr>
    <tr>
      <th>573</th>
      <td>YOLANDA</td>
      <td>WEAVER</td>
      <td>110.73</td>
    </tr>
    <tr>
      <th>574</th>
      <td>ETHEL</td>
      <td>WEBB</td>
      <td>135.68</td>
    </tr>
    <tr>
      <th>575</th>
      <td>RONALD</td>
      <td>WEINER</td>
      <td>132.70</td>
    </tr>
    <tr>
      <th>576</th>
      <td>MARLENE</td>
      <td>WELCH</td>
      <td>117.74</td>
    </tr>
    <tr>
      <th>577</th>
      <td>SHEILA</td>
      <td>WELLS</td>
      <td>73.82</td>
    </tr>
    <tr>
      <th>578</th>
      <td>EDNA</td>
      <td>WEST</td>
      <td>107.74</td>
    </tr>
    <tr>
      <th>579</th>
      <td>MITCHELL</td>
      <td>WESTMORELAND</td>
      <td>134.68</td>
    </tr>
    <tr>
      <th>580</th>
      <td>FRED</td>
      <td>WHEAT</td>
      <td>88.75</td>
    </tr>
    <tr>
      <th>581</th>
      <td>LUCY</td>
      <td>WHEELER</td>
      <td>91.74</td>
    </tr>
    <tr>
      <th>582</th>
      <td>BETTY</td>
      <td>WHITE</td>
      <td>117.72</td>
    </tr>
    <tr>
      <th>583</th>
      <td>ROY</td>
      <td>WHITING</td>
      <td>143.71</td>
    </tr>
    <tr>
      <th>584</th>
      <td>JON</td>
      <td>WILES</td>
      <td>87.76</td>
    </tr>
    <tr>
      <th>585</th>
      <td>LINDA</td>
      <td>WILLIAMS</td>
      <td>135.74</td>
    </tr>
    <tr>
      <th>586</th>
      <td>GINA</td>
      <td>WILLIAMSON</td>
      <td>111.72</td>
    </tr>
    <tr>
      <th>587</th>
      <td>BERNICE</td>
      <td>WILLIS</td>
      <td>145.67</td>
    </tr>
    <tr>
      <th>588</th>
      <td>SUSAN</td>
      <td>WILSON</td>
      <td>92.76</td>
    </tr>
    <tr>
      <th>589</th>
      <td>DARREN</td>
      <td>WINDHAM</td>
      <td>108.76</td>
    </tr>
    <tr>
      <th>590</th>
      <td>VIRGIL</td>
      <td>WOFFORD</td>
      <td>107.73</td>
    </tr>
    <tr>
      <th>591</th>
      <td>LORI</td>
      <td>WOOD</td>
      <td>141.69</td>
    </tr>
    <tr>
      <th>592</th>
      <td>FLORENCE</td>
      <td>WOODS</td>
      <td>126.70</td>
    </tr>
    <tr>
      <th>593</th>
      <td>TYLER</td>
      <td>WREN</td>
      <td>88.79</td>
    </tr>
    <tr>
      <th>594</th>
      <td>BRENDA</td>
      <td>WRIGHT</td>
      <td>104.74</td>
    </tr>
    <tr>
      <th>595</th>
      <td>BRIAN</td>
      <td>WYMAN</td>
      <td>52.88</td>
    </tr>
    <tr>
      <th>596</th>
      <td>LUIS</td>
      <td>YANEZ</td>
      <td>79.80</td>
    </tr>
    <tr>
      <th>597</th>
      <td>MARVIN</td>
      <td>YEE</td>
      <td>75.79</td>
    </tr>
    <tr>
      <th>598</th>
      <td>CYNTHIA</td>
      <td>YOUNG</td>
      <td>111.68</td>
    </tr>
  </tbody>
</table>
<p>599 rows × 3 columns</p>
</div>




```python
#7a. display movies with titles starting with K, Q in English language
sql_query = """
    SELECT title
    FROM film
    WHERE substring(title, 1, 1) in ('K', 'Q')
    AND language_id in 
        (SELECT language_id
         FROM language
         WHERE name = 'English')
    ;
"""
film_count = pd.read_sql_query(sql_query, engine)
film_count

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
      <th>title</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>KANE EXORCIST</td>
    </tr>
    <tr>
      <th>1</th>
      <td>KARATE MOON</td>
    </tr>
    <tr>
      <th>2</th>
      <td>KENTUCKIAN GIANT</td>
    </tr>
    <tr>
      <th>3</th>
      <td>KICK SAVANNAH</td>
    </tr>
    <tr>
      <th>4</th>
      <td>KILL BROTHERHOOD</td>
    </tr>
    <tr>
      <th>5</th>
      <td>KILLER INNOCENT</td>
    </tr>
    <tr>
      <th>6</th>
      <td>KING EVOLUTION</td>
    </tr>
    <tr>
      <th>7</th>
      <td>KISS GLORY</td>
    </tr>
    <tr>
      <th>8</th>
      <td>KISSING DOLLS</td>
    </tr>
    <tr>
      <th>9</th>
      <td>KNOCK WARLOCK</td>
    </tr>
    <tr>
      <th>10</th>
      <td>KRAMER CHOCOLATE</td>
    </tr>
    <tr>
      <th>11</th>
      <td>KWAI HOMEWARD</td>
    </tr>
    <tr>
      <th>12</th>
      <td>QUEEN LUKE</td>
    </tr>
    <tr>
      <th>13</th>
      <td>QUEST MUSSOLINI</td>
    </tr>
    <tr>
      <th>14</th>
      <td>QUILLS BULL</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7b. display actors in movie Alone Trip
sql_query = """
    SELECT first_name, last_name
    FROM actor a
    JOIN film_actor f
    ON a.actor_id = f.actor_id
    WHERE f.film_id in 
        (SELECT film_id
         FROM film
         WHERE title = 'Alone Trip')
    ;
"""
actors_alone_trip = pd.read_sql_query(sql_query, engine)
actors_alone_trip

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
      <th>first_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ED</td>
      <td>CHASE</td>
    </tr>
    <tr>
      <th>1</th>
      <td>KARL</td>
      <td>BERRY</td>
    </tr>
    <tr>
      <th>2</th>
      <td>UMA</td>
      <td>WOOD</td>
    </tr>
    <tr>
      <th>3</th>
      <td>WOODY</td>
      <td>JOLIE</td>
    </tr>
    <tr>
      <th>4</th>
      <td>SPENCER</td>
      <td>DEPP</td>
    </tr>
    <tr>
      <th>5</th>
      <td>CHRIS</td>
      <td>DEPP</td>
    </tr>
    <tr>
      <th>6</th>
      <td>LAURENCE</td>
      <td>BULLOCK</td>
    </tr>
    <tr>
      <th>7</th>
      <td>RENEE</td>
      <td>BALL</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7c. display names and email addresses of Canadian customers
sql_query = """
    SELECT first_name, last_name, email
    FROM customer r
    JOIN store s
    ON r.store_id = s.store_id
    WHERE r.address_id in 
        (SELECT address_id
         FROM address
         WHERE city_id in
         (SELECT city_id
          FROM city
          WHERE country_id in 
          (SELECT country_id
           FROM country
           WHERE country = 'Canada')))
    ;
"""
canadian = pd.read_sql_query(sql_query, engine)
canadian

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
      <th>first_name</th>
      <th>last_name</th>
      <th>email</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>DERRICK</td>
      <td>BOURQUE</td>
      <td>DERRICK.BOURQUE@sakilacustomer.org</td>
    </tr>
    <tr>
      <th>1</th>
      <td>DARRELL</td>
      <td>POWER</td>
      <td>DARRELL.POWER@sakilacustomer.org</td>
    </tr>
    <tr>
      <th>2</th>
      <td>LORETTA</td>
      <td>CARPENTER</td>
      <td>LORETTA.CARPENTER@sakilacustomer.org</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CURTIS</td>
      <td>IRBY</td>
      <td>CURTIS.IRBY@sakilacustomer.org</td>
    </tr>
    <tr>
      <th>4</th>
      <td>TROY</td>
      <td>QUIGLEY</td>
      <td>TROY.QUIGLEY@sakilacustomer.org</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7d. display names and email addresses of Canadian customers
sql_query = """
    SELECT r.first_name, r.last_name, r.email
    FROM customer r
        JOIN store s
             ON r.store_id = s.store_id
        JOIN address a 
             ON r.address_id = a.address_id
        JOIN city c
             ON a.city_id = c.city_id 
        JOIN country y
             ON c.country_id = y.country_id 
    WHERE y.country = 'Canada'
    ;
"""
canadian = pd.read_sql_query(sql_query, engine)
canadian
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
      <th>first_name</th>
      <th>last_name</th>
      <th>email</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>DERRICK</td>
      <td>BOURQUE</td>
      <td>DERRICK.BOURQUE@sakilacustomer.org</td>
    </tr>
    <tr>
      <th>1</th>
      <td>DARRELL</td>
      <td>POWER</td>
      <td>DARRELL.POWER@sakilacustomer.org</td>
    </tr>
    <tr>
      <th>2</th>
      <td>LORETTA</td>
      <td>CARPENTER</td>
      <td>LORETTA.CARPENTER@sakilacustomer.org</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CURTIS</td>
      <td>IRBY</td>
      <td>CURTIS.IRBY@sakilacustomer.org</td>
    </tr>
    <tr>
      <th>4</th>
      <td>TROY</td>
      <td>QUIGLEY</td>
      <td>TROY.QUIGLEY@sakilacustomer.org</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7e. display most frequently rented movies in descending order
sql_query = """
    SELECT f.title, count(r.inventory_id) as count_rental
    FROM rental r
        JOIN inventory i
             ON r.inventory_id = i.inventory_id
        JOIN film f
             ON i.film_id = f.film_id
    GROUP BY r.inventory_id
    ORDER BY count_rental DESC
    ;
"""
frequent = pd.read_sql_query(sql_query, engine)
frequent
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
      <th>title</th>
      <th>count_rental</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>FREDDY STORM</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>FURY MURDER</td>
      <td>5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>GALAXY SWEETHEARTS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>GANGS PRIDE</td>
      <td>5</td>
    </tr>
    <tr>
      <th>4</th>
      <td>GILMORE BOILED</td>
      <td>5</td>
    </tr>
    <tr>
      <th>5</th>
      <td>GLEAMING JAWBREAKER</td>
      <td>5</td>
    </tr>
    <tr>
      <th>6</th>
      <td>GRADUATE LORD</td>
      <td>5</td>
    </tr>
    <tr>
      <th>7</th>
      <td>GUN BONNIE</td>
      <td>5</td>
    </tr>
    <tr>
      <th>8</th>
      <td>HALF OUTFIELD</td>
      <td>5</td>
    </tr>
    <tr>
      <th>9</th>
      <td>HAMLET WISDOM</td>
      <td>5</td>
    </tr>
    <tr>
      <th>10</th>
      <td>HILLS NEIGHBORS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>11</th>
      <td>HOLY TADPOLE</td>
      <td>5</td>
    </tr>
    <tr>
      <th>12</th>
      <td>HOPE TOOTSIE</td>
      <td>5</td>
    </tr>
    <tr>
      <th>13</th>
      <td>HOTEL HAPPINESS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>14</th>
      <td>HUNTING MUSKETEERS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>15</th>
      <td>HYDE DOCTOR</td>
      <td>5</td>
    </tr>
    <tr>
      <th>16</th>
      <td>AFRICAN EGG</td>
      <td>5</td>
    </tr>
    <tr>
      <th>17</th>
      <td>AIRPORT POLLOCK</td>
      <td>5</td>
    </tr>
    <tr>
      <th>18</th>
      <td>ALASKA PHANTOM</td>
      <td>5</td>
    </tr>
    <tr>
      <th>19</th>
      <td>ALIEN CENTER</td>
      <td>5</td>
    </tr>
    <tr>
      <th>20</th>
      <td>ALLEY EVOLUTION</td>
      <td>5</td>
    </tr>
    <tr>
      <th>21</th>
      <td>ALTER VICTORY</td>
      <td>5</td>
    </tr>
    <tr>
      <th>22</th>
      <td>AMADEUS HOLY</td>
      <td>5</td>
    </tr>
    <tr>
      <th>23</th>
      <td>AMERICAN CIRCUS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>24</th>
      <td>ANACONDA CONFESSIONS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>25</th>
      <td>ANTITRUST TOMATOES</td>
      <td>5</td>
    </tr>
    <tr>
      <th>26</th>
      <td>APACHE DIVINE</td>
      <td>5</td>
    </tr>
    <tr>
      <th>27</th>
      <td>AUTUMN CROW</td>
      <td>5</td>
    </tr>
    <tr>
      <th>28</th>
      <td>BANGER PINOCCHIO</td>
      <td>5</td>
    </tr>
    <tr>
      <th>29</th>
      <td>BEACH HEARTBREAKERS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>4550</th>
      <td>THIEF PELICAN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4551</th>
      <td>TRUMAN CRAZY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4552</th>
      <td>TURN STAR</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4553</th>
      <td>TWISTED PIRATES</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4554</th>
      <td>UNDEFEATED DALMATIONS</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4555</th>
      <td>UNFAITHFUL KILL</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4556</th>
      <td>UNITED PILOT</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4557</th>
      <td>ILLUSION AMELIE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4558</th>
      <td>INDIAN LOVE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4559</th>
      <td>VANISHING ROCKY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4560</th>
      <td>VERTIGO NORTHWEST</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4561</th>
      <td>INTENTIONS EMPIRE</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4562</th>
      <td>VIRGINIAN PLUTO</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4563</th>
      <td>VOICE PEACH</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4564</th>
      <td>WAGON JAWS</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4565</th>
      <td>JAWS HARRY</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4566</th>
      <td>WAR NOTTING</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4567</th>
      <td>JEKYLL FROGMEN</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4568</th>
      <td>JOON NORTHWEST</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4569</th>
      <td>LADYBUGS ARMAGEDDON</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4570</th>
      <td>WONKA SEA</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4571</th>
      <td>LEAGUE HELLFIGHTERS</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4572</th>
      <td>LEBOWSKI SOLDIERS</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4573</th>
      <td>WRONG BEHAVIOR</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4574</th>
      <td>LICENSE WEEKEND</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4575</th>
      <td>LION UNCUT</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4576</th>
      <td>MUSKETEERS WAIT</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4577</th>
      <td>ROCKY WAR</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4578</th>
      <td>GALAXY SWEETHEARTS</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4579</th>
      <td>MIXED DOORS</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
<p>4580 rows × 2 columns</p>
</div>




```python
#7f. display total dollars from each store
sql_query = """
    SELECT s.store_id, sum(p.amount) as total_dollars
    FROM payment p
        JOIN customer c
             ON p.customer_id = c.customer_id
        JOIN store s
             ON s.store_id = c.store_id
    GROUP BY s.store_id
    ;
"""
store_dollars = pd.read_sql_query(sql_query, engine)
store_dollars
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
      <th>store_id</th>
      <th>total_dollars</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>37001.52</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>30414.99</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7g. display each store id, city, country
sql_query = """
    SELECT s.store_id, c.city, y.country
    FROM store s
        JOIN address a
             ON s.address_id = a.address_id
        JOIN city c
             ON a.city_id = c.city_id
        JOIN country y
             ON c.country_id = y.country_id
    ;
"""
store_location = pd.read_sql_query(sql_query, engine)
store_location
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
      <th>store_id</th>
      <th>city</th>
      <th>country</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Lethbridge</td>
      <td>Canada</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Woodridge</td>
      <td>Australia</td>
    </tr>
  </tbody>
</table>
</div>




```python
#7h. display top five genres in revenues in descending order
sql_query = """
    SELECT cy.name, sum(p.amount) as revenues
    FROM payment p
        JOIN rental r
             ON p.rental_id = r.rental_id
        JOIN inventory i
             ON r.inventory_id = i.inventory_id
        JOIN film f
             ON i.film_id = f.film_id
        JOIN film_category fc
             ON f.film_id = fc.film_id
        JOIN category cy
             ON fc.category_id = cy.category_id
    GROUP BY fc.category_id
    ORDER BY revenues DESC
    LIMIT 5
    ;
"""
top_five_genres = pd.read_sql_query(sql_query, engine)
top_five_genres
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
      <th>name</th>
      <th>revenues</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Sports</td>
      <td>5314.21</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Sci-Fi</td>
      <td>4756.98</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Animation</td>
      <td>4656.30</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Drama</td>
      <td>4587.39</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Comedy</td>
      <td>4383.58</td>
    </tr>
  </tbody>
</table>
</div>




```python
#8a. create view of 7h.
engine.execute("CREATE VIEW top_view as \
   (SELECT cy.name, sum(p.amount) as revenues \
    FROM payment p \
        JOIN rental r \
             ON p.rental_id = r.rental_id \
        JOIN inventory i \
             ON r.inventory_id = i.inventory_id \
        JOIN film f \
             ON i.film_id = f.film_id \
        JOIN film_category fc \
             ON f.film_id = fc.film_id \
        JOIN category cy \
             ON fc.category_id = cy.category_id \
    GROUP BY fc.category_id \
    ORDER BY revenues DESC \
    LIMIT 5)")

```




    <sqlalchemy.engine.result.ResultProxy at 0x1686930add8>




```python
#8b. display view created in 8a.
sql_query = """
    SELECT *
    FROM top_view
    ;
"""
view = pd.read_sql_query(sql_query, engine)
view

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
      <th>name</th>
      <th>revenues</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Sports</td>
      <td>5314.21</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Sci-Fi</td>
      <td>4756.98</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Animation</td>
      <td>4656.30</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Drama</td>
      <td>4587.39</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Comedy</td>
      <td>4383.58</td>
    </tr>
  </tbody>
</table>
</div>




```python
#8c. delete the top_view
engine.execute("DROP VIEW top_view")
```




    <sqlalchemy.engine.result.ResultProxy at 0x16868e15ba8>


