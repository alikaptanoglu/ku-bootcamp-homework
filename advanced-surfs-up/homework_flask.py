import datetime as dt
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from dateutil.relativedelta import relativedelta
import datetime

from flask import Flask, jsonify


# database Setup
engine = create_engine("sqlite:///databases/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to the invoices and invoice_items tables
measurements = Base.classes.measurements
stations = Base.classes.stations

# Create our session (link) from Python to the DB
session = Session(engine)

# Flask Setup
app = Flask(__name__)


# Flask Routes
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Avalable Routes:<br/>"
        f"/api/v1.0/precipitation"
        f"- Query for dates and temperature observations from previous year<br/>"

        f"/api/v1.0/stations"
        f"- List of stations<br/>"

        f"/api/v1.0/tobs"
        f"- List of temperature observations for previous year<br/>"

        f"/api/v1.0/<start>"
        f"/api/v1.0/<start>/<end>"
        f"- List of min, max, avg temperature of a given start date<br/>"
        f"- List of min, max, avg temperature of a given range (start, end) date<br/>"
    )


@app.route("/api/v1.0/precipitation")
def getPrecipitation():
    # set today's date as end date
    start_date = datetime.datetime.now() + relativedelta(months=-12)

    # query for dates and temperature observations from previous year"
    result = session.query(measurements.date, measurements.prcp).filter(measurements.date >= start_date).all()

    # convert list of tuples into normal list
    prcps = []
    for each in result:
        prcps.append({"date": each.date, "prcp": each.prcp})

    return jsonify(prcps)


@app.route("/api/v1.0/stations")
def getStations():
    # list all stations
    result = session.query(stations.station, stations.name).all()

    # convert list of tuples into normal list
    stats = []
    for each in result:
        stats.append({"station": each[0], "name": each[1]})

    return jsonify(stats)


@app.route("/api/v1.0/tobs")
def getTemperatureObservations():
    # set today's date as end date
    start_date = datetime.datetime.now() + relativedelta(months=-12)

    # get all temperature observations for previous year
    result = session.query(measurements.date, measurements.tobs).filter(measurements.date >= start_date).all()

    # convert list of tuples into normal list
    tobs = []
    for each in result:
        tobs.append({"date": each.date, "tobs": each.tobs})

    return jsonify(tobs)


@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def getTobsStart(start, end='0000'):
 
    # convert string to date
    start_date = datetime.datetime.strptime(start,"%Y-%m-%d")

    #calculate the average, min, max
    if end == '0000':
        t_avg = session.query(func.avg(measurements.tobs)).filter(measurements.date >= start_date).scalar()
        t_min = session.query(func.min(measurements.tobs)).filter(measurements.date >= start_date).scalar()
        t_max = session.query(func.max(measurements.tobs)).filter(measurements.date >= start_date).scalar()
    else:
        end_date = datetime.datetime.strptime(end,"%Y-%m-%d")
        
        t_avg = session.query(func.avg(measurements.tobs)).filter(measurements.date >= start_date).filter(measurements.date <= end_date).scalar()
        t_min = session.query(func.min(measurements.tobs)).filter(measurements.date >= start_date).filter(measurements.date <= end_date).scalar()
        t_max = session.query(func.max(measurements.tobs)).filter(measurements.date >= start_date).filter(measurements.date <= end_date).scalar()
  
    tobs = []
    tobs.append({'t_avg': t_avg, 't_min': t_min, 't_max': t_max})
 
    return jsonify(tobs)




if __name__ == '__main__':
    app.run()
