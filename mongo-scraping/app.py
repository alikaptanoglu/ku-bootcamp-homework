from flask import Flask, render_template, jsonify, redirect, send_from_directory

import pymongo
import pandas as pd
import scrape_mars

   
app = Flask(__name__)


# use mongodb
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)
db_planetdetails = client.planetdetails
db_planetfacts = client.planetfacts
db_planetimages = client.planetimages



@app.route("/")
def home():
    # details
    planet_details = db_planetdetails.listings.find()
    if planet_details != None:
        planet_details = planet_details.next()
    # facts
    planet_facts = db_planetfacts.listings.find()
    facts = []
    labels = []
    for each in planet_facts:
        print(each)
        facts.append(each['fact'])
        labels.append(each['label'])
    df = pd.DataFrame()
    df['Description'] = labels
    df['Value'] = facts
    df = df.set_index('Description')
    planet_details['facts'] = df.to_html()
    # images
    planet_images = db_planetimages.listings.find()
    images = []
    for each in planet_images:
        images.append({'label': each['label'], 'image_url': each['image_url']})

    return render_template("index.html", planetdetails=planet_details, planetimages=images)


@app.route("/scrape")
def scrape():
    scrape_mars.scrape_all()     
    return redirect("http://localhost:5000/", code=302)


if __name__ == "__main__":
    app.run(debug=True)
