from splinter import Browser
from bs4 import BeautifulSoup

import pandas as pd
import time
import json
import tweepy
import os
import re
import pymongo
 
# chrome driver
executable_path = {'executable_path': 'chromedriver.exe'}
browser = Browser("chrome", **executable_path, headless=False)

# API keys
api_dir = os.path.dirname(os.path.dirname(os.path.realpath('keys')))
file_name = os.path.join(api_dir + "//keys", "api_keys.json")
data = json.load(open(file_name))

consumer_key = data['twitter_consumer_key']
consumer_secret = data['twitter_consumer_secret']
access_token = data['twitter_access_token']
access_token_secret = data['twitter_access_token_secret']

# tweepy setup
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

# use mongodb
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)
db_planetdetails = client.planetdetails
db_planetfacts = client.planetfacts
db_planetimages = client.planetimages


def scrape_latest_news():
    url = 'https://mars.nasa.gov/news/?page=0&per_page=40&order=publish_date+desc%2Ccreated_at+desc&search=&category=19%2C165%2C184%2C204&blank_scope=Latest'
    browser.visit(url)
    time.sleep(1)
    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')
    
    result = soup.find('li', class_='slide')
    
    title = 'not found'
    para = 'not found'
    try:
        title = result.find('div', class_='content_title').text
        para = result.find('div', class_='article_teaser_body').text
        # only do it once and return
        return {'title': title, 'para': para}
    except Exception as e:
        print('error ', e)
        return {'title': title, 'para': para}


def scrape_feature_image():
    url = 'https://www.jpl.nasa.gov/spaceimages/?search=&category=Mars'
    browser.visit(url)
    time.sleep(1)
    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')
    
    result = soup.find('article', class_='carousel_item')
    try:
        style = result['style']
        img = style.replace('background-image: url(\'', '')
        img = img.replace("');", '')
        return img

    except Exception as e:
        print('error ', e)
        return 'not found'
    
    
        
def scrape_latest_weather_tweet():
    mars_status = api.get_user('@MarsWxReport')
    return mars_status['status']['text']
    
    

def scrape_mars_facts():
    url = 'https://space-facts.com/mars/'
    browser.visit(url)
    time.sleep(1)
    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')
    
    facts_found = soup.find_all('tr', {'class' : re.compile('row*')})
    
    labels = []
    facts = []
    df = pd.DataFrame()
    
    try:
        for fact in facts_found:
            details = fact.find_all('td', {'class' : re.compile('column*')})
            labels.append(details[0].text)
            facts.append(details[1].text)
            
        df['label'] = labels
        df['fact'] = facts
        
    except Exception as e:
        print('error ', e)
        
    return df
        
        

def scrape_hemisphere_images():
    url = 'https://astrogeology.usgs.gov/search/results?q=hemisphere+enhanced&k1=target&v1=Mars'
    browser.visit(url)
    time.sleep(1)
    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')
    astro_url = 'https://astrogeology.usgs.gov'
    
    results = soup.find_all('div', class_='description')

    images = []
    
    try:
        for result in results:
            # look into each hemisphere
            hemis = result.find('a', class_="itemLink")
            # remove some text
            text = hemis.text
            
            # click the url
            browser.visit(astro_url + hemis['href'])
            html = browser.html
            soup = BeautifulSoup(html, 'html.parser')
            href = soup.find('a', {'target' : '_blank'})
           
            # build the data dictionary
            each_image = {'title': text, 'image_url': href['href']}
            images.append(each_image)
            
    except Exception as e:
        print('error ', e)

    return images


def scrape_all():
    # remove old mars data
    planetdetails = db_planetdetails.listings
    planetdetails.drop()
    planetdetails = db_planetdetails.listings

    planetfacts = db_planetfacts.listings
    planetfacts.drop()
    planetfacts = db_planetfacts.listings

    planetimages = db_planetimages.listings
    planetimages.drop()
    planetimages = db_planetimages.listings

    planetdetail = {}
    planetfact = {}
    planetimage = {}

    #1. latest news title and paragraph
    title_para = scrape_latest_news()
    planetdetail['latest_news_title'] = title_para['title']
    planetdetail['latest_news_para'] = title_para['para']

    #2. feature image
    jpl_url = 'https://www.jpl.nasa.gov'
    feature_image_jpg = scrape_feature_image()
    feature_image_url = jpl_url + feature_image_jpg
    planetdetail['feature_image_url'] = feature_image_url
    
    #3. latest weather tweet
    mars_weather = scrape_latest_weather_tweet()
    planetdetail['mars_weather'] = mars_weather

    # add new row to the planetdetails table
    planetdetails.insert_one(planetdetail)

    #4. MARS facts
    mars_facts = scrape_mars_facts()
    for i, row in mars_facts.iterrows():
        planetfact = {'label': row['label'], 'fact': row['fact']}
        planetfacts.insert_one(planetfact)

    #5. high resolution images of each hemisphere
    images = scrape_hemisphere_images()
    for image in images:
        planetimage = {'label': image['title'], 'image_url': image['image_url']}
        planetimages.insert_one(planetimage)
        
    # close the current browser tab
    browser.driver.close()
    