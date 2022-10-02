import urllib.request
from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import re
import sqlite3
import datetime
import pandas as pd
import numpy as np
import pycountry
import country_converter as coco

class Citymapper_Data():
    def __init__(self):
        # Ignore SSL certificate errors
        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE
        self.cities = ['Vienna', 'Melbourne', 'Sydney', 'Brussels', 'Saopaulo', 'Montreal', 'Toronto', 'Vancouver', 'Berlin', 'Hamburg', 'Rhineruhr', 'Copenhagen', 'Barcelona', 'Madrid', 'Lyon', 'Paris', 'Hongkong', 'Milan', 'Rome', 'Tokyo', 'Seoul', 'Df', 'Randstad', 'Lisbon', 'Moscow', 'Stpetersburg', 'Stockholm', 'Singapore', 'Istanbul', 'Birmingham', 'London', 'Manchester', 'Boston', 'Chicago', 'Dc', 'La', 'Nyc', 'Philadelphia', 'Seattle', 'Sf', 'World']
        self.countries = ['Hong Kong', 'World', 'Japan', 'South Korea', 'Singapore', 'Italy', 'Germany', 'France', 'United States', 'Austria', 'Australia', 'Belgium', 'Brazil', 'Canada', 'Denmark', 'Spain', 'Mexico', 'Netherlands', 'Portugal', 'Russia', 'Sweden', 'Turkey', 'United Kingdom']

    def retrieve_city_movements(self):
        date, region, move, time=[], [], [], []
        url = 'https://citymapper.com/api/gobot_tab/data'
        html = urlopen(url, context=self.ctx).read()
        soup = BeautifulSoup(html, "html.parser")
        soup=str(soup).replace('        ','').replace('    ','').replace("\n","").replace('false', '0').replace('true', '0').replace('null', '0')
        soup=soup.split('[')[1].split(']')[0]
        data=eval(soup)
        for data in data:
            if data['name'].startswith('s'): continue
            date.append(data['date'])
            region.append(data['region_id'])
            move.append(data['value'])
        citymovement = pd.DataFrame({'date': date,'city': region,'movement': move})
        citymovement = citymovement.replace(np.nan, 0).fillna(0).replace('NaN', 0).replace('nan', 0)
        world_movement = citymovement.groupby([pd.Grouper(key='date')]).mean().sort_values('date').reset_index()
        world_movement['city'] = 'world'
        self.citymovement = pd.concat([citymovement, world_movement])
        self.country_movement = self.citymovement.groupby(['date', self.citymovement["city"].str[:2]]).mean().reset_index()#.describe gives you a bunch of cool stuff
        self.citymovement["city"]=self.citymovement["city"].str[3:]
        self.citymovement["city"] = self.citymovement["city"].replace("ld", "World").str.title()
        self.citymovement=self.citymovement.sort_values(by="date", ascending=True)
        self.country_movement["city"] = self.country_movement["city"].str.upper()
        country_list = self.country_movement["city"].tolist()
        c = []
        for country in country_list:
            if not country in c: c.append(country)
        c.remove('WO')
        c.remove('UK')
        country = coco.convert(names=c, to='name_short')
        map = dict(zip(c,country))
        self.country_movement["country"]= self.country_movement["city"].map(map)
        self.country_movement["country"][self.country_movement["city"]== "WO"] = "World"
        self.country_movement["country"][self.country_movement["city"]== "UK"] = "United Kingdom"
        self.country_movement["movement"] = pd.to_numeric(self.country_movement["movement"], errors="coerce")
        self.country_movement=self.country_movement.sort_values(by="date", ascending=True)
        # print(self.country_movement)
        rolling_average = []
        for city in self.cities:
            df = self.citymovement[self.citymovement["city"] == city]
            df["movement_smooth"] = df["movement"].rolling(window=7).mean()
            rolling_average.append(df)
        self.citymovement = pd.concat(rolling_average)
        rolling_average=[]
        for country in self.countries:
            df = self.country_movement[self.country_movement["country"] == country]
            df["movement_smooth"] = df["movement"].rolling(window=7).mean()
            rolling_average.append(df)
        self.country_movement = pd.concat(rolling_average)

    def save_citimapper_data(self):
        conn = sqlite3.connect('covid.sqlite')
        self.citymovement.to_sql('citymovement', conn, if_exists='replace', index=False)
        self.country_movement.to_sql('country_movement', conn, if_exists='replace', index=False)
        conn.commit()

if __name__ == '__main__':
    # 3 Get Citymapper data
    print('\n\nRunning CityMapper Movement Analysis...\nGenerating Location Data...')
    citymapper_class_object = Citymapper_Data()
    print("Retrieving city data...\nRetrieving country data ...")
    citymapper_class_object.retrieve_city_movements()
    print('Citymapper Data Saving...')
    citymapper_class_object.save_citimapper_data()
    print("\n\nMODEL HAS BEEN UPDATED")