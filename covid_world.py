import urllib.request
from bs4 import BeautifulSoup
import re
import sqlite3
import datetime
import pandas as pd
import numpy as np

'''This class scrapes and cleans table data from the https://www.worldometers.info/coronavirus/ site'''
class Infometer_Data():
    def __init__(self):
        Infometer_Data.alldates=pd.date_range(start='2022-01-22', end=datetime.datetime.today())
        Infometer_Data.country_count = 200

    '''this method collects the names of all of the countries whose data is posted on the site, this is so the program can quickly iterate through URLs later to pull individual country data'''
    def retrieve_countries(self):
        print('Creating Website Connection...')
        a=[]
        url = 'https://www.worldometers.info/coronavirus/'
        page=urllib.request.Request(url,headers={'User-Agent': 'Mozilla/5.0'})
        infile=urllib.request.urlopen(page).read()
        data = infile.decode('ISO-8859-1')
        soup = BeautifulSoup(data, "html.parser")
        count=soup.text.split('\n\nAll\n\n')[1].split('\n\n\n\nTotal')[0].split('\n')
        for line in count:
            if line.startswith(('Eur','North Am','Afri','Asia','N/A','South Am','Australia/Oce')):continue
            if re.match("^[a-zA-Z]", line): a.append(line)
        a.insert(0, a.pop())
        Infometer_Data.countries = a
        for country in Infometer_Data.countries:
            area=country.replace(' ','').replace('.','').replace('-','')

    '''This method takes each of the countries on the infometer coronavirus front page and iterate rates through there sites (website path + '/country_name')'''
    def store_webpages(self):
        print('\nStoring Worldometers data locally\n')
        n=-1
        broken_urls = []
        Infometer_Data.country_pages ={}
        Infometer_Data.analysed_countries = []
        for country in Infometer_Data.countries:
            n=n+1
            if n==Infometer_Data.country_count: break
            try:
                url = 'https://www.worldometers.info/coronavirus/country/'+country.replace('Hong Kong','china-hong-kong-sar').replace('USA','US').replace('UAE','united-arab-emirates').replace('S.','South').replace('Czechia','czech-republic').replace('North Macedonia','macedonia').replace(' ','-')
                page=urllib.request.Request(url,headers={'User-Agent': 'Mozilla/5.0'})
                infile=urllib.request.urlopen(page).read()
                data = infile.decode('ISO-8859-1')
                soup = BeautifulSoup(data, "html.parser")
                webpage = soup.decode('ISO-8859-1')
                with open("main.html", "w", encoding="utf-8") as f:
                    f.write(webpage)
                if country == "SKorea": country = "South Korea"
                Infometer_Data.country_pages[country] = soup
                Infometer_Data.analysed_countries.append(country)
            except: broken_urls.append(country)
        print('Samples: ', Infometer_Data.analysed_countries, '\n\nBroken links: ', broken_urls, sep = '')

    '''Each country specific page has a similar structure, as such the core data points are extracted for each one - this method in particular gets the dates of the available data'''
    def date(self):
        soup = Infometer_Data.country_pages[Infometer_Data.country]
        dates=eval(str(soup).split(' categories: ')[1].split('\n')[0].split('        },')[0])
        clean_date = []
        for n, date in enumerate(dates):
            date=datetime.datetime.strptime(date, '%b %d, %Y').strftime('%Y-%m-%d')
            clean_date.append(date)
        self.clean_date = clean_date

    '''The next set of script do the same thing instread retrieving the data points mentioned in the method names'''
    def population_data(self):
        url = "https://www.worldometers.info/world-population/population-by-country/"
        page=urllib.request.Request(url,headers={'User-Agent': 'Mozilla/5.0'})
        infile=urllib.request.urlopen(page).read()
        data = infile.decode('ISO-8859-1')
        soup = BeautifulSoup(data, "html.parser")
        rank, country, population, growth_rate, growth_abs, pop_dens, area, net_migrants, fert_rate, med_age, urban_pop, world_share = [], [], [], [], [], [], [], [], [], [] , [], []
        table_rows = soup.find_all("tr")
        for row in table_rows:
            cells = row.find_all("td")
            cells = [cell.text for cell in cells]
            if not cells:
                continue
            [column.append(value) for column,value in zip([rank, country, population, growth_rate, growth_abs, pop_dens, area, net_migrants, fert_rate, med_age, urban_pop, world_share], cells)]
        population_df = pd.DataFrame({"rank":rank, "country":country, "population":population, "growth_rate":growth_rate, "growth_abs":growth_abs, "pop_dens":pop_dens, "area":area, "net_migrants":net_migrants, "fert_rate":fert_rate, "med_age":med_age, "urban_pop":urban_pop, "world_share":world_share})
        percentage_columns = ["growth_rate", "urban_pop", "world_share"]
        for column in population_df.columns.drop("country"):
            population_df[column] = population_df[column].str.replace("N.A.", "0")
            population_df[column] = population_df[column].str.replace(",", "")
            population_df[column] = pd.to_numeric(population_df[column].str.strip(' %'))
            population_df[column] = population_df[column].fillna(0)
        for column in percentage_columns:
            population_df[column] = population_df[column]/100
        population_df = population_df.replace("Inf", 0)
        Infometer_Data.population_table = population_df
    def cumulative_data(self):
        url = "https://www.worldometers.info/coronavirus/"
        page=urllib.request.Request(url,headers={'User-Agent': 'Mozilla/5.0'}) #work aro
        infile=urllib.request.urlopen(page).read()
        data = infile.decode('ISO-8859-1')
        soup = BeautifulSoup(data, "html.parser")

        rank, country, cases, n_cases, deaths, n_deaths, recovered, n_recovered, active_cases, critcal, cases_1m, deaths_1m, tests, tests_1m, population, continent, cases_p1, death_p1, test_p1 = [], [], [], [], [], [], [], [], [], [] , [], [], [], [], [], [], [], [], []
        table_rows = soup.find_all("tr")
        for n, row in enumerate(table_rows):
            cells = row.find_all("td")
            cells = [cell.text for cell in cells]
            if not cells:
                continue
            [column.append(value) for column,value in zip([rank, country, cases, n_cases, deaths, n_deaths, recovered, n_recovered, active_cases, critcal, cases_1m, deaths_1m, tests, tests_1m, population, continent, cases_p1, death_p1, test_p1], cells)]

        cumulative_df = pd.DataFrame({"rank":rank, "country":country, "cases":cases, "new_cases":n_cases, "deaths":deaths, "new_deaths":n_deaths, "recovered":recovered, "new_recovered":n_recovered, "active_cases":active_cases, "critcal":critcal, "cases_per_million":cases_1m, "deaths_per_million":deaths_1m, "tests":tests, "tests_per_million":tests_1m,  "population":population,  "continent":continent, "people_per_case":cases_p1, "people_per_death":death_p1, "people_per_test":test_p1})
        cumulative_df = cumulative_df.shift(-8)

        for column in cumulative_df.columns.drop(["country", "continent"]):
            cumulative_df[column] = cumulative_df[column].str.replace("N/A", "0")
            cumulative_df[column] = cumulative_df[column].str.replace("+", "")
            cumulative_df[column] = cumulative_df[column].str.replace("\n", "")
            cumulative_df[column] = cumulative_df[column].str.replace(",", "")
            cumulative_df[column] = cumulative_df[column].str.replace(" ", "")
            cumulative_df[column] = pd.to_numeric(cumulative_df[column])#, errors='coerce')
            cumulative_df[column] = cumulative_df[column].fillna(0) #fillna only works with numeric data
        cumulative_df["deaths_per_million"] = (cumulative_df["deaths"]/cumulative_df["population"])*1000000
        cumulative_df = cumulative_df.groupby(['country', 'continent']).mean().reset_index()
        cumulative_df = cumulative_df.replace("inf", 0)
        cumulative_df["country"] = cumulative_df["country"].str.replace("S. Korea", "South Korea")
        Infometer_Data.cumulative_table = cumulative_df
    def retrieve_total_death_data(self):
        country = Infometer_Data.country
        if country == 'China':
            Infometer_Data.total_death_table=Infometer_Data.alldates.to_frame()#index=True)
            Infometer_Data.total_death_table.rename(columns={0:'date'}, inplace=True) #integers not strings!
        soup = Infometer_Data.country_pages[country]
        data=eval(str(soup).split("name: 'Cases'")[1].split("data: ")[1].split('      ')[0].replace('null','0'))
        mptable=dict(zip(self.clean_date, data))
        country=country.replace(' ','').replace('.','')
        Infometer_Data.total_death_table[country]=Infometer_Data.total_death_table['date'].astype(str).map(mptable)
        Infometer_Data.total_death_table=Infometer_Data.total_death_table.replace(np.nan, 0).fillna(0).replace('NaN', 0).replace('nan', 0)
        Infometer_Data.total_death_table['date']=Infometer_Data.total_death_table.index
    def retrieve_death_data(self):
        country = Infometer_Data.country
        if country == 'China':
            Infometer_Data.daily_deaths_table=Infometer_Data.alldates.to_frame()#index=True)
            Infometer_Data.daily_deaths_table.rename(columns={0:'date'}, inplace=True) #integers not strings!
        soup = Infometer_Data.country_pages[country]
        data=eval(str(soup).split("name: 'Daily Deaths'")[1].split("data: ")[1].split('      ')[0].replace('null','0'))
        mptable=dict(zip(self.clean_date, data))
        country=country.replace(' ','').replace('.','')
        Infometer_Data.daily_deaths_table[country]=Infometer_Data.daily_deaths_table['date'].astype(str).map(mptable)
        Infometer_Data.daily_deaths_table=Infometer_Data.daily_deaths_table.replace(np.nan, 0).fillna(0).replace('NaN', 0).replace('nan', 0)
        Infometer_Data.daily_deaths_table['date']=Infometer_Data.daily_deaths_table.index
        Infometer_Data.daily_deaths_table[country] = Infometer_Data.daily_deaths_table[country].rolling(window=7).mean()
    def retrieve_death_three_d_data(self):
        country = Infometer_Data.country
        if country == 'China':
            Infometer_Data.daily_deaths_three_d=Infometer_Data.alldates.to_frame()
            Infometer_Data.daily_deaths_three_d.rename(columns={0:'date'}, inplace=True)
        soup = Infometer_Data.country_pages[country]
        data=eval(str(soup).split("name: '3-day moving average'")[2].split("data: ")[1].split(',\n')[0].replace('null','0'))
        mptable=dict(zip(self.clean_date, data))
        country=country.replace(' ','').replace('.','')
        Infometer_Data.daily_deaths_three_d[country]=Infometer_Data.daily_deaths_three_d['date'].astype(str).map(mptable)
        Infometer_Data.daily_deaths_three_d=Infometer_Data.daily_deaths_three_d.replace(np.nan, 0).fillna(0).replace('NaN', 0).replace('nan', 0)
        Infometer_Data.daily_deaths_three_d['date']=Infometer_Data.daily_deaths_three_d.index
    def retrieve_death_seven_d_data(self):
        country = Infometer_Data.country
        if country == 'China':
            Infometer_Data.daily_deaths_seven_d=Infometer_Data.alldates.to_frame()#index=True)
            Infometer_Data.daily_deaths_seven_d.rename(columns={0:'date'}, inplace=True) #integers not strings!
        soup = Infometer_Data.country_pages[country]
        data=eval(str(soup).split("name: '7-day moving average'")[2].split("data: ")[1].split(',\n')[0].replace('null','0'))
        mptable=dict(zip(self.clean_date, data))
        country=country.replace(' ','').replace('.','')
        Infometer_Data.daily_deaths_seven_d[country]=Infometer_Data.daily_deaths_seven_d['date'].astype(str).map(mptable)
        Infometer_Data.daily_deaths_seven_d=Infometer_Data.daily_deaths_seven_d.replace(np.nan, 0).fillna(0).replace('NaN', 0).replace('nan', 0)
        Infometer_Data.daily_deaths_seven_d['date']=Infometer_Data.daily_deaths_seven_d.index
    def retrieve_death_rate_data(self):
        country = Infometer_Data.country
        if country == 'China':
            Infometer_Data.death_rate_table=Infometer_Data.alldates.to_frame()#index=True)
            Infometer_Data.death_rate_table.rename(columns={0:'date'}, inplace=True) #integers not strings!
        try:
            soup = Infometer_Data.country_pages[country]
            data=eval(str(soup).split("name: 'Death Rate'")[1].split("data: ")[1].split('      ')[0].replace('null','0'))
            mptable=dict(zip(self.clean_date, data))
            country=country.replace(' ','').replace('.','')
            Infometer_Data.death_rate_table[country]=Infometer_Data.death_rate_table['date'].astype(str).map(mptable)
            Infometer_Data.death_rate_table=Infometer_Data.death_rate_table.replace(np.nan, 0).fillna(0).replace('NaN', 0).replace('nan', 0)
            Infometer_Data.death_rate_table['date']=Infometer_Data.death_rate_table.index
        except: pass
    def retrieve_recovery_rate_data(self):
        country = Infometer_Data.country
        if country == 'China':
            Infometer_Data.recovery_rate_table=Infometer_Data.alldates.to_frame()#index=True)
            Infometer_Data.recovery_rate_table.rename(columns={0:'date'}, inplace=True) #integers not strings!
        try:
            soup = Infometer_Data.country_pages[country]
            data=eval(str(soup).split("name: 'Recovery Rate'")[1].split("data: ")[1].split('      ')[0].replace('null','0'))
            mptable=dict(zip(self.clean_date, data))
            country=country.replace(' ','').replace('.','')
            Infometer_Data.recovery_rate_table[country]=Infometer_Data.recovery_rate_table['date'].astype(str).map(mptable)
            Infometer_Data.recovery_rate_table=Infometer_Data.recovery_rate_table.replace(np.nan, 0).fillna(0).replace('NaN', 0).replace('nan', 0)
            Infometer_Data.recovery_rate_table['date']=Infometer_Data.recovery_rate_table.index
        except: pass
    def retrieve_daily_cases_data(self):
        country = Infometer_Data.country
        if country == 'China':
            Infometer_Data.daily_cases_table=Infometer_Data.alldates.to_frame()#index=True)
            Infometer_Data.daily_cases_table.rename(columns={0:'date'}, inplace=True) #integers not strings!
        try:
            soup = Infometer_Data.country_pages[country]
            data=eval(str(soup).split("name: \'Daily Cases\',")[1].split("data: ")[1].split('         ')[0].replace('null','0'))
            mptable=dict(zip(self.clean_date, data))
            country=country.replace(' ','').replace('.','')
            Infometer_Data.daily_cases_table[country]=Infometer_Data.daily_cases_table['date'].astype(str).map(mptable)
            Infometer_Data.daily_cases_table=Infometer_Data.daily_cases_table.replace(np.nan, 0).fillna(0).replace('NaN', 0).replace('nan', 0)
            Infometer_Data.daily_cases_table['date']=Infometer_Data.daily_cases_table.index
            Infometer_Data.daily_cases_table[country] = Infometer_Data.daily_cases_table[country].rolling(window=7).mean()
        except: pass


    '''Some derived data was also calculated in this section, as this metrics were developed early on some suimplofying assumptions were made, i.e. the death rate is fair constant across different countries, therefore you could assess the efficacy of testing regimes by looking at the ratio of cases to deaths'''
    def testing_performance(self):
        Infometer_Data.testing_performance = Infometer_Data.daily_cases_table.sum()/Infometer_Data.daily_deaths_table.sum()
        Infometer_Data.testing_performance = Infometer_Data.testing_performance.reset_index().rename(columns={'index':'country', 0 : 'testing_performance'}) #integers not strings!
        row = ['World', Infometer_Data.testing_performance['testing_performance'].mean()]
        Infometer_Data.testing_performance.loc[len(Infometer_Data.testing_performance)] = row

    def country_case_rate(self):
        Infometer_Data.daily_cases_table.rename(columns={"SKorea":'South Korea'}, inplace=True)
        pop_map = Infometer_Data.cumulative_table[["country", "population"]].drop_duplicates()
        pop_map = dict(zip(Infometer_Data.cumulative_table["country"],Infometer_Data.cumulative_table["population"]))
        Infometer_Data.daily_cases_table = pd.melt(Infometer_Data.daily_cases_table, id_vars='date', value_vars=Infometer_Data.daily_cases_table.columns.drop('date').tolist(), value_name = 'cases', var_name = 'country')
        Infometer_Data.daily_cases_table["country"] = Infometer_Data.daily_cases_table["country"].str.replace(" ", "").str.replace("(?<=[a-z])(?=[A-Z])", " ").str.replace("aa", "a a").str.replace("dadand", "dad and").str.replace("eof", "e of")
        Infometer_Data.daily_deaths_table =  pd.melt(Infometer_Data.daily_deaths_table, id_vars='date', value_vars=Infometer_Data.daily_deaths_table.columns.drop('date').tolist(), value_name = 'deaths', var_name = 'country')
        Infometer_Data.daily_deaths_table["country"] = Infometer_Data.daily_deaths_table["country"].str.replace(" ", "").str.replace("(?<=[a-z])(?=[A-Z])", " ").str.replace("aa", "a a").str.replace("dadand", "dad and").str.replace("eof", "e of")
        Infometer_Data.dc_melt = Infometer_Data.daily_cases_table
        Infometer_Data.dc_melt["population"] = Infometer_Data.dc_melt["country"].map(pop_map)
        print(Infometer_Data.dc_melt["country"][Infometer_Data.dc_melt["population"].isnull()].drop_duplicates())
        Infometer_Data.dc_melt = Infometer_Data.dc_melt[~Infometer_Data.dc_melt["population"].isnull()]
        Infometer_Data.dc_melt["case_rate_1M"] = 1e6 * Infometer_Data.dc_melt["cases"]/Infometer_Data.dc_melt["population"]
        Infometer_Data.country_df = Infometer_Data.dc_melt["country"].drop_duplicates()

    def save_infometer_data(self):
        conn = sqlite3.connect('covid.sqlite')
        Infometer_Data.total_death_table.to_sql('total_death_table', conn, if_exists='replace', index=False)
        Infometer_Data.daily_deaths_table.to_sql('daily_deaths_table', conn, if_exists='replace', index=False)
        Infometer_Data.daily_deaths_three_d.to_sql('deaths_3dra', conn, if_exists='replace', index=False)
        Infometer_Data.daily_deaths_seven_d.to_sql('deaths_7dra', conn, if_exists='replace', index=False)
        Infometer_Data.death_rate_table.to_sql('death_rate_table', conn, if_exists='replace', index=False)
        Infometer_Data.recovery_rate_table.to_sql('recovery_rate_table', conn, if_exists='replace', index=False)
        Infometer_Data.daily_cases_table.to_sql('daily_cases_table', conn, if_exists='replace', index=False)
        Infometer_Data.testing_performance.to_sql('mortality_rate_table', conn, if_exists='replace', index=False)
        Infometer_Data.population_table.to_sql('population_table', conn, if_exists='replace', index=False)
        Infometer_Data.cumulative_table.to_sql('cumulative_table', conn, if_exists='replace', index=False)
        Infometer_Data.dc_melt.to_sql('global_rates_table', conn, if_exists='replace', index=False)
        Infometer_Data.country_df.to_sql('country_names_table', conn, if_exists='replace', index=False)
        conn.commit()

if __name__ == '__main__':
    # 2 Get the world stats
    print("\n\nCovid World Analysis\n")
    infometer_class_object = Infometer_Data()
    infometer_class_object.retrieve_countries()
    infometer_class_object.store_webpages()
    print("\nRunning Infometer Aggregate Analysis...")
    infometer_class_object.population_data()
    infometer_class_object.cumulative_data()
    print("\nRunning Infometer Main Analysis...\n")
    bad_data = []
    for Infometer_Data.country in Infometer_Data.analysed_countries:
        try:
            infometer_class_object.date()
            infometer_class_object.retrieve_death_data()
            infometer_class_object.retrieve_total_death_data()
            infometer_class_object.retrieve_death_three_d_data()
            infometer_class_object.retrieve_death_seven_d_data()
            infometer_class_object.retrieve_death_rate_data()
            infometer_class_object.retrieve_recovery_rate_data()
            infometer_class_object.retrieve_daily_cases_data()
        except:
            print(f"{Infometer_Data.country} did not contain complete data")
            bad_data.append(Infometer_Data.country)
            continue
    for country in bad_data:
        Infometer_Data.analysed_countries.remove(country)
    infometer_class_object.testing_performance()
    infometer_class_object.country_case_rate()
    print('\nInfometer Data Saving...')
    infometer_class_object.save_infometer_data()