from uk_covid19 import Cov19API
import numpy as np
import pandas as pd
import sqlite3
import datetime
from datetime import timedelta
from pandas import ExcelWriter
import warnings
import re
import itertools
from urllib.parse import urlencode
from json import dumps
import requests
import io
warnings.filterwarnings('ignore')

class Covid_Gov_UK():
    def __init__(self):
        Covid_Gov_UK.filters ={"UK Regions":"region", "United Kingdom":"overview", "England":"nation","Scotland":"nation","Wales":"nation","Northern Ireland":"nation","ltla":"ltla"} #TO ITERATE THROUGH (COULD PROBS BE
        self.metrics ={"date":"date",
                       "area":"areaName",
                       "new_cases":"newCasesByPublishDate",
                       "new_deaths":"newDeaths28DaysByPublishDate",
                       'new_hospital_admissions':'newAdmissions',
                       'new_tests':'newTestsByPublishDate',
                       'ventillators_in_use':'covidOccupiedMVBeds',
                       "double_jabbed":"newPeopleVaccinatedSecondDoseByVaccinationDate",
                       "triple_jabbed":"newPeopleVaccinatedThirdInjectionByVaccinationDate"} #COLUMNS for country level data set

        self.metrics_la = {"date":"date",
                           "area":"areaName",
                           "new_cases":"newCasesBySpecimenDate",
                           "new_deaths": "newDeaths28DaysByPublishDate",
                           "tests":"newVirusTestsBySpecimenDate",
                           "double_jabbed":"newPeopleVaccinatedSecondDoseByVaccinationDate",
                           "triple_jabbed":"newPeopleVaccinatedThirdInjectionByVaccinationDate"} #COLUMNS in county data set
        self.blank_g_data = "{'age': '40_to_44', 'rate': 0.0, 'value': 0}, {'age': '5_to_9', 'rate': 0.0, 'value': 0}, {'age': '25_to_29', 'rate': 0.0, 'value': 0}, {'age': '30_to_34', 'rate': 0.0, 'value': 0}, {'age': '80_to_84', 'rate': 0.0, 'value': 0}, {'age': '90+', 'rate': 0.0, 'value': 0}, {'age': '35_to_39', 'rate': 0.0, 'value': 0}, {'age': '75_to_79', 'rate': 0.0, 'value': 0}, {'age': '20_to_24', 'rate': 0.1, 'value': 1}, {'age': '15_to_19', 'rate': 0.0, 'value': 0}, {'age': '85_to_89', 'rate': 0.0, 'value': 0}, {'age': '65_to_69', 'rate': 0.0, 'value': 0}, {'age': '45_to_49', 'rate': 0.0, 'value': 0}, {'age': '0_to_4', 'rate': 0.0, 'value': 0}, {'age': '60_to_64', 'rate': 0.0, 'value': 0}, {'age': '70_to_74', 'rate': 0.0, 'value': 0}, {'age': '10_to_14', 'rate': 0.0, 'value': 0}, {'age': '50_to_54', 'rate': 0.0, 'value': 0}, {'age': '55_to_59', 'rate': 0.0, 'value': 0}"

        self.nations = []
        self.nations_concat = []


        self.metrics_m, self.metrics_f = {'male_cases':'maleCases'},{'female_cases':'femaleCases'}


        self.metrics_g =[self.metrics_m, self.metrics_f]
        self.update_headings ={'newCasesByPublishDate' : 'new_cases',
                               'newDeaths28DaysByPublishDate' : 'new_deaths',
                               'newAdmissions' : 'new_admissions',
                               'newTestsByPublishDate' : 'tests',
                               'covidOccupiedMVBeds':'ventillators_in_use',
                               'maleCases':'male_cases',
                               'femaleCases':'female_cases',
                               "newCasesByPublishDate":"new_cases",
                               "newCasesBySpecimenDate":"new_cases"}
        self.age_buckets =['0_to_4',
                            '5_to_9',
                            '10_to_14',
                            '15_to_19',
                            '20_to_24',
                            '25_to_29',
                            '30_to_34',
                            '35_to_39',
                            '40_to_44',
                            '45_to_49',
                            '50_to_54',
                            '55_to_59',
                            '60_to_64',
                            '65_to_69',
                            '70_to_74',
                            '75_to_79',
                            '80_to_84',
                            '85_to_89',
                            '90+']
        
    def get_data(self):
        ##full column data
        if Covid_Gov_UK.area_name == "UK Regions":
            # pass
            api = Cov19API(
                filters=[f"areaType={Covid_Gov_UK.area_type}"],
                structure=self.metrics)
            self.data = api.get_dataframe()

        #county level cases, deaths, tests, vaccinations

        elif Covid_Gov_UK.area_name == "ltla": #proper api data gathering - not in a nice df for me already :(((
            pass
            filters = [f"areaType={Covid_Gov_UK.area_type}"]
            structure = self.metrics_la

            api_params = {
                "filters": str.join(";", filters),
                "structure": dumps(structure, separators=(",", ":"))}
            # print(api_params)

            dfs = []
            for n in range(1,1000): #up to 38
                try:

                    api_params["page"] = n
                    endpoint = "https://api.coronavirus.data.gov.uk/v1/data"
                    response = requests.get(endpoint, params=api_params, timeout=20)
                    self.data = response.text.replace("null", "None") #so ic ould evaluate
                    self.data = eval(self.data)["data"]
                    df = pd.DataFrame(self.data)
                    pd.to_numeric(df["new_cases"], errors="coerce")
                    df["7 days ago cases"] = df["new_cases"].shift(periods=-7, axis = 0)
                        # print(df)
                    dfs.append(df)

                except:
                    print(f"No. Pages: {n}")
                    break
            self.data = pd.concat(dfs)
        else:
            api = Cov19API(filters=[
                f"areaType={Covid_Gov_UK.area_type}",
                f"areaName={Covid_Gov_UK.area_name}",
            ],
            structure=self.metrics
            )
            self.data = api.get_dataframe()


    def clean_data(self):
        for k,v in self.update_headings.items():
            try: self.data.rename(columns={k:v}, inplace=True)
            except: continue

        for column in self.data.columns[2:]:
            self.data[column]=self.data[column].apply(pd.to_numeric, errors='ignore').fillna(0)
        self.data['date'] = pd.to_datetime(self.data['date'])
        self.main_table = self.data
        count = []
        dfs = []
        for cty in self.main_table["area"].to_list():
            if cty not in count: count.append(cty)
        if len(count) > 1:
            for cty in count:
                df = self.main_table[self.main_table["area"]==cty]
                df["deaths T - 3 weeks"] = df["new_deaths"].shift(periods=10, axis = 0)
                dfs.append(df)
        else:
            df = self.main_table[self.main_table["area"]==count[0]]
            df["deaths T - 3 weeks"] = df["new_deaths"].shift(periods=13, axis = 0)
            dfs.append(df)
        self.main_table = pd.concat(dfs)

    def calculated_columns(self):
        self.main_table['new_cases(k)']=self.main_table['new_cases']/1000
        self.main_table['death rate %']=(self.main_table['new_deaths']/self.main_table['new_cases'])*100
        self.main_table['death rate % - T - 3 weeks']=(self.main_table['deaths T - 3 weeks']/self.main_table['new_cases'])*100
        self.main_table["2nd jab/case ratio"] =  self.main_table["double_jabbed"]/self.main_table['new_cases']
        self.main_table["3rd jab/case ratio"] =  self.main_table["triple_jabbed"]/self.main_table['new_cases']

        try:
            self.main_table['tests(M)']=self.main_table['new_tests']/1000000
            self.main_table['positivity rate %']=(self.main_table['new_cases']/self.main_table['new_tests'])*100
            self.main_table["7 days positivity rate %"] = self.main_table["positivity rate %"].shift(periods=-7, axis = 0)
            self.main_table['hospital death rate %']=(self.main_table['new_deaths']/self.main_table['new_hospital_admissions'])*100
            self.main_table['hospitalisation rate %']=(self.main_table['new_hospital_admissions']/self.main_table['new_cases'])*100
            self.main_table['ventilation rate %']=(self.main_table['ventillators_in_use']/self.main_table['new_hospital_admissions'])*100
            self.main_table=self.main_table.replace([np.inf, -np.inf], np.nan)
        except Exception as e: print(e)
        u=[]
        dfs = []
        for cty in self.main_table["area"].to_list():
            if cty not in u: u.append(cty)
        self.main_table=self.main_table.sort_values(by=['date'])
        for cty in u:
            df = self.main_table[self.main_table["area"] == cty]
            for column in df.drop(columns=['date','area']):
                df[column] = pd.to_numeric(df[column], errors="coerce")
                df[f"{column} 7da"] = df[column].rolling(window=7).mean()
                if Covid_Gov_UK.area_name == "ltla":
                    df[f"{column} %"] = df[column].pct_change() * 100

            dfs.append(df)

        self.main_table = pd.concat(dfs)

        if Covid_Gov_UK.area_name != "ltla":
            self.main_table["Wumis R Rate"] = self.main_table["positivity rate % 7da"]/self.main_table["7 days positivity rate % 7da"]
        if Covid_Gov_UK.area_name == "ltla":
            self.county_list = self.data["area"].drop_duplicates().to_list()
            # print(self.county_list)
            self.main_table["Wumis R Rate"] = 0
            # print(self.main_table.head())
            for county in self.county_list:
                self.main_table["Wumis R Rate"][self.main_table["area"] == county] = self.main_table["new_cases 7da"][self.main_table["area"] == county] / self.main_table["7 days ago cases 7da"][self.main_table["area"] == county]
            # df = pd.read_excel("ltla_map.xlsx", header=None, sheet_name="map")
            df = pd.read_csv("DataSources/ltla_map.csv", header=None)
            df[1] = pd.to_numeric(df[1], errors = "coerce")
            # print(df)
            map = dict(zip(df[0],df[1]))
            self.main_table["population"] = self.main_table["area"].map(map)
            self.main_table["population"] = pd.to_numeric(self.main_table["population"], errors="coerce")
            self.main_table["new_cases_per_1m"] = self.main_table["new_cases 7da"]/self.main_table["population"]*1000000
            self.main_table["Area_UK"] = self.main_table["area"] + ", GB"
        print(self.main_table.head())
        print(len(self.main_table))

        self.nations.append(self.main_table)

    def data_save(self):
        conn = sqlite3.connect('covid.sqlite')
        cur = conn.cursor()
        self.main_table.to_sql(f'covid_uk_daily: {Covid_Gov_UK.area_name}', conn, if_exists='replace', index=False)
        print(f"UK Data lens: {len(self.nations)}")
        if len(self.nations) == 6:
            nations = pd.concat(self.nations[2:5])
            nations.to_sql('covid_uk_daily: GB', conn, if_exists='replace', index=False)

    def get_gender_data(self):
        daily = []
        for item in self.metrics_g:
            api = Cov19API(filters=[
                f"areaType=nation",
                f"areaName=England",
            ],
            structure=item
            )
            dfs = []
            self.data_g = api.get_dataframe()
            print(self.data_g.columns)
            # self.data_g.replace("[]", self.blank_g_data, inplace=True)
            self.data_g.to_excel("DataSources/fgdf.xlsx")
            n=0
            for c in self.data_g.values.tolist():
                if len(c[0]) < 4: c[0] = eval(self.blank_g_data)
                df = pd.DataFrame(c[0])
                dfs.append(df)
                n += 1
            m_table = pd.concat(dfs)
            print(n)
            m_table.to_excel("DataSources/gendertable.xlsx")
            m_table["gender"] = re.split("'", str(item))[1]
            datelist = pd.date_range(end=datetime.datetime.today().date()-timedelta(1), periods=len(self.data_g)).to_list()

            datelist=datelist[::-1]
            dates = list(itertools.repeat(datelist, 19))

            dates = list(itertools.chain.from_iterable(dates))
            print(len(dates))
            dates=pd.DataFrame(dates).sort_values(by=0, ascending=False)
            dates[0]=dates[0].astype(str)
            print(len(m_table))
            m_table=m_table.set_index(dates[0])
            m_table=m_table.sort_values(by=0, ascending=True)
            for bucket in self.age_buckets:
                df = m_table[m_table["age"] == bucket]
                df["daily cases"] = df["value"].diff()
                df["daily cases 7da"] = df["daily cases"].rolling(window=7).mean()
                df["Cases d%"] = df["daily cases"].pct_change() * 100
                df["Cases d% "] = df["Cases d%"].rolling(window=7).mean()
                daily.append(df)
        m_table = pd.concat(daily)
        m_table.rename(columns={"value":"cum cases"}, inplace=True)
        m_table["age"] = m_table["age"].str.replace("_", " ")
        m_table["gender"] = m_table["gender"].str.replace("maleCases", "male").str.replace("femaleCases", "female")

        df = pd.read_csv("DataSources/Age population brackets.csv" , header=None).iloc[12:31][[0,4]]
        df[4] = df[4].str.replace(",","")
        df[4] = pd.to_numeric(df[4], errors = "coerce")
        df[0] = df[0].str.replace(" ", "").str.replace("-", " to ").str.replace("andover","+")
        dfm = df.copy(deep=True)
        dfm[4] = dfm[4] * 0.494
        dfm["gender"] = "male"
        dff = df.copy(deep=True)
        dff[4] = dff[4] * 0.506
        dff["gender"] = "female"
        dfc = pd.concat([dfm,dff])
        age_brackets = m_table["age"].drop_duplicates().to_list()
        mapp = dict(dfc[[0,4]].values)
        m_table["group population"] = m_table["age"].map(mapp).combine_first(m_table["gender"].map(mapp))
        m_table["case rate per 1m"] = m_table["daily cases 7da"]/m_table["group population"]*1e6
        m_table["7 days ago cases"] = 0
        for age in age_brackets:
            m_table["7 days ago cases"][m_table["age"]==age] = m_table["daily cases 7da"][m_table["age"]==age].shift(periods=7, axis = 0)
        m_table["Wumis R Rate"] = m_table["daily cases 7da"]/m_table["7 days ago cases"]
        print(m_table.tail(10))
        conn = sqlite3.connect('covid.sqlite')
        m_table.to_sql(f'covid_uk_daily cases by age ', conn, if_exists='replace')

    def save_xls(self, list_dfs, xls_path):
        self.list_dfs = []
        self.list_dfs.append(self.main_table)
        self.list_dfs.append(self.weekly_table)
        self.xls_path = 'covid_uk.xlsx'
        with ExcelWriter(self.xls_path) as writer:
            for n, df in enumerate(self.list_dfs):
                df.to_excel(writer,'sheet%s' % n)
            writer.save()


if __name__ == '__main__':
    print("\n\nRunning Covid UK Analysis (GovUK)...")
    covid_object = Covid_Gov_UK()
    l = 0
    for Covid_Gov_UK.area_name,Covid_Gov_UK.area_type in Covid_Gov_UK.filters.items():
        print(f"Running Covid Analysis for: {Covid_Gov_UK.area_name}...")
        covid_object.get_data()
        covid_object.clean_data()
        covid_object.calculated_columns()
        covid_object.data_save()
        
    covid_object.get_gender_data()
    print("Additonal Metrics have been added")
    print("Gov UK Data Saving...")
