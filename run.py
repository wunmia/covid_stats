from covid_world import Infometer_Data
from covid_uk import Covid_Gov_UK
from movement import Citymapper_Data

def run():
    # 1 Get the UK Stats
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
    infometer_class_object.positivity()
    infometer_class_object.country_case_rate()
    print('\nInfometer Data Saving...')
    infometer_class_object.save_infometer_data()

    # 3 Get Citymapper data
    #depreciated due to city mapper api going off the grid
    print('\n\nRunning CityMapper Movement Analysis...\nGenerating Location Data...')
    citymapper_class_object = Citymapper_Data()
    print("Retrieving city data...\nRetrieving country data ...")
    citymapper_class_object.retrieve_city_movements()
    print('Citymapper Data Saving...')
    citymapper_class_object.save_citimapper_data()
    print("\n\nMODEL HAS BEEN UPDATED")

if __name__ == '__main__':
    run()
