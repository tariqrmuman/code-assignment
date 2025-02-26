import pandas as pd
import numpy as np
import random
from datetime import datetime
import os 

if __name__ == "__main__" :
        
    # List of sample names to choose from
    sample_names = [
        "Emma", "Liam", "Olivia", "Noah", "Ava", "Oliver", "Isabella", "Elijah",
        "Sophia", "William", "Mia", "James", "Charlotte", "Benjamin", "Amelia",
        "Lucas", "Harper", "Mason", "Evelyn", "Ethan", "Abigail", "Michael",
        "Emily", "Alexander", "Elizabeth", "Daniel", "Sofia", "Matthew", "Avery",
        "Henry", "Ella", "Jackson", "Madison", "Sebastian", "Scarlett"
    ]

    # Generate 20 random names
    random_names = random.sample(sample_names, 20)

    # Create a list that repeats each name 10 times to get 200 total entries
    repeated_names = []
    for name in random_names:
        repeated_names.extend([name] * 100)

    # Generate 200 random times between 1.00 and 6.00
    random_times = np.round(np.random.uniform(1, 6, 2000), 2)

    # Create DataFrame
    race_times = pd.DataFrame({
        'names': repeated_names,
        'times': random_times
    })

    #create avg race times and sort ascending
    agg_race_times = race_times.groupby('names').mean().sort_values(by='times',ascending=True)
    race_times.groupby('names').mean().sort_values(by='times',ascending=True)

    #select the top 3 racers by avg lap time 
    agg_race_times = agg_race_times.head(3)
    agg_race_times.columns = ['avg_lap_time']
    race_times = race_times[race_times['names'].isin( list(agg_race_times.index) )]

    #add extra columns as directed 
    agg_race_times['fastest_lap_time'] =  race_times.groupby('names').min() 
    agg_race_times['slowest_lap_time'] =  race_times.groupby('names').max() 

    #write requested output to current directory 
    formatted_time = now.strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.getcwd()+ '\\f1_race_times_simulated_' + formatted_time + '.csv'
    print('writing simulated results to' +' ' +filename )
    agg_race_times.to_csv(filename, index=True, encoding='utf-8')
