import pandas as pd
df = pd.DataFrame.from_dict({
    'Gender': ['Female', 'Female', 'Female', 'Male', 'Male', 'Male', 'Male'],
    'Name': ['Jane', 'Kate', 'Melissa', 'Nik', 'Evan', 'Doug', 'Joe'],
    'Age': [10, 35, 34, 23, 70, 55, 89],
    'Height': [130, 178, 155, 133, 195, 150, 205],
    'Weight': [80, 200, 220, 150, 140, 95, 180]
}).set_index(['Gender', 'Name'])

print(df)