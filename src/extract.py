import pandas as pd
import os


def create_list_of_files(data_path:str) -> list:
    file_list = os.listdir(data_path)

    csv_files = [f for f in file_list if f.endswith('.csv')]
    return csv_files


def create_dataframe(path) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df


def read_files(data_path: str) -> dict:
    dataframes = {}
    csv_files= create_list_of_files(data_path)
    for file in csv_files:
        if file.split('_')[2] == 'dataset.csv':
            df_name = f"df_{file.split('_')[1]}"
        else:
            part_one = file.split('_')[1]
            part_two = file.split('_')[2]
            df_name = f'df_{part_one}_{part_two}'

        path = os.path.join(data_path, file)
        df = create_dataframe(path)
    
        dataframes[df_name] = df
    return dataframes


dataframes = read_files(data_path='./data/')

