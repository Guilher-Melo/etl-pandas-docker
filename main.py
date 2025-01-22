from src.extract import dataframes
from src.transform import transformation
from src.load import load

def main(dataframes):
    dict_dataframes_transformed = transformation(dataframes)
    load(dict_dataframes_transformed)


if __name__ == '__main__':
    main(dataframes)