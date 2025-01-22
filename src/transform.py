import src.extract as extract
import pandas as pd
from datetime import date, timedelta

dataframes = extract.read_files('./data/')


def create_merge(df_1: pd.DataFrame, df_2: pd.DataFrame, type_merge:str,
                column_merge:str, columns_df2: list = None) -> pd.DataFrame:
    if columns_df2:
        df_merge = df_1.merge(
            df_2[columns_df2],
            how=type_merge,
            on=column_merge
        )
        return df_merge
    else:
        df_merge = df_1.merge(
            df_2,
            how=type_merge,
            on=column_merge
        )
        return df_merge


def create_inconsistency_df(df:pd.DataFrame, column) -> pd.DataFrame:
    df_invalids = df[df[column].isna()]
    return df_invalids


def create_id(df: pd.DataFrame, column_name:str) -> pd.DataFrame:
    new_df = df.copy()
    df_id = range(1, int(len(df) + 1))
    new_df.insert(0, column_name, df_id)
    return new_df


def replace_id(df:pd.DataFrame, original_id:str, new_id_name: str) -> pd.DataFrame:
    duplicated_id = df.duplicated(subset=[original_id]).sum()

    if duplicated_id > 0:
        print('Operação não é possível, id duplicado!')
        return
    new_df = create_id(df, new_id_name)
    return new_df


def convert_and_fill_column(df: pd.DataFrame, columns:list | str) -> pd.DataFrame:
    if isinstance(columns, list):
        for column in columns:
            df[column] = df[column].fillna(0)
            df[column] = df[column].astype(int)
    else:        
        df[columns] = df[columns].fillna(0)
        df[columns] = df[columns].astype(int)
    return df


def cleaning_df(df_name:str | pd.DataFrame, columns_drop: list|str = None, column_inconsitency:str = None) -> tuple | pd.DataFrame:
    if isinstance(df_name, pd.DataFrame):
        df = df_name
    else:
        df = dataframes[df_name]
    if columns_drop:
        df = df.drop(columns_drop, axis=1)
    

    if column_inconsitency and df[column_inconsitency].isna().any():
        df_inconsistency = create_inconsistency_df(df, column_inconsitency)
        df = df.dropna(subset=[column_inconsitency])
        return df, df_inconsistency
    return df


def create_dates(inicio: date, fim: date):
    datas = []
    data_atual = inicio
    while data_atual <= fim:
        datas.append(data_atual)
        data_atual += timedelta(days=1)
    return datas


def split_date(df: pd.DataFrame, 
                column_date: str,
                column_day:str,
                column_month:str, 
                column_year:str,
                column_timestamp: str = None,
                column_time:str = None
                ) -> pd.DataFrame:
    if column_timestamp:
        df[column_timestamp] = pd.to_datetime(df[column_timestamp])
        df[column_date] = df[column_timestamp].dt.date
        df[column_time] = df[column_timestamp].dt.time
    
    df[column_date] = pd.to_datetime(df[column_date])
    df[column_day] = df[column_date].dt.day
    df[column_month] = df[column_date].dt.month
    df[column_year] = df[column_date].dt.year
    return df


def df_dates(orders: pd.DataFrame) -> pd.DataFrame:
    dim_order_dates = orders[['order_id', 'order_purchase_timestamp']].copy()
    dim_order_dates['order_purchase_timestamp'] = pd.to_datetime(
        dim_order_dates['order_purchase_timestamp'])
    dim_order_dates = split_date(dim_order_dates,
                                'order_purchase_date',
                                'order_purchase_day',
                                'order_purchase_month',
                                'order_purchase_year',
                                'order_purchase_timestamp',
                                'order_purchase_time'
                                )
    return dim_order_dates



def create_df_dates():
    df_orders = dataframes['df_orders']
    dim_dates = df_dates(df_orders)
    return dim_dates


def create_fact(
        dim_orders: pd.DataFrame,
        dim_order_dates: pd.DataFrame,
        dim_order_payments: pd.DataFrame,
        products_merge: pd.DataFrame,
        dim_customers:pd.DataFrame
):

    df_order_reviews = dataframes['df_order_reviews']
    fct_sales = dim_orders[['order_id', 'customer_id', 'id_order']].merge(
        dim_customers[['customer_id', 'customer_unique_id', 'id_customer']], on='customer_id', how='left'
    ).merge(
        dim_order_dates[['order_id', 'orders_date_id']], on='order_id', how='left'
    ).merge(
        dim_order_payments[['order_id', 'payment_type_id',
                            'payment_value', 'payment_installments']],
        on='order_id', how='left'
    ).merge(
        products_merge[['order_id', 'product_id', 'price', 'freight_value', 'id_product']],
        on='order_id', how='left'
    ).merge(
        df_order_reviews[['order_id',  'review_score']],
        on='order_id', how='left'
    )

    fct_sales = create_id(fct_sales, 'id_sales')
    return fct_sales


def transformation(dataframes):
    # Transformando produtos
    columns_drop_products = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
    columns_numeric_products = [
        'product_weight_g',
        'product_length_cm',
        'product_height_cm',
        'product_width_cm'
    ]

    dim_products, products_without_name = cleaning_df('df_products', columns_drop_products, 'product_category_name')
    dim_products = convert_and_fill_column(dim_products, columns_numeric_products)
    dim_products = replace_id(dim_products, 'product_id', 'id_product')
    products_without_name = replace_id(products_without_name, 'product_id', 'id_product')
    products_without_name = cleaning_df(products_without_name, 'product_id')

    list_merge_products = ['product_id', 'order_id', 'price', 'freight_value']
    df_products_merge = create_merge(dim_products, dataframes['df_order_items'], 'left', 'product_id', list_merge_products)

    # Criando dimensão pagamentos

    order_payments = cleaning_df('df_order_payments')
    order_payments = create_id(order_payments, 'order_payments_id')

    payment_type = order_payments['payment_type'].drop_duplicates()\
        .reset_index(drop=True)

    payment_type_df = pd.DataFrame({
        'payment_type': payment_type
    })

    payment_type_df = create_id(payment_type_df, 'payment_type_id')


    df_payments_merge = create_merge(order_payments, payment_type_df, 'left', 'payment_type')


    # Criando datas
    dim_dates = create_df_dates()

    min_year = dim_dates['order_purchase_year'].min()
    max_year = dim_dates['order_purchase_year'].max()
    all_dates = create_dates(date(min_year, 1, 1), date(max_year, 12, 31))

    all_dates_df = pd.DataFrame({'order_purchase_date': all_dates})
    all_dates_df['order_purchase_date'] = pd.to_datetime(
        all_dates_df['order_purchase_date'])

    dim_order_dates = create_merge(all_dates_df, dim_dates, 'left', 'order_purchase_date')
    dim_order_dates = create_id(dim_order_dates, 'orders_date_id')

    dim_order_dates, dates_without_sales = cleaning_df(dim_order_dates, column_inconsitency='order_id')
    dim_order_dates = convert_and_fill_column(dim_order_dates, ['order_purchase_day', 'order_purchase_month','order_purchase_year'])

    list_columns_dates = ['order_id', 'order_purchase_timestamp', 'order_purchase_time']
    dates_without_sales = cleaning_df(dates_without_sales, list_columns_dates)


    dates_without_sales = split_date(dates_without_sales,
                                    'order_purchase_date',
                                    'order_purchase_day',
                                    'order_purchase_month',
                                    'order_purchase_year'
                                    )
    
    # Criando dimensão orders
    dim_orders = dataframes['df_orders']

    dim_orders = replace_id(dim_orders, 'order_id', 'id_order')


    # criando dimensão customers
    df_customer = dataframes['df_customers']

    dim_customers= df_customer.drop_duplicates('customer_unique_id')

    dim_customers = replace_id(dim_customers, 'customer_unique_id', 'id_customer')

    dim_customers_join = create_merge(df_customer, dim_customers, 'left', 'customer_unique_id', ['id_customer', 'customer_unique_id'])

    # Criando fato 
    fct_sales = create_fact(dim_orders, dim_order_dates, df_payments_merge, df_products_merge, dim_customers_join)

    columns_drop_fct = ['order_id', 'customer_id', 'customer_unique_id', 'product_id']
    fct_sales, sales_without_payment = cleaning_df(fct_sales, columns_drop_fct, 'payment_type_id')

    fct_sales, sales_without_product = cleaning_df(fct_sales, column_inconsitency='id_product')

    convert_columns = ['payment_type_id', 'payment_installments', 'id_product', 'review_score']

    fct_sales = convert_and_fill_column(fct_sales, convert_columns)

    # Removendo colunas desnecessárias

    dim_customers = cleaning_df(dim_customers, ['customer_id', 'customer_unique_id'])
 

    dim_order_dates = cleaning_df(dim_order_dates, 'order_id')


    dim_orders = dim_orders[['id_order','order_status']].copy()


    dim_products = cleaning_df(dim_products, 'product_id')

    news_dfs = {
        'dimensions': {
            'dim_customers': dim_customers, 'dim_order_dates': dim_order_dates,
            'dim_orders' : dim_orders, 'dim_products': dim_products, 'dim_order_payments': payment_type_df
            },
        'fact': {'fct_sales': fct_sales},

        'incosistencies': {
            'dates_without_sales': dates_without_sales, 'sales_without_payment': sales_without_payment,
            'sales_without_product': sales_without_product, 'products_without_name': products_without_name
            }
    }

    return news_dfs


new_dfs = transformation(dataframes)

