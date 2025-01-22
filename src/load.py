from src.transform import transformation
from src.extract import dataframes
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from dotenv import load_dotenv
import os

dataframes_transformed= transformation(dataframes)

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


# Estabelecendo conexão

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port='5432'
)

cur = conn.cursor()

create_table_products = """CREATE TABLE IF NOT EXISTS dim_products (
    id_product INT PRIMARY KEY,
    product_category_name VARCHAR(60) NOT NULL,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);"""

tabela_incosistencia_produto = """
    CREATE TABLE IF NOT EXISTS products_without_name (
        id_product INT PRIMARY KEY,
        product_category_name VARCHAR(60),
        product_weight_g DECIMAL,
        product_length_cm DECIMAL,
        product_height_cm DECIMAL,
        product_width_cm DECIMAL
    );
"""

create_table_payments = """
CREATE TABLE IF NOT EXISTS dim_order_payments (
    payment_type_id INT PRIMARY KEY,
    payment_type VARCHAR(20) NOT NULL
);
"""

create_table_customers = """CREATE TABLE IF NOT EXISTS dim_customers (
    id_customer int PRIMARY KEY,
    customer_zip_code_prefix INT,
    customer_city VARCHAR(50),
    customer_state VARCHAR(50)
);"""

create_table_dates = """
CREATE TABLE IF NOT EXISTS dim_order_dates (
    orders_date_id INT PRIMARY KEY,
    order_purchase_date DATE,
    order_purchase_day INT,
    order_purchase_month INT,
    order_purchase_year INT,
    order_purchase_timestamp TIMESTAMP,
    order_purchase_time TIME
);
"""


create_table_dates_without_sales = """
CREATE TABLE IF NOT EXISTS dates_without_sales (
    orders_date_id INT PRIMARY KEY,
    order_purchase_date DATE,
    order_purchase_day INT,
    order_purchase_month INT,
    order_purchase_year INT
);
"""

create_table_orders = """
CREATE TABLE IF NOT EXISTS dim_orders (
    id_order INT PRIMARY KEY,
    order_status VARCHAR(50) NOT NULL
);
"""


create_table_facts = """
CREATE TABLE IF NOT EXISTS fct_sales (
    id_sales INT PRIMARY KEY,
    id_order INT,
    id_customer INT,
    orders_date_id INT,
    payment_type_id INT,
    payment_value DECIMAL,
    payment_installments INT,
    price DECIMAL,
    freight_value DECIMAL,
    id_product INT,
    review_score INT,
    FOREIGN KEY (orders_date_id) REFERENCES dim_order_dates(orders_date_id),
    FOREIGN KEY (payment_type_id) REFERENCES dim_order_payments(payment_type_id),
    FOREIGN KEY (id_product) REFERENCES dim_products(id_product),
    FOREIGN KEY (id_order) REFERENCES dim_orders(id_order),
    FOREIGN KEY (id_customer) REFERENCES dim_customers(id_customer)
);
"""

create_table_sem_pagamento = """
CREATE TABLE IF NOT EXISTS sales_without_payment (
        id_sales INT PRIMARY KEY,
        id_order INT,
        id_customer INT,
        orders_date_id INT,
        payment_type_id DECIMAL,
        payment_value DECIMAL,
        payment_installments DECIMAL,
        price DECIMAL,
        freight_value DECIMAL,
        id_product DECIMAL,
        review_score DECIMAL
);"""


create_table_vendas_sem_produto = """
CREATE TABLE IF NOT EXISTS sales_without_product (
        id_sales INT PRIMARY KEY,
        id_order INT,
        id_customer INT,
        orders_date_id INT,
        payment_type_id DECIMAL,
        payment_value DECIMAL,
        payment_installments DECIMAL,
        price DECIMAL,
        freight_value DECIMAL,
        id_product DECIMAL,
        review_score DECIMAL
);"""


def inserir_tabelas_banco(cur, conn, *args):
    for query in args:
        cur.execute(query)

    print('Tabelas criadas com sucesso!')
    conn.commit()


def inserir_dados(cur, conn, df: pd.DataFrame, tabela, colunas):
    # Convertemos a lista de colunas para uma string separada por vírgulas
    colunas_str = ', '.join(colunas)

    # Criamos o template para as colunas
    template = '(' + ', '.join(['%({})s'.format(col) for col in colunas]) + ')'

    try:
        # Usamos execute_values para inserir os dados
        execute_values(cur,
                       f'''INSERT INTO {tabela} ({colunas_str}) VALUES %s''',
                       df.to_dict('records'),
                       template=template)
        print('Dados inseridos com sucesso!')
        conn.commit()
    except (Exception) as error:
        print(error)
        conn.rollback()



def load(dataframes_transformed):
    inserir_tabelas_banco(cur, conn, create_table_products,
                      tabela_incosistencia_produto,
                      create_table_payments,
                      create_table_customers,
                      create_table_dates,
                      create_table_dates_without_sales,
                      create_table_orders,
                      create_table_facts,
                      create_table_sem_pagamento,
                      create_table_vendas_sem_produto)
    for type in dataframes_transformed.keys():
        for table, df in dataframes_transformed[type].items():
            table_name = table
            df_bd = df
            df_columns = df.columns.tolist()
            inserir_dados(cur, conn, df_bd, table_name, df_columns)
