import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.base_hook import BaseHook
from airflow import settings
from airflow.models import Connection
from airflow.models import Variable
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
import requests
import psycopg2
import random


#################
### Constants ###

CSV_URL = 'https://raw.githubusercontent.com/serggavr/1t_airflow/main/raw_data/supermarket_1/data.csv'

###################################
### Set Variables & Connections ###

try:
    store_connection_params = BaseHook.get_connection('connection_store')
except:
    conn = Connection(
        conn_id='connection_store',
        conn_type='postgres',
        host='db',
        login='postgres',
        password='password',
        schema='supermarket',
        port=5432
    )
    session = settings.Session()
    session.add(conn)
    session.commit()

    store_connection_params = BaseHook.get_connection('connection_store')


try:
    fs_connection_params = BaseHook.get_connection('filepath')
except:
    conn = Connection(
        conn_id='filepath',
        conn_type='fs',
        extra='{"path":"/opt/airflow/"}'
    )
    session = settings.Session()
    session.add(conn)
    session.commit()

    fs_connection_params = BaseHook.get_connection('filepath')


Variable.setdefault(key='raw_data_path1', default='/opt/airflow/raw_data')
Variable.setdefault(key='raw_data_folder_name1', default='supermarket_1')
Variable.setdefault(key='raw_data_file_name1', default='supermarket_raw_data.csv')
Variable.setdefault(key='raw_table_name1', default='supermarket_data')
Variable.setdefault(key='raw_store_name1', default='raw_store')
Variable.setdefault(key='core_store_name1', default='core_store')
Variable.setdefault(key='mart_store_name1', default='mart_store')

###################################
### Get Variables & Connections ###

# store_connection_params = BaseHook.get_connection('connection_store')
raw_data_path = Variable.get('raw_data_path1')
raw_data_folder_name = Variable.get('raw_data_folder_name1')
raw_data_file_name = Variable.get('raw_data_file_name1')
raw_table_name = Variable.get('raw_table_name1')

raw_store_name = Variable.get('raw_store_name1')
core_store_name = Variable.get('core_store_name1')
mart_store_name = Variable.get('mart_store_name1')

core_sql_query_create_table_segments = """
CREATE TABLE IF NOT EXISTS core_store.segments (
        segment varchar(32),
        segment_id SERIAL,
        PRIMARY KEY (segment_id)
    );
"""
core_sql_query_create_table_category = """
CREATE TABLE IF NOT EXISTS core_store.category (
        category_name varchar(32),
        category_id SERIAL,
        PRIMARY KEY (category_id)
    );
"""
core_sql_query_create_table_sub_category = """
CREATE TABLE IF NOT EXISTS core_store.sub_category (
        sub_category_name varchar(32),
        category_id INTEGER,
        sub_category_id SERIAL,
        PRIMARY KEY (sub_category_id),
        FOREIGN KEY (category_id) REFERENCES core_store.category(category_id) ON DELETE SET NULL
    );
"""
core_sql_query_create_table_products = """
CREATE TABLE IF NOT EXISTS core_store.products (
        product_id varchar(32),
        product_name varchar(128),
        category_id INTEGER,
        sub_category_id INTEGER,
        product_price double precision,
        PRIMARY KEY (product_id, product_name),
        FOREIGN KEY (category_id) REFERENCES core_store.category(category_id) ON DELETE SET NULL,
        FOREIGN KEY (sub_category_id) REFERENCES core_store.sub_category(sub_category_id) ON DELETE SET NULL
    );
"""
core_sql_query_create_table_countries = """
CREATE TABLE IF NOT EXISTS core_store.countries (
        country varchar(32),
        country_id SERIAL,
        PRIMARY KEY (country_id)
    );
"""
core_sql_query_create_table_regions = """
CREATE TABLE IF NOT EXISTS core_store.regions (
        region varchar(32),
        country_id INTEGER,
        region_id SERIAL,
        PRIMARY KEY (region_id),
        FOREIGN KEY (country_id) REFERENCES core_store.countries(country_id) ON DELETE SET NULL
    );
"""
core_sql_query_create_table_states = """
CREATE TABLE IF NOT EXISTS core_store.states (
        state varchar(32),
        country_id INTEGER,
        region_id INTEGER,
        state_id SERIAL,
        PRIMARY KEY (state_id),
        FOREIGN KEY (region_id) REFERENCES core_store.regions(region_id) ON DELETE SET NULL,
        FOREIGN KEY (country_id) REFERENCES core_store.countries(country_id) ON DELETE SET NULL
    );
"""
core_sql_query_create_table_cities = """
CREATE TABLE IF NOT EXISTS core_store.cities (
        city varchar(32),
        country_id INTEGER,
        region_id INTEGER,
        state_id INTEGER,
        city_id SERIAL,
        PRIMARY KEY (city_id),
        FOREIGN KEY (country_id) REFERENCES core_store.countries(country_id) ON DELETE SET NULL,
        FOREIGN KEY (region_id) REFERENCES core_store.regions(region_id) ON DELETE SET NULL,
        FOREIGN KEY (state_id) REFERENCES core_store.states(state_id) ON DELETE SET NULL
    );
"""
core_sql_query_create_table_customers_address = """
CREATE TABLE IF NOT EXISTS core_store.customers_address (
        country_id INTEGER,
        region_id INTEGER,
        state_id INTEGER,
        city_id INTEGER,
        customer_postal_code varchar(32),
        customer_address_id SERIAL,
        PRIMARY KEY (customer_address_id),
        FOREIGN KEY (country_id) REFERENCES core_store.countries(country_id) ON DELETE SET NULL,
        FOREIGN KEY (region_id) REFERENCES core_store.regions(region_id) ON DELETE SET NULL,
        FOREIGN KEY (state_id) REFERENCES core_store.states(state_id) ON DELETE SET NULL,
        FOREIGN KEY (city_id) REFERENCES core_store.cities(city_id) ON DELETE SET NULL
    );
"""
core_sql_query_create_table_customers = """
CREATE TABLE IF NOT EXISTS core_store.customers(
        customer_id varchar(32),
        customer_name varchar(32),
        customer_address_id INTEGER,
        PRIMARY KEY (customer_id, customer_address_id),
        FOREIGN KEY (customer_address_id) REFERENCES core_store.customers_address(customer_address_id) ON DELETE SET NULL
    );
"""
core_sql_query_create_table_ship_modes = """
CREATE TABLE IF NOT EXISTS core_store.ship_modes(
        ship_mode_name varchar(32),
        ship_mode_id SERIAL,
        PRIMARY KEY (ship_mode_id)
    );
"""
core_sql_query_create_table_ship_orders = """
CREATE TABLE IF NOT EXISTS core_store.ship_orders(
        order_id varchar(32),
        ship_mode_id INTEGER,
        order_ship_date DATE,
        PRIMARY KEY (order_id),
        FOREIGN KEY (ship_mode_id) REFERENCES core_store.ship_modes(ship_mode_id) ON DELETE SET NULL
    );
"""
core_sql_query_create_table_orders = """
CREATE TABLE IF NOT EXISTS core_store.orders(
        order_id varchar(32),
        order_date DATE,
        PRIMARY KEY (order_id)
    );
"""
core_sql_query_create_table_sales = """
CREATE TABLE IF NOT EXISTS core_store.sales(
        sale_id integer,
        order_id varchar(32),
        product_id varchar(32),
        product_name varchar(128),
        segment_id integer,
        customer_id varchar(32),
        customer_address_id INTEGER,
        product_price double precision,
        order_quantity INTEGER,
        order_discount double precision,
        sales double precision,
        order_profit double precision,
        PRIMARY KEY (sale_id),
        FOREIGN KEY (product_id, product_name) REFERENCES core_store.products(product_id, product_name) ON DELETE SET NULL,
        FOREIGN KEY (order_id) REFERENCES core_store.orders(order_id) ON DELETE SET NULL,
        FOREIGN KEY (customer_id, customer_address_id) REFERENCES core_store.customers(customer_id, customer_address_id) ON DELETE SET NULL,
        FOREIGN KEY (segment_id) REFERENCES core_store.segments(segment_id) ON DELETE SET NULL
    );
"""

core_sql_query_fill_table_segments = """
INSERT INTO core_store.segments (segment)
    select
        "Segment" as order_segment
    from raw_store.supermarket_data
    group by order_segment
"""
core_sql_query_fill_table_category = """
INSERT INTO core_store.category (category_name)
    select
        "Category" as category_name
    from raw_store.supermarket_data
    group by category_name
"""
core_sql_query_fill_table_sub_category = """
INSERT INTO core_store.sub_category (sub_category_name, category_id)
    select
        "Sub-Category" as sub_category_name,
        core_store.category.category_id as category_id
    from raw_store.supermarket_data as s
    JOIN core_store.category ON s."Category" = core_store.category.category_name
    group by sub_category_name, category_id
"""
core_sql_query_fill_table_products = """
INSERT INTO core_store.products (product_id, product_name, category_id, sub_category_id, product_price)
    select
        "Product ID" as product_id,
        "Product Name" as product_name,
        core_store.category.category_id as category_id,
        core_store.sub_category.sub_category_id as sub_category_id,
        cast(cast(("Sales" / (1 - "Discount")) / "Quantity" as numeric) as double precision) as product_price
    from raw_store.supermarket_data as s
    JOIN core_store.category ON s."Category" = core_store.category.category_name
    JOIN core_store.sub_category ON s."Sub-Category" = core_store.sub_category.sub_category_name
    group by product_id, product_name, core_store.category.category_id, sub_category_id, product_price
"""
core_sql_query_fill_table_countries = """
INSERT INTO core_store.countries (country)
    select
        "Country" as country
    from raw_store.supermarket_data as s
    group by country
"""
core_sql_query_fill_table_regions = """
INSERT INTO core_store.regions (region, country_id)
    select
        "Region" as region,
        core_store.countries.country_id as country_id
    from raw_store.supermarket_data as s
    JOIN core_store.countries ON s."Country" = core_store.countries.country
    group by region, country_id
"""
core_sql_query_fill_table_states = """
INSERT INTO core_store.states (state, country_id, region_id)
    select
        "State" as state,
        core_store.countries.country_id as country_id,
        core_store.regions.region_id as region_id
    from raw_store.supermarket_data as s
    JOIN core_store.countries ON s."Country" = core_store.countries.country
    JOIN core_store.regions ON s."Region" = core_store.regions.region
    group by state, core_store.countries.country_id, region_id
"""
core_sql_query_fill_table_cities = """
INSERT INTO core_store.cities (city, country_id, region_id, state_id)
    select
        "City" as city,
        core_store.countries.country_id as country_id,
        core_store.regions.region_id as region_id,
        core_store.states.state_id as state_id
    from raw_store.supermarket_data as s
    JOIN core_store.countries ON s."Country" = core_store.countries.country
    JOIN core_store.regions ON s."Region" = core_store.regions.region
    JOIN core_store.states ON s."State" = core_store.states.state
    group by city, core_store.countries.country_id, core_store.regions.region_id, state_id
"""
core_sql_query_fill_table_customers_address = """
INSERT INTO core_store.customers_address (country_id, region_id, state_id, city_id, customer_postal_code)
    select
        core_store.countries.country_id as country_id,
        core_store.regions.region_id as region_id,
        core_store.states.state_id as state_id,
        core_store.cities.city_id as city_id,
        "Postal Code" as customer_postal_code
    from raw_store.supermarket_data as s
    JOIN core_store.countries ON s."Country" = core_store.countries.country
    JOIN core_store.regions ON s."Region" = core_store.regions.region
    JOIN core_store.states ON s."State" = core_store.states.state
    JOIN core_store.cities ON s."City" = core_store.cities.city
    group by core_store.countries.country_id, core_store.regions.region_id, core_store.states.state_id, core_store.cities.city_id, customer_postal_code
"""
core_sql_query_fill_table_customers = """
INSERT INTO core_store.customers (customer_id, customer_name, customer_address_id)
    select
        "Customer ID" as customer_id,
        "Customer Name" as customer_name,
        core_store.customers_address.customer_address_id as customer_address_id
    from raw_store.supermarket_data as s
    JOIN core_store.countries ON s."Country" = core_store.countries.country
    JOIN core_store.regions ON s."Region" = core_store.regions.region
    JOIN core_store.states ON s."State" = core_store.states.state
    JOIN core_store.cities ON s."City" = core_store.cities.city
    JOIN core_store.customers_address
        ON core_store.customers_address.city_id = core_store.cities.city_id
        AND core_store.customers_address.state_id = core_store.states.state_id
        AND core_store.customers_address.region_id = core_store.regions.region_id
        AND core_store.customers_address.country_id = core_store.countries.country_id
    group by customer_id, customer_name, core_store.customers_address.customer_address_id
"""
core_sql_query_fill_table_ship_modes = """
INSERT INTO core_store.ship_modes (ship_mode_name)
    select
        "Ship Mode" as ship_mode_name
    from raw_store.supermarket_data as s
    group by ship_mode_name
"""
core_sql_query_fill_table_ship_orders = """
INSERT INTO core_store.ship_orders (order_id, ship_mode_id, order_ship_date)
    select
        "Order ID" as order_id,
        core_store.ship_modes.ship_mode_id as ship_mode_id,
        "Ship Date"::DATE as order_ship_date
    from raw_store.supermarket_data as s
    JOIN core_store.ship_modes ON s."Ship Mode" = core_store.ship_modes.ship_mode_name
    group by order_id, core_store.ship_modes.ship_mode_id, order_ship_date
"""
core_sql_query_fill_table_orders = """
INSERT INTO core_store.orders (order_id, order_date)
    select
        "Order ID" as order_id,
        "Order Date"::DATE as order_date
    from raw_store.supermarket_data as s
    group by order_id, order_date
"""
core_sql_query_fill_table_sales = """
INSERT INTO core_store.sales (sale_id, order_id, product_id, product_name, segment_id, customer_id, customer_address_id, product_price, order_quantity, order_discount, sales, order_profit)
    select
        "Row ID" as sale_id,
        "Order ID" as order_id,
        "Product ID" as product_id,
        "Product Name" as product_name,
        core_store.segments.segment_id as segment_id,
        core_store.customers.customer_id as customer_id,
        core_store.customers_address.customer_address_id as customer_address_id,
        core_store.products.product_price as product_price,
        "Quantity" as order_quantity,
        "Discount" as order_discount,
        "Sales" as sales,
        "Profit" as order_profit
    from raw_store.supermarket_data as s
    JOIN core_store.products
        ON s."Product ID" = core_store.products.product_id
        AND s."Product Name" = core_store.products.product_name
    JOIN core_store.customers
        ON s."Customer ID" = core_store.customers.customer_id
        AND s."Customer Name" = core_store.customers.customer_name
    JOIN core_store.countries
        ON s."Country" = core_store.countries.country
    JOIN core_store.regions
        ON s."Region" = core_store.regions.region
        AND core_store.countries.country_id = core_store.regions.country_id
    JOIN core_store.states
        ON s."State" = core_store.states.state
        AND core_store.regions.region_id = core_store.states.region_id
        AND core_store.countries.country_id = core_store.states.country_id
    JOIN core_store.cities
        ON s."City" = core_store.cities.city
        AND core_store.regions.region_id = core_store.cities.region_id
        AND core_store.countries.country_id = core_store.cities.country_id
        AND core_store.states.state_id = core_store.cities.state_id
    JOIN core_store.customers_address
        ON core_store.regions.region_id = core_store.customers_address.region_id
        AND core_store.countries.country_id = core_store.customers_address.country_id
        AND core_store.states.state_id = core_store.customers_address.state_id
        AND core_store.cities.city_id = core_store.customers_address.city_id
        AND s."Postal Code"::varchar(32) = core_store.customers_address.customer_postal_code
    JOIN core_store.segments
        ON s."Segment" = core_store.segments.segment
    group by sale_id, s."Product ID", s."Product Name", order_id, product_id, product_name, segment_id, customer_id, core_store.customers_address.customer_address_id, core_store.products.product_price, order_quantity, order_discount, sales, order_profit
"""

mart_sql_query_create_table_sales_per_year_corporate_segment = """
CREATE TABLE IF NOT EXISTS mart_store.sales_per_year_corporate_segment(
        year INTEGER,
        corporate_sales double precision
    );
"""
mart_sql_query_create_table_sales_per_year_category_sub_category = """
CREATE TABLE IF NOT EXISTS mart_store.sales_per_year_category_sub_category(
            year INTEGER,
            category_name varchar(32),
            sub_category_name varchar(32),
            sub_category_sales_per_year double precision,
            category_sales_per_year double precision
        );
"""

mart_sql_query_fill_table_sales_per_year_corporate_segment = """
INSERT INTO mart_store.sales_per_year_corporate_segment (year, corporate_sales)
     select
            extract('YEAR' FROM core_store.orders.order_date)::INTEGER as year,
            round(sum(sl.sales)::numeric, 2) as corporate_sales
        from core_store.sales as sl
        JOIN core_store.orders
            ON sl.order_id = core_store.orders.order_id
        JOIN core_store.segments
            ON sl.segment_id = core_store.segments.segment_id
        WHERE core_store.segments.segment = 'Corporate'
        group by year
"""


#################
### Functions ###

# def create_connections():
#     if BaseHook.get_connection('connection_store'):
#         print('connection connection_store already exist')
#     else:
#         conn = Connection(
#                 conn_id='connection_store',
#                 conn_type='postgres',
#                 host='db',
#                 login='postgres',
#                 password='password',
#                 schema='supermarket',
#                 port=5432
#         )
#         session = settings.Session()
#         session.add(conn)
#         session.commit()
#
#     if BaseHook.get_connection('filepath'):
#         print('connection filepath already exist')
#     else:
#         conn = Connection(
#                 conn_id='filepath',
#                 conn_type='fs',
#                 extra='{"path":"/opt/airflow/"}'
#         )
#         session = settings.Session()
#         session.add(conn)
#         session.commit()

def fn_load_data_to_folder(path, folder, file):
    response = requests.get(CSV_URL)
    with open(f'{path}/{folder}/{file}', 'wb') as f:
        f.write(response.content)

def fn_load_csv_to_raw_store(db_connection_params, schema, table, path, folder, file):
    dataframe = pd.read_csv(f'{path}/{folder}/{file}', encoding="latin-1")

    try:
        engine = create_engine(f'postgresql+psycopg2://{db_connection_params.login}:{db_connection_params.password}@{db_connection_params.host}/{db_connection_params.schema}')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    dataframe.to_sql(table, schema=schema, con=engine, if_exists='replace', index=False)

def psycopg2_connection(store_connection_params):

    try:
        store_connection = psycopg2.connect(
            dbname=store_connection_params.schema,
            user=store_connection_params.login,
            password=store_connection_params.password,
            host=store_connection_params.host,
            port=store_connection_params.port)

        return store_connection

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def fn_create_store(store_name, store_connection):

    store_cursor = store_connection.cursor()

    # drop old store & create new
    store_cursor.execute(f"""
            DROP SCHEMA IF EXISTS {store_name} CASCADE;
            CREATE SCHEMA {store_name};
        """)
    store_connection.commit()

def fn_do_sql_query(store_connection, create_table_query):
    store_cursor = store_connection.cursor()

    # create table
    store_cursor.execute(create_table_query)
    store_connection.commit()

def fn_return_random_category(store_connection, **kwargs):

    store_cursor = store_connection.cursor()

    # load data
    store_cursor.execute("""
         select
            category_name
        from core_store.category
        """)
    resp_data = store_cursor.fetchall()

    ti = kwargs['ti']
    ti.xcom_push(key='category_name', value=random.choice(resp_data))

    store_cursor.close()
    store_connection.close()

def fn_load_sub_category_sales_data_from_core_to_mart_db(store_connection, **kwargs):
    ti = kwargs['ti']

    category_name = ti.xcom_pull(key='category_name')[0]

    store_cursor = store_connection.cursor()

    # load data
    store_cursor.execute(f"""
    insert into mart_store.sales_per_year_category_sub_category(
        year,
        category_name,
        sub_category_name,
        sub_category_sales_per_year,
        category_sales_per_year
    ) select
        *,
        sum(sub_category_sales_per_year) over (partition by year) as category_sales_per_year
    from (
        select
            extract('YEAR' FROM core_store.orders.order_date)::INTEGER as year,
            core_store.category.category_name as category_name,
            core_store.sub_category.sub_category_name as sub_category_name,
            round(sum(sl.sales)::numeric, 2) as sub_category_sales_per_year
        from core_store.sales as sl
        JOIN core_store.orders
            ON sl.order_id = core_store.orders.order_id
        JOIN core_store.products
            ON sl.product_id = core_store.products.product_id
        JOIN core_store.category
            ON core_store.category.category_id = core_store.products.category_id
        JOIN core_store.sub_category
            ON core_store.sub_category.category_id = core_store.products.category_id
            AND core_store.sub_category.sub_category_id = core_store.products.sub_category_id
        where core_store.category.category_name = '{category_name}'
        group by year, category_name, sub_category_name
    ) as ss
        """)
    store_connection.commit()

    store_cursor.close()
    store_connection.close()


############
### DAGs ###

my_dag = DAG(
    dag_id='supermarket',
    start_date=datetime(2023, 8, 2),
    schedule_interval='@once'
)

## start
task_start = DummyOperator(task_id='start', dag=my_dag)

## group_fs
with TaskGroup(group_id='group_fs', dag=my_dag) as group_fs:

    task_create_folder = BashOperator(
            task_id='create_folder',
            bash_command='mkdir -p {{ params.PATH }}/{{ params.FOLDER_NAME }};',
            # bash_command='echo "created";',
            params={'PATH': raw_data_path,
                    'FOLDER_NAME': raw_data_folder_name},
            dag=my_dag
        )

    task_load_data_to_folder = PythonOperator(
        task_id='load_data_to_folder',
        python_callable=fn_load_data_to_folder,
        op_kwargs={'path': raw_data_path,
                   'folder': raw_data_folder_name,
                   'file': raw_data_file_name
                   },
        dag=my_dag
    )

    task_group_end = EmptyOperator(task_id="end_group_fs", dag=my_dag)

    task_create_folder >> task_load_data_to_folder >> task_group_end

## wait_file_include_in_folder
task_check_folder_include_data = FileSensor(
        fs_conn_id='filepath',
        task_id='wait_file_include_in_folder',
        filepath=f'{raw_data_path}/{raw_data_folder_name}/{raw_data_file_name}',
    )

## group_raw_store
with TaskGroup(group_id='group_raw_store', dag=my_dag) as group_raw_store:

    task_load_data_from_folder_to_raw_store = PythonOperator(
        task_id='load_data_from_folder_to_raw_store',
        python_callable=fn_load_csv_to_raw_store,
        op_kwargs={'db_connection_params': store_connection_params,
                   'schema': raw_store_name,
                   'table': raw_table_name,
                   'path': raw_data_path,
                   'folder': raw_data_folder_name,
                   'file': raw_data_file_name
                   },
        provide_context=True,
        dag=my_dag
    )

    task_group_end = EmptyOperator(task_id="end_group_raw_store", dag=my_dag)

    task_load_data_from_folder_to_raw_store >> task_group_end

## group_core_store
with TaskGroup(group_id='group_core_store', dag=my_dag) as group_core_store:
    task_create_core_store = PythonOperator(
        task_id='create_core_store',
        python_callable=fn_create_store,
        op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                   'store_name': core_store_name},
        provide_context=True,
        dag=my_dag
    )

    task_group_end = EmptyOperator(task_id="end_group_core_store", dag=my_dag)

    with TaskGroup(group_id='group_core_store_create_tables', dag=my_dag) as group_core_store_create_tables:

        task_create_table_segments = PythonOperator(
            task_id='task_create_in_core_store_table_segments',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_segments},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_category = PythonOperator(
            task_id='task_create_in_core_store_table_category',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_category},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_sub_category = PythonOperator(
            task_id='task_create_in_core_store_table_sub_category',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_sub_category},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_products = PythonOperator(
            task_id='task_create_in_core_store_table_products',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_products},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_countries = PythonOperator(
            task_id='task_create_in_core_store_table_countries',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_countries},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_regions = PythonOperator(
            task_id='task_create_in_core_store_table_regions',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_regions},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_states = PythonOperator(
            task_id='task_create_in_core_store_table_states',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_states},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_cities = PythonOperator(
            task_id='task_create_in_core_store_table_cities',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_cities},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_customers_address = PythonOperator(
            task_id='task_create_in_core_store_table_customers_address',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_customers_address},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_customers = PythonOperator(
            task_id='task_create_in_core_store_table_customers',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_customers},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_ship_modes = PythonOperator(
            task_id='task_create_in_core_store_table_ship_modes',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_ship_modes},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_ship_orders = PythonOperator(
            task_id='task_create_in_core_store_table_ship_orders',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_ship_orders},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_orders = PythonOperator(
            task_id='task_create_in_core_store_table_orders',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_orders},
            provide_context=True,
            dag=my_dag
        )
        task_create_table_sales = PythonOperator(
            task_id='task_create_in_core_store_table_sales',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_create_table_sales},
            provide_context=True,
            dag=my_dag
        )

        task_create_table_segments >> task_create_table_category >> task_create_table_sub_category >> task_create_table_products >> task_create_table_countries >> task_create_table_regions >> task_create_table_states >> task_create_table_cities >> task_create_table_customers_address >> task_create_table_customers >> task_create_table_ship_modes >> task_create_table_ship_orders >> task_create_table_orders >> task_create_table_sales


    with TaskGroup(group_id='group_core_store_fill_tables', dag=my_dag) as group_core_store_fill_tables:
        task_fill_table_segments = PythonOperator(
            task_id='task_fill_in_core_store_table_segments',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_segments},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_category = PythonOperator(
            task_id='task_fill_in_core_store_table_category',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_category},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_sub_category = PythonOperator(
            task_id='task_fill_in_core_store_table_sub_category',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_sub_category},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_products = PythonOperator(
            task_id='task_fill_in_core_store_table_products',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_products},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_countries = PythonOperator(
            task_id='task_fill_in_core_store_table_countries',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_countries},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_regions = PythonOperator(
            task_id='task_fill_in_core_store_table_regions',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_regions},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_states = PythonOperator(
            task_id='task_fill_in_core_store_table_states',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_states},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_cities = PythonOperator(
            task_id='task_fill_in_core_store_table_cities',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_cities},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_customers_address = PythonOperator(
            task_id='task_fill_in_core_store_table_customers_address',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_customers_address},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_customers = PythonOperator(
            task_id='task_fill_in_core_store_table_customers',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_customers},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_ship_modes = PythonOperator(
            task_id='task_fill_in_core_store_table_ship_modes',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_ship_modes},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_ship_orders = PythonOperator(
            task_id='task_fill_in_core_store_table_ship_orders',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_ship_orders},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_orders = PythonOperator(
            task_id='task_fill_in_core_store_table_orders',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_orders},
            provide_context=True,
            dag=my_dag
        )
        task_fill_table_sales = PythonOperator(
            task_id='task_fill_in_core_store_table_sales',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': core_sql_query_fill_table_sales},
            provide_context=True,
            dag=my_dag
        )

        task_fill_table_segments >> task_fill_table_category >> task_fill_table_sub_category >> task_fill_table_products >> task_fill_table_countries >> task_fill_table_regions >> task_fill_table_states >> task_fill_table_cities >> task_fill_table_customers_address >> task_fill_table_customers >> task_fill_table_ship_modes >> task_fill_table_ship_orders >> task_fill_table_orders >> task_fill_table_sales

    task_create_core_store >> group_core_store_create_tables >> group_core_store_fill_tables >> task_group_end

## group_mart_store
with TaskGroup(group_id='group_mart_store', dag=my_dag) as group_mart_store:
    task_create_mart_store = PythonOperator(
        task_id='create_mart_store',
        python_callable=fn_create_store,
        op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                   'store_name': mart_store_name},
        provide_context=True,
        dag=my_dag
    )

    task_group_end = EmptyOperator(task_id="end_group_mart_store", dag=my_dag)

    with TaskGroup(group_id='group_mart_store_create_tables', dag=my_dag) as group_mart_store_create_tables:
        task_create_table_sales_per_year_corporate_segment = PythonOperator(
            task_id='task_create_in_mart_store_sales_per_year_corporate_segment',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': mart_sql_query_create_table_sales_per_year_corporate_segment},
            provide_context=True,
            dag=my_dag
        )


        task_create_table_sales_per_year_category_sub_category = PythonOperator(
            task_id='task_create_in_mart_store_table_sales_per_year_category_sub_category',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': mart_sql_query_create_table_sales_per_year_category_sub_category},
            provide_context=True,
            dag=my_dag
        )

        task_create_table_sales_per_year_corporate_segment >> task_create_table_sales_per_year_category_sub_category

    with TaskGroup(group_id='group_mart_store_fill_tables', dag=my_dag) as group_mart_store_fill_tables:
        task_fill_table_sales_per_year_corporate_segment = PythonOperator(
            task_id='task_fill_in_mart_store_table_sales_per_year_corporate_segment',
            python_callable=fn_do_sql_query,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                       'create_table_query': mart_sql_query_fill_table_sales_per_year_corporate_segment},
            provide_context=True,
            dag=my_dag
        )

        task_return_random_category = PythonOperator(
            task_id='return_random_category',
            python_callable=fn_return_random_category,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params)},
            provide_context=True,
            dag=my_dag
        )

        task_fill_table_sales_per_year_category_sub_category = PythonOperator(
            task_id='task_fill_in_mart_store_table_sales_per_year_category_sub_category',
            python_callable=fn_load_sub_category_sales_data_from_core_to_mart_db,
            op_kwargs={'store_connection': psycopg2_connection(store_connection_params)},
            provide_context=True,
            dag=my_dag
        )

        task_create_table_sales_per_year_category_sub_category >> task_return_random_category >> task_fill_table_sales_per_year_category_sub_category

    task_create_mart_store >> group_mart_store_create_tables >> group_mart_store_fill_tables >> task_group_end

dag_end = DummyOperator(task_id='end', dag=my_dag)


############
### graph ###

task_start >> group_fs >> task_check_folder_include_data >> group_raw_store >> group_core_store >> group_mart_store >> dag_end

