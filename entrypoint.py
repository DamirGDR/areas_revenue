import os

import pandas as pd
import numpy as np
import sqlalchemy as sa
import json
import google.oauth2.service_account
import googleapiclient.discovery
import datetime
import polyline
from shapely.geometry import Point, Polygon


# Секреты MySQL


def get_mysql_url() -> str:
    url = os.environ["mysql_url"]
    return url


def get_postgres_url() -> str:
    url = os.environ["postgres_url"]
    return url


def get_google_creds() -> str:
    url = os.environ["google_service_account_json"]
    return url


def read_sheet_data_to_pandas(service, spreadsheet_id: str, range_name: str):
    """
    Читает данные из Google Таблицы по указанному диапазону и преобразует их в Pandas DataFrame.

    Args:
        service: Объект службы Google Sheets API.
        spreadsheet_id: ID Google Таблицы.
        range_name: Диапазон ячеек (например, 'Sheet1!A1:D10').

    Returns:
        Pandas DataFrame с данными или None в случае ошибки.
    """
    if not service:
        return None

    try:
        # Выполняем запрос к Sheets API для получения значений
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            majorDimension='ROWS' # Получаем данные по строкам
        ).execute()

        values = result.get('values', [])

        if not values:
            print(f"В диапазоне '{range_name}' таблицы '{spreadsheet_id}' нет данных.")
            return pd.DataFrame() # Возвращаем пустой DataFrame

        # Pandas может напрямую создать DataFrame из списка списков.
        # Обычно первая строка содержит заголовки.
        headers = values[0]
        data_rows = values[1:]

        # Создаем Pandas DataFrame
        if headers:
            # Преобразуем данные в DataFrame, используя первую строку как заголовки
            df = pd.DataFrame(data_rows, columns=headers)
        else:
            # Если заголовков нет, просто создаем DataFrame из данных
            df = pd.DataFrame(values)
            print("Предупреждение: Заголовки не обнаружены. Столбцы названы автоматически (0, 1, 2...).")

        print(f"Данные из диапазона '{range_name}' успешно прочитаны и преобразованы в Pandas DataFrame.")
        return df

    except googleapiclient.errors.HttpError as error:
        print(f"Ошибка Google Sheets API: {error}")
        print(f"Код ошибки: {error.resp.status}")
        print(f"Сообщение об ошибке: {error._get_reason()}")
        return None
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        return None


def get_sheets_service(service_account_file: str):
    """
    Создает и возвращает объект службы Google Sheets API.

    Args:
        service_account_file: Путь к JSON-файлу учетных данных сервисного аккаунта.

    Returns:
        Объект googleapiclient.discovery.Resource для Sheets API.
    """
    try:
        # Определяем области доступа. Для чтения достаточно 'spreadsheets.readonly'.
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

        # Загружаем учетные данные сервисного аккаунта
        creds = google.oauth2.service_account.Credentials.from_service_account_file(
            service_account_file, scopes=SCOPES
        )

        # Строим сервис Sheets API
        service = googleapiclient.discovery.build('sheets', 'v4', credentials=creds)
        print("Сервис Google Sheets API успешно инициализирован.")
        return service
    except Exception as e:
        print(f"Ошибка при инициализации сервиса Google Sheets API: {e}")
        return None

def decode_polyline_to_tuples(encoded_polyline_string):
    coordinates_tuples = polyline.decode(encoded_polyline_string)
    return coordinates_tuples

def poly_contains(df):
    a = df['area_detail_tuple']
    b = df['parking_detail_tuple']
    res = Polygon(a).contains(Polygon(b))
    return res

def poly_contains_point_kvt(df):
    start_point = Point(df['g_lat'], df['g_lng'])
    area_poly = Polygon(df['area_poly'])
    res = area_poly.contains(start_point)
    return res

def poly_contains_point_orders(df):
    start_point = Point(df['start_lat'], df['start_lng'])
    area_poly = Polygon(df['area_poly'])
    res = area_poly.contains(start_point)
    return res

def main():

    url = get_mysql_url()
    url = sa.engine.make_url(url)
    url = url.set(drivername="mysql+mysqlconnector")
    engine_mysql = sa.create_engine(url)

    url = get_postgres_url()
    url = sa.engine.make_url(url)
    url = url.set(drivername="postgresql+psycopg")
    engine_postgresql = sa.create_engine(url)


    # Выгрузка t_area_revenue_stats2 pandas. Начало


    truncate_t_area_revenue_stats2 = '''
    DELETE FROM damir.t_area_revenue_stats2
    WHERE damir.t_area_revenue_stats2."timestamp_hour" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours';
    '''
    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(sa.text(truncate_t_area_revenue_stats2))
            transaction.commit()
            print(f"Таблица t_area_revenue_stats2 успешно очищена!")
    
    select_kvt = '''
        SELECT 
            res_tab."timestamp" ,
            res_tab.timestamp_hour ,
            res_tab.id ,
            res_tab.city_id ,
            res_tab.g_lat ,
            res_tab.g_lng
        FROM 
        (
            SELECT 
                tbh."timestamp" ,
                date_trunc('hour', tbh."timestamp") AS "timestamp_hour" ,
                tbh.id ,
                tbh.g_lat ,
                tbh.g_lng ,
                tb.city_id ,
                RANK() OVER (PARTITION BY date_trunc('hour', tbh."timestamp") ORDER BY tbh."timestamp" DESC) AS rn
            FROM damir.t_bike_history tbh
            LEFT JOIN damir.t_bike tb ON tbh.id = tb.id 
            WHERE tbh.error_status IN (0,7)
                AND tbh."timestamp" >= date_trunc('hour', NOW())
            ) AS res_tab
        WHERE res_tab.rn = 1
    '''
    df_kvt = pd.read_sql(select_kvt, engine_postgresql)
    print('select_kvt' + str(df_kvt['timestamp_hour'].unique()))
    x = df_kvt.groupby('timestamp_hour').agg({'id': 'count'})
    print('kvt1' + str(x))
    # Выгрузка areas
    select_areas = '''
        SELECT
            --ta.city_id ,
            ta.id AS area_id ,
            ta."name" AS area_name ,
            ta.detail AS area_detail
        FROM damir.t_area ta
        WHERE ta."name" LIKE '%%| Area |%%'
    '''
    df_areas = pd.read_sql(select_areas, engine_postgresql)
    # area в полигоны
    df_areas['area_poly'] = df_areas['area_detail'].apply(decode_polyline_to_tuples)

    df_kvt_area = df_kvt.merge(df_areas, how='cross')
    df_kvt_area['res'] = df_kvt_area.apply(poly_contains_point_kvt, axis=1)
    df_kvt_area_res = df_kvt_area[df_kvt_area['res'] == True]
    df_kvt_area_res = df_kvt_area_res.drop(
        columns=['timestamp', 'city_id', 'g_lat', 'g_lng', 'area_detail', 'area_poly', 'res'])

    df_kvt_area_res = df_kvt.merge(df_kvt_area_res, how='left', on=['timestamp_hour', 'id'])
    df_kvt_area_res['area_id'] = df_kvt_area_res['area_id'].fillna(0)
    df_kvt_area_res['area_name'] = df_kvt_area_res['area_name'].fillna('0')

    df_kvt_area_res = df_kvt_area_res.groupby(['timestamp_hour', 'city_id', 'area_id', 'area_name']) \
        .agg({'id': 'count'}) \
        .rename(columns={'id': 'kvt'}) \
        .reset_index()

    x = df_kvt_area_res.groupby('timestamp_hour').agg({'kvt': 'sum'})
    print('kvt2' + str(x))

    # Выгрузка заказов
    select_orders = '''
        SELECT 
            date_trunc('hour', tor."timestamp") AS timestamp_hour ,
            tor.id ,
            tb.city_id ,
            tor.start_lat ,
            tor.start_lng ,
            tor.ride_amount ,
            tor.discount ,
            tor.bike_discount_amount ,
            tor.subscription_price 
        FROM damir.t_orders_revenue tor 
        LEFT JOIN damir.t_bike tb ON tor.bid = tb.id
        WHERE tor."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    '''

    df_orders = pd.read_sql(select_orders, engine_postgresql)
    print('select_orders' + str(df_orders['timestamp_hour'].unique()))
    # Соединяю orders и area
    df_orders_area = df_orders.merge(df_areas, how='cross')
    df_orders_area['res'] = df_orders_area.apply(poly_contains_point_orders, axis=1)
    df_orders_area = df_orders_area[df_orders_area['res'] == True]
    df_orders_area = df_orders_area.drop(
        columns=['city_id', 'start_lat', 'start_lng', 'ride_amount', 'discount', 'bike_discount_amount',
                 'subscription_price', 'area_detail', 'area_poly', 'res'])
    df_orders_areas_res = df_orders.merge(df_orders_area, on=['timestamp_hour', 'id'], how='left')
    df_orders_areas_res['area_id'] = df_orders_areas_res['area_id'].fillna(0)
    df_orders_areas_res['area_name'] = df_orders_areas_res['area_name'].fillna('0')
    df_orders_areas_res = df_orders_areas_res.groupby(['timestamp_hour', 'city_id', 'area_id', 'area_name']) \
        .agg({'id': 'count',
              'ride_amount': 'sum',
              'discount': 'sum',
              'bike_discount_amount': 'sum', 'subscription_price': 'sum'}) \
        .rename(columns={'id': 'poezdok',
                         'ride_amount': 'obzchaya_stoimost',
                         'discount': 'oplacheno_bonusami',
                         'bike_discount_amount': 'skidka',
                         'subscription_price': 'abon'}) \
        .reset_index()
    # Выгрузка показателей для распределения
    select_distr = '''
        WITH dolgi AS (
            SELECT
                dolgi.city_id ,
                SUM(dolgi.debit_cash) AS dolgi
            FROM 
            (
                SELECT
                    dolgi.city_id ,
                    tpd.debit_cash
                FROM damir.t_payment_details tpd 
                RIGHT JOIN (
                    SELECT 
                        tbu."date" ,
                        tbu.id,
                        tbu.uid,
                        tbu.bid ,
                        tb.city_id ,
                        tbu.ride_amount 
                    FROM damir.t_bike_use tbu
                    LEFT JOIN t_bike tb ON tbu.bid = tb.id
                    WHERE tbu.ride_status = 2 
                        AND to_timestamp( tbu.start_time) >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                        --AND to_timestamp( tbu.start_time) >= '2025-12-18'::date
                    ) AS dolgi ON tpd.ride_id = dolgi.id 
                ) AS dolgi
            GROUP BY dolgi.city_id
        ),
        vyruchka_s_abonementov AS (
            SELECT 
                distr_poezdki_po_gorodam.city_id,
                sum_uspeh_abon.vyruchka_s_abonementov * distr_poezdki_po_gorodam.coef_goroda AS vyruchka_s_abonementov
            FROM (
                -- высчитываю пропорции по поездкам
                SELECT 
                    distr_poezdki_po_gorodam.start_time,
                    distr_poezdki_po_gorodam.city_id,
                    distr_poezdki_po_gorodam.poezdok,
                    -- Приведение к numeric важно, чтобы результат деления не был 0
                    distr_poezdki_po_gorodam.poezdok::numeric / SUM(distr_poezdki_po_gorodam.poezdok) OVER (PARTITION BY distr_poezdki_po_gorodam.start_time) AS coef_goroda
                FROM 
                    (
                    SELECT 
                        TO_CHAR(TO_TIMESTAMP(t_bike_use.start_time), 'YYYY-MM-DD') AS start_time,
                        t_bike.city_id,
                        COUNT(t_bike_use.ride_amount) AS poezdok
                    FROM damir.t_bike_use
                    LEFT JOIN damir.t_bike ON t_bike_use.bid = t_bike.id
                    WHERE t_bike_use.ride_status != 5 
                        AND TO_TIMESTAMP(t_bike_use.start_time) >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                        --AND TO_TIMESTAMP(t_bike_use.start_time) >= '2025-12-18'::date
                    GROUP BY 1, 2
                    ) 
                    AS distr_poezdki_po_gorodam
            ) AS distr_poezdki_po_gorodam
            LEFT JOIN (
                SELECT 
                    TO_CHAR(t_trade.date, 'YYYY-MM-DD') AS start_time,
                    SUM(COALESCE(t_trade.amount, 0)) AS vyruchka_s_abonementov
                FROM damir.t_trade
                WHERE t_trade.type = 6 
                    AND t_trade.status = 1 
                    AND t_trade.date >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                    --AND t_trade.date >= '2025-12-18'::date
                GROUP BY 1
                ) AS sum_uspeh_abon
                ON distr_poezdki_po_gorodam.start_time = sum_uspeh_abon.start_time
        ),
        sum_mnogor_abon AS (
            SELECT 
                distr_poezdki_po_gorodam.city_id,
                sum_mnogor_abon.sum_mnogor_abon * distr_poezdki_po_gorodam.coef_goroda AS sum_mnogor_abon
            FROM (
                -- высчитываю пропорции по поездкам
                SELECT 
                    dp.start_time,
                    dp.city_id,
                    dp.poezdok,
                    dp.poezdok::numeric / NULLIF(SUM(COALESCE(dp.poezdok, 0)) OVER (PARTITION BY dp.start_time), 0) AS coef_goroda
                FROM (
                    SELECT 
                        TO_CHAR(TO_TIMESTAMP(t_bike_use.start_time), 'YYYY-MM-DD') AS start_time,
                        t_bike.city_id,
                        COUNT(t_bike_use.ride_amount) AS poezdok
                    FROM damir.t_bike_use
                    LEFT JOIN damir.t_bike ON t_bike_use.bid = t_bike.id
                    WHERE t_bike_use.ride_status != 5 
                        AND TO_TIMESTAMP(t_bike_use.start_time) >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                        --AND TO_TIMESTAMP(t_bike_use.start_time) >= '2025-12-18'::date
                    GROUP BY 1, 2
                ) AS dp
            ) AS distr_poezdki_po_gorodam
            LEFT JOIN (
                SELECT 
                    TO_CHAR(t_subscription_mapping.start_time, 'YYYY-MM-DD') AS start_time,
                    SUM(COALESCE(t_subscription.price, 0)) AS sum_mnogor_abon
                FROM damir.t_subscription_mapping
                LEFT JOIN damir.t_subscription ON t_subscription_mapping.subscription_id = t_subscription.id
                WHERE 
                    t_subscription_mapping.start_time >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                    --t_subscription_mapping.start_time >= '2025-12-18'::date 
                GROUP BY 1
            ) AS sum_mnogor_abon
            ON distr_poezdki_po_gorodam.start_time = sum_mnogor_abon.start_time
        )
        SELECT 
            COALESCE(dolgi.city_id , vyruchka_s_abonementov.city_id , sum_mnogor_abon.city_id ) AS city_id,
            COALESCE(dolgi.dolgi, 0) AS dolgi,
            vyruchka_s_abonementov.vyruchka_s_abonementov ,
            sum_mnogor_abon.sum_mnogor_abon
        FROM dolgi
        FULL JOIN vyruchka_s_abonementov ON dolgi.city_id = vyruchka_s_abonementov.city_id 
        FULL JOIN sum_mnogor_abon ON COALESCE(dolgi.city_id , vyruchka_s_abonementov.city_id ) = sum_mnogor_abon.city_id 
    '''
    df_distr = pd.read_sql(select_distr, engine_postgresql)

    # Соединяю основную таблицу с таблицей distr
    df_orders_areas_res = df_orders_areas_res.merge(df_distr, how='left', on='city_id')
    df_orders_areas_res['dolgi'] = df_orders_areas_res['dolgi'].fillna(0)
    df_orders_areas_res['vyruchka_s_abonementov'] = df_orders_areas_res['vyruchka_s_abonementov'].fillna(0)
    df_orders_areas_res['sum_mnogor_abon'] = df_orders_areas_res['sum_mnogor_abon'].fillna(0)
    df_orders_areas_res = df_orders_areas_res.assign(
        cum_poezdok=df_orders_areas_res.groupby('city_id')['poezdok'].transform('sum'))
    df_orders_areas_res['dolgi_res'] = df_orders_areas_res['dolgi'] * df_orders_areas_res['poezdok'] / \
                                       df_orders_areas_res['cum_poezdok']
    df_orders_areas_res['vyruchka_s_abonementov_res'] = df_orders_areas_res['vyruchka_s_abonementov'] * \
                                                        df_orders_areas_res['poezdok'] / df_orders_areas_res[
                                                            'cum_poezdok']
    df_orders_areas_res['sum_mnogor_abon_res'] = df_orders_areas_res['sum_mnogor_abon'] * df_orders_areas_res[
        'poezdok'] / df_orders_areas_res['cum_poezdok']
    # Соединяю КВТ и orders
    df_orders_kvt_area_res = df_kvt_area_res.merge(df_orders_areas_res, how='left',
                                                   on=['timestamp_hour', 'city_id', 'area_id', 'area_name'])
    df_orders_kvt_area_res = df_orders_kvt_area_res[['timestamp_hour',
                                                     'city_id',
                                                     'area_id',
                                                     'area_name',
                                                     'kvt',
                                                     'poezdok',
                                                     'obzchaya_stoimost',
                                                     'oplacheno_bonusami',
                                                     'skidka',
                                                     'abon',
                                                     'dolgi_res',
                                                     'vyruchka_s_abonementov_res',
                                                     'sum_mnogor_abon_res']]
    df_orders_kvt_area_res = df_orders_kvt_area_res \
        .rename(columns={'dolgi_res': 'dolgi',
                         'vyruchka_s_abonementov_res': 'vyruchka_s_abonementov',
                         'sum_mnogor_abon_res': 'sum_mnogor_abon'})
    df_orders_kvt_area_res['poezdok'] = df_orders_kvt_area_res['poezdok'].fillna(0).astype(float)
    df_orders_kvt_area_res['obzchaya_stoimost'] = df_orders_kvt_area_res['obzchaya_stoimost'].fillna(0).astype(float)
    df_orders_kvt_area_res['oplacheno_bonusami'] = df_orders_kvt_area_res['oplacheno_bonusami'].fillna(0).astype(float)
    df_orders_kvt_area_res['skidka'] = df_orders_kvt_area_res['skidka'].fillna(0).astype(float)
    df_orders_kvt_area_res['abon'] = df_orders_kvt_area_res['abon'].fillna(0).astype(float)
    df_orders_kvt_area_res['dolgi'] = df_orders_kvt_area_res['dolgi'].fillna(0)
    df_orders_kvt_area_res['vyruchka_s_abonementov'] = df_orders_kvt_area_res['vyruchka_s_abonementov'].fillna(0)
    df_orders_kvt_area_res['sum_mnogor_abon'] = df_orders_kvt_area_res['sum_mnogor_abon'].fillna(0)

    df_orders_kvt_area_res['add_time'] = pd.Timestamp.now()

    x = df_orders_kvt_area_res.groupby('timestamp_hour').agg({'kvt': 'sum'})
    print('kvt3' + str(x))

    df_orders_kvt_area_res.to_sql("t_area_revenue_stats2", engine_postgresql, if_exists="append", index=False)
    print('Таблица t_area_revenue_stats2 успешно обновлена!')

    # Выгрузка t_area_revenue_stats2 pandas. Конец


if __name__ == "__main__":
    main()
