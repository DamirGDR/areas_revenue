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
    print('Время aws: {}'.format(datetime.datetime.now()))
    # Выгрузка за сегодня из MySQL
    select_vni_total = """
    -- Из Datalens
	-- ВНИ Общий
    WITH three_left_cols AS (
        SELECT 
            -- t_bike_use.id ,
            DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') AS  'start_time',
            COUNT(t_bike_use.ride_amount) AS 'poezdok',
            SUM(IFNULL(t_bike_use.ride_amount,0)) AS 'obzchaya_stoimost',
            SUM(IFNULL(t_bike_use.discount,0)) AS 'oplacheno_bonusami',
            SUM(IFNULL(t_bike_use.duration,0)) / 60 AS 'obschee_vremya_min',
            SUM(IFNULL(tpd.bike_discount_amount,0)) AS skidka 
            FROM shamri.t_bike_use
        LEFT JOIN shamri.t_payment_details tpd ON t_bike_use.id = tpd.ride_id 
        WHERE t_bike_use.ride_status!=5 AND DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') >= '2024-07-21'
        GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')
    ),
    sum_uspeh_abon AS(
        SELECT 
            DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
            sum(IFNULL(t_trade.amount,0)) AS vyruchka_s_abonementov
        FROM t_trade
        WHERE t_trade.`type` = 6 AND t_trade.status = 1
        GROUP BY start_time
        ),
    sum_mnogor_abon AS (
        SELECT 
            DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d') AS start_time,
            sum(IFNULL(t_subscription.price,0)) AS sum_mnogor_abon
        FROM t_subscription_mapping
        LEFT JOIN t_subscription ON t_subscription_mapping.subscription_id = t_subscription.id
        GROUP BY DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d')
    ),
    kvt AS (
        SELECT 
            DATE_FORMAT(FROM_UNIXTIME(t_bike.heart_time), '%Y-%m-%d') AS start_time,
            COUNT(t_bike.id) AS kvt
        FROM t_bike
        WHERE TIMESTAMPDIFF(SECOND, now(), DATE_FORMAT(FROM_UNIXTIME(t_bike.g_time), '%Y-%m-%d %H:%m:%s')) < 900 AND t_bike.error_status IN (0, 7)
        GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike.heart_time), '%Y-%m-%d')
        LIMIT 1
    ),
    vyruchka_payTabs_stripe AS ( 
    SELECT
        vyruchka_v_statuse_1.start_time,
        vyruchka_v_statuse_1.vyruchka_v_statuse_1,
        IFNULL(vozvraty.vozvraty,0) AS 'vozvraty', 
        chastichno_vozvrascheny.chastichno_vozvrascheny AS 'chastichno_vozvrascheny',
        IFNULL(vyruchka_v_statuse_1.vyruchka_v_statuse_1,0) + IFNULL(chastichno_vozvrascheny.chastichno_vozvrascheny,0) AS 'vyruchka_payTabs',
        IFNULL(stripe_1.stripe_1, 0) AS 'stripe_1',
        IFNULL(stripe_4.stripe_4, 0) AS 'stripe_4',
        IFNULL(stripe_1.stripe_1, 0) - IFNULL(stripe_4.stripe_4, 0) AS 'vyruchka_stripe'
    FROM 
        (
        SELECT 
            DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
            SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vyruchka_v_statuse_1'
        FROM t_trade
        WHERE t_trade.status=1 AND t_trade.way=26 AND t_trade.`type` IN (1,2,6,7)
        GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
        ) AS vyruchka_v_statuse_1
    LEFT JOIN 	
        (SELECT 
            DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
            SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vozvraty'
         FROM t_trade
         WHERE t_trade.status=4 AND t_trade.way=26
         GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
    ) AS vozvraty ON vyruchka_v_statuse_1.start_time=vozvraty.start_time
    LEFT JOIN 
        (SELECT 
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
            SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'stripe_1'
         FROM t_trade
         WHERE t_trade.status=1 AND t_trade.way=6
         GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
        ) AS stripe_1 ON vyruchka_v_statuse_1.start_time=stripe_1.start_time
    LEFT JOIN 
        (SELECT 
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
            SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'stripe_4'
         FROM t_trade
         WHERE t_trade.status=4 AND t_trade.way=6
         GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
        ) AS stripe_4 ON vyruchka_v_statuse_1.start_time=stripe_4.start_time
    LEFT JOIN 
        (
        SELECT 
            DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
            SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'chastichno_vozvrascheny'
        FROM t_trade
        WHERE t_trade.status=3
        GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
        ) AS chastichno_vozvrascheny ON vyruchka_v_statuse_1.start_time = chastichno_vozvrascheny.start_time
    ),
    user_v_den_register AS (
    SELECT 
        DATE_FORMAT(t_user.register_date, '%Y-%m-%d') AS start_time,
        COUNT(t_user.id) AS 'user_v_den_register'
    FROM t_user
    GROUP BY DATE_FORMAT(t_user.register_date, '%Y-%m-%d')
    ),
    kolichestvo_novyh_s_1_poezdkoy AS ( 
        SELECT
            register_date_as_start_date.start_date,
            COUNT(DISTINCT register_date_as_start_date.uid) AS 'kolichestvo_novyh_s_1_poezdkoy',
            COUNT(register_date_as_start_date.ride_amount) AS 'kolichestvo_poezdok_vsego'
        FROM
            (SELECT 
                DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) AS start_date, 
                t_bike_use.*,
                register_users.register_date,
                register_users.id AS register_user_id
            FROM t_bike_use
            LEFT JOIN (
                SELECT 
                    DATE(DATE_FORMAT(t_user.register_date, '%Y-%m-%d')) AS register_date, 
                    t_user.id
                FROM t_user
                                ) AS register_users
            ON t_bike_use.uid=register_users.id
            WHERE DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) = DATE(DATE_FORMAT(register_users.register_date, '%Y-%m-%d')) 
            AND t_bike_use.ride_status!=5
            ) AS register_date_as_start_date
        GROUP BY register_date_as_start_date.start_date
    ),
    dolgi AS (
        SELECT 
            DATE_FORMAT(t_payment_details.created, '%Y-%m-%d') AS 'create_debit_date', 
            SUM(IFNULL(t_payment_details.debit_cash,0)) AS 'dolgi'
        FROM t_payment_details
        RIGHT JOIN (
            SELECT 
                t_bike_use.*
            FROM t_bike_use
            WHERE t_bike_use.ride_status = 2
            ORDER BY t_bike_use.id DESC
        ) AS dolgovye_poezdki
        ON t_payment_details.user_id = dolgovye_poezdki.uid AND t_payment_details.ride_id = dolgovye_poezdki.id
        GROUP BY DATE_FORMAT(t_payment_details.created, '%Y-%m-%d')
    )
    SELECT 
        NOW() AS 'timestamp',
        -- CAST(DATE_FORMAT(three_left_cols.start_time, '%Y-%m-%d %h:%m:%s') AS datetime) AS 'day_', 
        IFNULL(three_left_cols.poezdok,0)  AS 'poezdok',
        IFNULL(three_left_cols.poezdok / kvt.kvt,0) AS 'poezdok_v_srednem_na_samokat',
        IFNULL((IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)),0) / IFNULL(kvt.kvt,0) AS 'vyruchka_sim',
        (IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)) / IFNULL(three_left_cols.poezdok,0) AS 'srednyaa_cena_poezdki',
        -- IFNULL(three_left_cols.obzchaya_stoimost,0) AS 'obschaya_stoimost',
        -- IFNULL(dolgi.dolgi,0) AS 'dolgi',
        IFNULL(three_left_cols.oplacheno_bonusami,0) AS 'oplacheno_bonusami',
        IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) AS 'vyruchka',
        IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) AS 'vyruchka_bez_bonusov',
        IFNULL(three_left_cols.skidka,0) AS 'skidka',
        IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) AS 'vyruchka_s_abonementov', 
        -- IFNULL(sum_mnogor_abon.sum_mnogor_abon,0) AS 'vyruchka_s_mnogor_abonementov',
        IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(three_left_cols.skidka,0) AS 'vyruchka_bez_bonusov_vyruchka_s_abonementov',
        SUM(IFNULL(three_left_cols.obzchaya_stoimost, 0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'vni',
        SUM(IFNULL(three_left_cols.obzchaya_stoimost, 0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(three_left_cols.skidka,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) - SUM(IFNULL(three_left_cols.oplacheno_bonusami,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'vni_bez_bonusov',
        --
        -- vyruchka_payTabs_stripe.vyruchka_v_statuse_1,
        -- vyruchka_payTabs_stripe.vozvraty,
        -- vyruchka_payTabs_stripe.chastichno_vozvrascheny,
        -- IFNULL(vyruchka_payTabs_stripe.vyruchka_v_statuse_1,0) - IFNULL(vyruchka_payTabs_stripe.vozvraty,0) AS 'vyruchka PayTabs_возвраты',
        -- SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_v_statuse_1,0) - IFNULL(vyruchka_payTabs_stripe.vozvraty,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'obsch_PayTabs_возвраты',
        IFNULL(vyruchka_payTabs_stripe.vyruchka_v_statuse_1,0) + IFNULL(vyruchka_payTabs_stripe.chastichno_vozvrascheny,0) AS 'vyruchka PayTabs',
        SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_v_statuse_1,0) + IFNULL(vyruchka_payTabs_stripe.chastichno_vozvrascheny,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'obsch_PayTabs',
        --
        IFNULL(vyruchka_payTabs_stripe.vyruchka_stripe,0) AS 'vyruchka_Stripe',
        SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_stripe,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'obsch_Stripe',
        SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_payTabs,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) + SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_stripe,0) + IFNULL(vyruchka_payTabs_stripe.chastichno_vozvrascheny,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'obsch_PayTabs_i_obsch_Stripe',
        IFNULL(kvt.kvt,0) AS 'kvt',
        IFNULL(user_v_den_register.user_v_den_register,0) AS 'user_v_den',
        SUM(IFNULL(user_v_den_register.user_v_den_register,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'user_v_den_obshc',
        IFNULL(three_left_cols.obschee_vremya_min,0) AS 'obcshee_vrmeya_min',
        IFNULL(three_left_cols.obschee_vremya_min,0) / IFNULL(three_left_cols.poezdok,0) AS 'srednyee_vremya_poezdki',
        IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy,0) AS 'iz_nih_usero_sovershivshih_poezdki_vsego',
        IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_poezdok_vsego,0) AS 'kol_vo_poezdok_vsego',
        IFNULL(ROUND((IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy,0) / IFNULL(user_v_den_register.user_v_den_register,0)) * 100, 2),0) AS 'proniknovenie',
        IFNULL(ROUND(IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_poezdok_vsego,0) / IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy,0), 2),0) AS 'poezdok_novyi_user'
    FROM three_left_cols
    LEFT JOIN sum_uspeh_abon ON three_left_cols.start_time = sum_uspeh_abon.start_time
    LEFT JOIN sum_mnogor_abon ON three_left_cols.start_time = sum_mnogor_abon.start_time
    LEFT JOIN kvt ON three_left_cols.start_time = kvt.start_time
    LEFT JOIN vyruchka_payTabs_stripe ON three_left_cols.start_time = vyruchka_payTabs_stripe.start_time
    LEFT JOIN user_v_den_register ON three_left_cols.start_time = user_v_den_register.start_time
    LEFT JOIN kolichestvo_novyh_s_1_poezdkoy ON three_left_cols.start_time = kolichestvo_novyh_s_1_poezdkoy.start_date
    LEFT JOIN dolgi ON three_left_cols.start_time = dolgi.create_debit_date
    -- WHERE DATE_FORMAT(NOW(), '%Y-%m-%d') = three_left_cols.start_time
    ORDER BY three_left_cols.start_time DESC
    LIMIT 1
	    """

    url = get_mysql_url()
    url = sa.engine.make_url(url)
    url = url.set(drivername="mysql+mysqlconnector")
    engine_mysql = sa.create_engine(url)
    df_vni = pd.read_sql(select_vni_total, engine_mysql)

    # Загрузка за сегодня в Postgres
    url = get_postgres_url()
    url = sa.engine.make_url(url)
    url = url.set(drivername="postgresql+psycopg")
    engine_postgresql = sa.create_engine(url)
    df_vni.to_sql("vni_total", engine_postgresql, if_exists="append", index=False)
    
    print('Got it: VNI_total')

    select_vni_cities = '''
    WITH three_left_cols AS 
    (    
        SELECT 
            three_left_cols.start_time,
            three_left_cols.city_id,
            three_left_cols.poezdok,
            three_left_cols.obzchaya_stoimost,
            SUM(IFNULL(three_left_cols.obzchaya_stoimost, 0)) OVER(PARTITION BY three_left_cols.city_id ORDER BY three_left_cols.start_time) AS 'obzchaya_stoimost_ni',
            SUM(IFNULL(three_left_cols.oplacheno_bonusami, 0)) OVER(PARTITION BY three_left_cols.city_id ORDER BY three_left_cols.start_time) AS 'oplacheno_bonusami_ni',
            three_left_cols.oplacheno_bonusami,
            three_left_cols.obschee_vremya_min,
            IFNULL(three_left_cols.skidka,0) AS 'skidka',
            SUM(IFNULL(three_left_cols.skidka, 0)) OVER(PARTITION BY three_left_cols.city_id ORDER BY three_left_cols.start_time) AS 'skidka_ni'
        FROM 
            (
                SELECT 
                    DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') AS  'start_time',
                    t_bike.city_id,
                    COUNT(t_bike_use.ride_amount) AS 'poezdok',
                    SUM(IFNULL(t_bike_use.ride_amount,0)) AS 'obzchaya_stoimost',
                    SUM(IFNULL(t_bike_use.discount,0)) AS 'oplacheno_bonusami',
                    SUM(IFNULL(t_bike_use.duration,0)) / 60 AS 'obschee_vremya_min',
                    SUM(IFNULL(tpd.bike_discount_amount,0)) AS skidka
                FROM shamri.t_bike_use
                LEFT JOIN t_bike ON t_bike_use.bid = t_bike.id
                LEFT JOIN shamri.t_payment_details tpd ON t_bike_use.id = tpd.ride_id
                WHERE 
                t_bike_use.ride_status!=5 
                AND 
                DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') >= '2024-07-21'
                GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d'), t_bike.city_id
            ) AS three_left_cols
        ORDER BY three_left_cols.start_time DESC
    ),
    sum_uspeh_abon AS (
        SELECT 
            distr_poezdki_po_gorodam.start_time,
            distr_poezdki_po_gorodam.city_id,
            distr_poezdki_po_gorodam.poezdok,
            distr_poezdki_po_gorodam.coef_goroda,
            sum_uspeh_abon.vyruchka_s_abonementov AS 'vyruchka_s_abonementov_po_vsem_gorodam',
            distr_poezdki_po_gorodam.coef_goroda AS 'coef_goroda_vyruchka_s_abonementov_po_vsem_gorodam',
            sum_uspeh_abon.vyruchka_s_abonementov * distr_poezdki_po_gorodam.coef_goroda AS 'vyruchka_s_abonementov',
            SUM(IFNULL(sum_uspeh_abon.vyruchka_s_abonementov, 0) * IFNULL(distr_poezdki_po_gorodam.coef_goroda,0)) OVER (PARTITION BY distr_poezdki_po_gorodam.city_id ORDER BY distr_poezdki_po_gorodam.start_time) AS 'vyruchka_s_abonementov_ni'
        FROM (
            -- высчитываю пропорции по поездкам
            SELECT 
                distr_poezdki_po_gorodam.start_time,
                distr_poezdki_po_gorodam.city_id,
                distr_poezdki_po_gorodam.poezdok,
                distr_poezdki_po_gorodam.poezdok / SUM(distr_poezdki_po_gorodam.poezdok) OVER (PARTITION BY distr_poezdki_po_gorodam.start_time) AS 'coef_goroda'
            FROM 
                (
                SELECT 
                    DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') AS  'start_time',
                    t_bike.city_id,
                    COUNT(t_bike_use.ride_amount) AS 'poezdok'
                FROM shamri.t_bike_use
                LEFT JOIN t_bike ON t_bike_use.bid = t_bike.id
                WHERE t_bike_use.ride_status!=5 
                    AND DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') >= '2024-07-21'
                GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d'), t_bike.city_id
                ORDER BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') DESC
                ) 
                AS distr_poezdki_po_gorodam
            ORDER BY distr_poezdki_po_gorodam.start_time DESC
        ) AS distr_poezdki_po_gorodam
        LEFT JOIN (
            SELECT 
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                sum(IFNULL(t_trade.amount,0)) AS vyruchka_s_abonementov
            FROM shamri.t_trade
            WHERE t_trade.`type` = 6 
                AND t_trade.status = 1 
                 AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
            ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC
        ) AS sum_uspeh_abon
        ON distr_poezdki_po_gorodam.start_time = sum_uspeh_abon.start_time
        ORDER BY distr_poezdki_po_gorodam.start_time DESC
    ),
    sum_mnogor_abon AS (
        SELECT 
            distr_poezdki_po_gorodam.start_time,
            distr_poezdki_po_gorodam.city_id,
            distr_poezdki_po_gorodam.poezdok,
            distr_poezdki_po_gorodam.coef_goroda,
            sum_mnogor_abon.sum_mnogor_abon AS 'sum_mnogor_abon_po_vsem_gorodam',
            sum_mnogor_abon.sum_mnogor_abon * distr_poezdki_po_gorodam.coef_goroda AS 'sum_mnogor_abon',
            SUM(IFNULL(sum_mnogor_abon.sum_mnogor_abon,0) * IFNULL(distr_poezdki_po_gorodam.coef_goroda,0)) OVER (PARTITION BY distr_poezdki_po_gorodam.city_id ORDER BY distr_poezdki_po_gorodam.start_time) AS 'sum_mnogor_abon_ni'
        FROM (
    -- высчитываю пропорции по поездкам
        SELECT 
            distr_poezdki_po_gorodam.start_time,
            distr_poezdki_po_gorodam.city_id,
            distr_poezdki_po_gorodam.poezdok,
            distr_poezdki_po_gorodam.poezdok / SUM(IFNULL(distr_poezdki_po_gorodam.poezdok,0)) OVER (PARTITION BY distr_poezdki_po_gorodam.start_time) AS 'coef_goroda'
        FROM 
            (SELECT 
                DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') AS  'start_time',
                t_bike.city_id,
                COUNT(t_bike_use.ride_amount) AS 'poezdok'
            FROM shamri.t_bike_use
            LEFT JOIN t_bike ON t_bike_use.bid = t_bike.id
            WHERE t_bike_use.ride_status!=5 
                AND DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d'), t_bike.city_id
            ORDER BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') DESC) 
            AS distr_poezdki_po_gorodam
        ORDER BY distr_poezdki_po_gorodam.start_time DESC
        ) AS distr_poezdki_po_gorodam
        LEFT JOIN (
            SELECT 
                DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d') AS start_time,
                SUM(IFNULL(t_subscription.price,0)) AS sum_mnogor_abon
            FROM t_subscription_mapping
            LEFT JOIN t_subscription ON t_subscription_mapping.subscription_id = t_subscription.id
            WHERE DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d')
            ORDER BY DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d') DESC
            ) AS sum_mnogor_abon
        ON distr_poezdki_po_gorodam.start_time = sum_mnogor_abon.start_time
        ORDER BY distr_poezdki_po_gorodam.start_time DESC
    ),
    kvt AS (
        SELECT 
            DATE_FORMAT(FROM_UNIXTIME(t_bike.heart_time), '%Y-%m-%d') AS start_time,
            t_bike.city_id,
            COUNT(t_bike.id) AS kvt
        FROM t_bike
        WHERE TIMESTAMPDIFF(SECOND, now(), DATE_FORMAT(FROM_UNIXTIME(t_bike.g_time), '%Y-%m-%d %H:%m:%s')) < 900 
            AND t_bike.error_status IN (0, 7) 
        GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike.heart_time), '%Y-%m-%d'), t_bike.city_id
        HAVING start_time = DATE_FORMAT(NOW(), '%Y-%m-%d')
        ORDER BY t_bike.city_id DESC
    ),
    vyruchka_uspeh_payTabs AS 
    (
        SELECT
            vyruchka_uspeh_payTabs.start_time,
            vyruchka_uspeh_payTabs.city_id,
            vyruchka_uspeh_payTabs.vyruchka_v_statuse_1_payTabs,
            chastichno_vozvrascheny.chastichno_vozvrascheny AS 'chastichno_vozvrascheny'
        FROM
            (SELECT 
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                t_trade.city_id,
                SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vyruchka_v_statuse_1_payTabs'
            FROM t_trade
            WHERE t_trade.status=1 
                AND t_trade.way=26 
                AND t_trade.`type` IN (1,2,6,7)
                AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
            ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC, t_trade.city_id DESC
            ) AS vyruchka_uspeh_payTabs
        LEFT JOIN 
            (
            SELECT
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                t_trade.city_id,
                SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'chastichno_vozvrascheny'
            FROM t_trade
            WHERE t_trade.status=3
                AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
            ) AS chastichno_vozvrascheny ON vyruchka_uspeh_payTabs.start_time = chastichno_vozvrascheny.start_time AND vyruchka_uspeh_payTabs.city_id = chastichno_vozvrascheny.city_id
    ),
    vyruchka_uspeh_payTabs_ni AS 
    (
         SELECT
             vyruchka_uspeh_payTabs.city_id,
             vyruchka_uspeh_payTabs.start_time, 
             IFNULL(vyruchka_uspeh_payTabs.chastichno_vozvrascheny,0) AS 'chastichno_vozvrascheny',
            vyruchka_uspeh_payTabs.vyruchka_v_statuse_1_payTabs_ni AS 'vyruchka_v_statuse_1_payTabs_ni'
         FROM 
            (
            SELECT
                vyruchka_uspeh_payTabs.start_time,
                vyruchka_uspeh_payTabs.city_id,
                vyruchka_uspeh_payTabs.vyruchka_v_statuse_1_payTabs,
                chastichno_vozvrascheny.chastichno_vozvrascheny,
                SUM(IFNULL(vyruchka_uspeh_payTabs.vyruchka_v_statuse_1_payTabs,0) + IFNULL(chastichno_vozvrascheny.chastichno_vozvrascheny,0)) OVER (PARTITION BY vyruchka_uspeh_payTabs.city_id ORDER BY vyruchka_uspeh_payTabs.start_time) AS 'vyruchka_v_statuse_1_payTabs_ni'
            FROM
                (SELECT 
                    DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                    t_trade.city_id,
                    SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vyruchka_v_statuse_1_payTabs'
                FROM t_trade
                WHERE t_trade.status=1 
                    AND t_trade.way=26 
                    AND t_trade.`type` IN (1,2,6,7) 
                    AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21' 
                GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
                ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC, t_trade.city_id DESC) AS vyruchka_uspeh_payTabs
            LEFT JOIN 
            (
                SELECT
                    DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                    t_trade.city_id,
                    SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'chastichno_vozvrascheny'
                FROM t_trade
                WHERE t_trade.status=3
                    AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
                GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
                ) AS chastichno_vozvrascheny ON vyruchka_uspeh_payTabs.start_time = chastichno_vozvrascheny.start_time AND vyruchka_uspeh_payTabs.city_id = chastichno_vozvrascheny.city_id
                )
            AS vyruchka_uspeh_payTabs
    ),
    vosvraty_payTabs AS ( 
        SELECT
            trade.start_time,
            trade.city_id,
            trade.vozvraty_payTabs,
            SUM(IFNULL(trade.vozvraty_payTabs,0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'vozvraty_payTabs_ni'
        FROM
            (SELECT
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                t_trade.city_id,
                SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vozvraty_payTabs'
            FROM t_trade
            WHERE t_trade.status=4 
                 AND t_trade.way=26 
                 AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
            ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade
     ),
     vozvraty_payTabs_ni AS (
         SELECT
             vozvraty_payTabs.city_id,
             vozvraty_payTabs.start_time,
             vozvraty_payTabs.vozvraty_payTabs_ni
         FROM 
            (SELECT
                trade.start_time,
                trade.city_id,
                trade.vozvraty_payTabs,
                SUM(IFNULL(trade.vozvraty_payTabs,0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'vozvraty_payTabs_ni'
            FROM
                (SELECT
                    DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                    t_trade.city_id,
                    SUM(IFNULL(t_trade.account_pay_amount, 0)) AS 'vozvraty_payTabs'
                FROM t_trade
                WHERE t_trade.status=4 
                     AND t_trade.way=26 
                     AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
                GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
                ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade) AS vozvraty_payTabs
     ),
    uspeh_Stripe AS (
        SELECT
            trade.start_time,
            trade.city_id,
            trade.uspeh_Stripe,
            SUM(IFNULL(trade.uspeh_Stripe, 0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'uspeh_Stripe_ni'
        FROM 
            (SELECT 
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                t_trade.city_id,
                SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'uspeh_Stripe'
            FROM t_trade
            WHERE t_trade.status=1 
                 AND t_trade.way=6 
                 AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') > '2024-07-21'
            GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
            ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade
     ),
     uspeh_Stripe_ni AS(
         SELECT 
             uspeh_Stripe.city_id,
             uspeh_Stripe.start_time,
            uspeh_Stripe.uspeh_Stripe,
            uspeh_Stripe.uspeh_Stripe_ni
         FROM 
            (SELECT
                trade.start_time,
                trade.city_id,
                trade.uspeh_Stripe,
                SUM(IFNULL(trade.uspeh_Stripe, 0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'uspeh_Stripe_ni'
            FROM 
                (SELECT 
                    DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                    t_trade.city_id,
                    SUM(t_trade.account_pay_amount) AS 'uspeh_Stripe'
                FROM t_trade
                WHERE t_trade.status=1 
                     AND t_trade.way=6 
                     AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
                GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
                ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade) 
            AS uspeh_Stripe
     ),
     vozvraty_Stripe AS (
     -- Города - ок
        SELECT
            trade.start_time,
            trade.city_id,
            trade.vozvraty_Stripe,
            SUM(IFNULL(trade.vozvraty_Stripe, 0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'vozvraty_Stripe_ni'
        FROM
            (SELECT 
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                t_trade.city_id,
                SUM(t_trade.account_pay_amount) AS 'vozvraty_Stripe'
             FROM t_trade
             WHERE t_trade.status=4 
                 AND t_trade.way=6 
                 AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
             GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
             ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade
     ),
     vozvraty_Stripe_ni AS (
         SELECT
             vozvraty_Stripe.city_id,
             vozvraty_Stripe.start_time,
             vozvraty_Stripe.vozvraty_Stripe_ni
         FROM 
            (SELECT
                trade.start_time,
                trade.city_id,
                trade.vozvraty_Stripe,
                SUM(IFNULL(trade.vozvraty_Stripe, 0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'vozvraty_Stripe_ni'
            FROM
                (SELECT 
                    DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                    t_trade.city_id,
                    SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vozvraty_Stripe'
                 FROM t_trade
                 WHERE t_trade.status=4 
                     AND t_trade.way=6 
                     AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
                 GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
                 ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade)
            AS vozvraty_Stripe
     ),
    user_v_den_register AS (
        SELECT
            res_tab.start_time ,
            res_tab.city_id ,
            res_tab.user_v_den_register ,
            SUM(user_v_den_register) OVER (PARTITION BY res_tab.city_id ORDER BY res_tab.start_time) AS user_v_den_register_ni
        FROM 
        (
            SELECT 
                DATE_FORMAT(t_user.register_date, '%Y-%m-%d') AS start_time,
                t_user.registration_city_id AS city_id ,
                COUNT(t_user.id) AS 'user_v_den_register'
            FROM t_user
            WHERE DATE_FORMAT(t_user.register_date, '%Y-%m-%d') >= '2025-11-01'
            GROUP BY DATE_FORMAT(t_user.register_date, '%Y-%m-%d'), t_user.registration_city_id
            UNION ALL
            SELECT 
            '2025-10-31' AS start_time, 21 AS city_id, 4062 AS user_v_den_register
            UNION ALL
            SELECT '2025-10-31', 19, 2153
            UNION ALL
            SELECT '2025-10-31', 22, 2142
            UNION ALL
            SELECT '2025-10-31', 15, 4535
            UNION ALL
            SELECT '2025-10-31', 17, 11471
            UNION ALL
            SELECT '2025-10-31', 25, 965
            UNION ALL
            SELECT '2025-10-31', 18, 3394
            UNION ALL
            SELECT '2025-10-31', 16, 1718
            UNION ALL
            SELECT '2025-10-31', 11, 3562
            UNION ALL
            SELECT '2025-10-31', 13, 2246
            UNION ALL
            SELECT '2025-10-31', 20, 1275
        ) AS res_tab
    ),
    kolichestvo_novyh_s_1_poezdkoy AS 
        ( 
        SELECT 
            DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) AS start_date, 
            register_users.city_id,
            COUNT(DISTINCT t_bike_use.uid) AS 'kolichestvo_novyh_s_1_poezdkoy',
            COUNT(t_bike_use.ride_amount) AS 'kolichestvo_poezdok_vsego'
        FROM t_bike_use
        LEFT JOIN (
            SELECT 
                DATE(DATE_FORMAT(t_user.register_date, '%Y-%m-%d')) AS register_date, 
                t_user.id,
                t_user.city_id
            FROM t_user
                            ) AS register_users
        ON t_bike_use.uid=register_users.id
        WHERE DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) = DATE(DATE_FORMAT(register_users.register_date, '%Y-%m-%d'))
            AND DATE(DATE_FORMAT(register_users.register_date, '%Y-%m-%d')) >= '2024-07-21'
            AND DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) >= '2024-07-21'
            AND t_bike_use.ride_status != 5
        GROUP BY DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')), register_users.city_id   
    ),
    dolgi AS (
        SELECT 
            dolgi.create_debit_date,
            dolgi.city_id,
            dolgi.dolgi,
            SUM(IFNULL(dolgi.dolgi,0)) OVER (PARTITION BY dolgi.city_id ORDER BY dolgi.create_debit_date) AS 'dolgi_ni'
        FROM 
            (SELECT 
                DATE_FORMAT(t_payment_details.created, '%Y-%m-%d') AS 'create_debit_date', 
                dolgovye_poezdki.city_id,
                SUM(IFNULL(t_payment_details.debit_cash,0)) AS 'dolgi'
            FROM t_payment_details
            RIGHT JOIN (
                    SELECT 
                        t_bike_use.id,
                        t_bike_use.uid,
                        t_bike_use.bid,
                        t_bike.city_id
                    FROM t_bike_use
                    LEFT JOIN t_bike ON t_bike_use.bid = t_bike.id
                    WHERE t_bike_use.ride_status = 2 
                        AND DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) >= '2024-07-21'
                        ) AS dolgovye_poezdki
            ON t_payment_details.user_id = dolgovye_poezdki.uid AND t_payment_details.ride_id = dolgovye_poezdki.id
            WHERE DATE_FORMAT(t_payment_details.created, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_payment_details.created, '%Y-%m-%d'), dolgovye_poezdki.city_id
            ORDER BY DATE_FORMAT(t_payment_details.created, '%Y-%m-%d') DESC
            ) AS dolgi
    ),
    t_city_with_noname AS (
        SELECT
            calendar.gen_date AS 'start_day',
            t_city_with_noname.id,
            t_city_with_noname.name
        FROM (
        select * from 
            (select adddate('1970-01-01',t4*10000 + t3*1000 + t2*100 + t1*10 + t0) gen_date from
             (select 0 t0 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t0,
             (select 0 t1 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t1,
             (select 0 t2 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t2,
             (select 0 t3 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t3,
             (select 0 t4 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t4) v
            where gen_date between '2024-07-21' and DATE_FORMAT(NOW(), '%Y-%m-%d')
        ) AS calendar
        CROSS JOIN (
            SELECT 
                t_city.*
            FROM t_city
            UNION
            SELECT 
                0 AS 'id', 
                'Unknown' AS 'name', 
                '' AS 'note', 
                NULL AS 'code', 
                '' AS 'area_detail', 
                0 AS 'area_lat', 
                0 AS 'area_lng', 
                NULL AS 'currency', 
                '00:00' AS 'start_time', 
                '00:24' AS 'end_time', 
                NULL AS 'invite_code', 
                '{"max_speed_limit":0}' AS 'extend_info', 
                1 AS 'industry_id'
        ) AS t_city_with_noname
    )
    SELECT
        NOW() AS 'timestamp' ,
        res.id ,
        res.name ,
        res.poezdok ,
        IFNULL(res.poezdok_v_srednem_na_samokat,0) AS poezdok_v_srednem_na_samokat,
        IFNULL(res.vyruchka_sim,0) AS vyruchka_sim,
        IFNULL(res.srednyaa_cena_poezdki,0) AS srednyaa_cena_poezdki,
        res.oplacheno_bonusami ,
        res.vyruchka,
        res.vyruchka_bez_bonusov,
        res.skidka,
        res.vyruchka_s_abonementov,
        res.vyruchka_bez_bonusov_vyruchka_s_abonementov,
        res.vni,
        res.vni_bez_bonusov,
        res.kvt,
        res.user_v_den_register,
        res.user_v_den_obshc,
        res.obcshee_vrmeya_min,
        IFNULL(res.srednyee_vremya_poezdki,0) AS srednyee_vremya_poezdki,
        res.iz_nih_usero_sovershivshih_poezdki_vsego,
        res.kol_vo_poezdok_vsego,
        res.proniknovenie,
        res.poezdok_novyi_user
    FROM
        (
        SELECT 
            NOW() AS 'timestamp',
            t_city_with_noname.start_day ,
            t_city_with_noname.id,
            t_city_with_noname.name,
            IFNULL(three_left_cols.poezdok, 0)  AS 'poezdok',
            IFNULL(three_left_cols.poezdok, 0) / IFNULL(kvt.kvt, 0) AS 'poezdok_v_srednem_na_samokat',
            IFNULL((IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)),0) / IFNULL(kvt.kvt,0) AS 'vyruchka_sim',
            (IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)) / IFNULL(three_left_cols.poezdok,0) AS 'srednyaa_cena_poezdki',
            IFNULL(three_left_cols.oplacheno_bonusami,0) AS 'oplacheno_bonusami',
            IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) AS 'vyruchka',
            IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) AS 'vyruchka_bez_bonusov',
            IFNULL(three_left_cols.skidka,0) AS 'skidka',
            IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) AS 'vyruchka_s_abonementov',
            IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(three_left_cols.skidka,0) AS 'vyruchka_bez_bonusov_vyruchka_s_abonementov',
            IFNULL(three_left_cols.obzchaya_stoimost_ni,0) - IFNULL(dolgi.dolgi_ni,0) - IFNULL(sum_uspeh_abon.vyruchka_s_abonementov_ni,0) + IFNULL(sum_mnogor_abon.sum_mnogor_abon_ni,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov_ni,0) AS 'vni',
            IFNULL(three_left_cols.obzchaya_stoimost_ni,0) - IFNULL(dolgi.dolgi_ni,0) - IFNULL(sum_uspeh_abon.vyruchka_s_abonementov_ni,0) + IFNULL(sum_mnogor_abon.sum_mnogor_abon_ni,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov_ni,0) - IFNULL(three_left_cols.oplacheno_bonusami_ni,0) - IFNULL(three_left_cols.skidka_ni,0) AS 'vni_bez_bonusov',
            IFNULL(kvt.kvt, 0) AS 'kvt',
            IFNULL(user_v_den_register.user_v_den_register, 0) AS 'user_v_den_register',
            SUM(IFNULL(user_v_den_register.user_v_den_register, 0)) OVER (PARTITION BY  t_city_with_noname.id) AS 'user_v_den_obshc',
            -- IFNULL(user_v_den_register.user_v_den_register_ni, 0) AS 'user_v_den_obshc',
            IFNULL(three_left_cols.obschee_vremya_min, 0) AS 'obcshee_vrmeya_min',
            IFNULL(three_left_cols.obschee_vremya_min, 0) / IFNULL(three_left_cols.poezdok, 0) AS 'srednyee_vremya_poezdki',
            IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy, 0) AS 'iz_nih_usero_sovershivshih_poezdki_vsego',
            IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_poezdok_vsego, 0) AS 'kol_vo_poezdok_vsego',
            IFNULL(ROUND((kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy / user_v_den_register.user_v_den_register) * 100, 2), 0) AS 'proniknovenie',
            IFNULL(ROUND(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_poezdok_vsego / kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy, 2), 0) AS 'poezdok_novyi_user'
        FROM t_city_with_noname
        LEFT JOIN kvt ON t_city_with_noname.start_day = kvt.start_time AND t_city_with_noname.id = kvt.city_id
        LEFT JOIN sum_uspeh_abon ON t_city_with_noname.start_day = sum_uspeh_abon.start_time AND t_city_with_noname.id = sum_uspeh_abon.city_id
        LEFT JOIN sum_mnogor_abon ON t_city_with_noname.start_day = sum_mnogor_abon.start_time AND t_city_with_noname.id = sum_mnogor_abon.city_id
        LEFT JOIN vyruchka_uspeh_payTabs ON t_city_with_noname.start_day = vyruchka_uspeh_payTabs.start_time AND t_city_with_noname.id = vyruchka_uspeh_payTabs.city_id
        LEFT JOIN vyruchka_uspeh_payTabs_ni ON t_city_with_noname.start_day = vyruchka_uspeh_payTabs_ni.start_time AND t_city_with_noname.id = vyruchka_uspeh_payTabs_ni.city_id
        LEFT JOIN vosvraty_payTabs ON t_city_with_noname.start_day = vosvraty_payTabs.start_time AND t_city_with_noname.id = vosvraty_payTabs.city_id
        LEFT JOIN vozvraty_payTabs_ni ON t_city_with_noname.start_day = vozvraty_payTabs_ni.start_time AND t_city_with_noname.id = vozvraty_payTabs_ni.city_id
        LEFT JOIN uspeh_Stripe ON t_city_with_noname.start_day = uspeh_Stripe.start_time AND t_city_with_noname.id = uspeh_Stripe.city_id
        LEFT JOIN uspeh_Stripe_ni ON t_city_with_noname.start_day = uspeh_Stripe_ni.start_time AND t_city_with_noname.id = uspeh_Stripe_ni.city_id
        LEFT JOIN vozvraty_Stripe ON t_city_with_noname.start_day = vozvraty_Stripe.start_time AND t_city_with_noname.id = vozvraty_Stripe.city_id
        LEFT JOIN vozvraty_Stripe_ni ON t_city_with_noname.start_day = vozvraty_Stripe_ni.start_time AND t_city_with_noname.id = vozvraty_Stripe_ni.city_id
        LEFT JOIN user_v_den_register ON t_city_with_noname.start_day = user_v_den_register.start_time AND t_city_with_noname.id = user_v_den_register.city_id 
        LEFT JOIN kolichestvo_novyh_s_1_poezdkoy ON t_city_with_noname.start_day = kolichestvo_novyh_s_1_poezdkoy.start_date AND t_city_with_noname.id = kolichestvo_novyh_s_1_poezdkoy.city_id
        LEFT JOIN dolgi ON t_city_with_noname.start_day = dolgi.create_debit_date AND t_city_with_noname.id = dolgi.city_id
        LEFT JOIN three_left_cols ON t_city_with_noname.start_day = three_left_cols.start_time AND t_city_with_noname.id = three_left_cols.city_id
        -- WHERE 
            -- IFNULL(three_left_cols.poezdok, 0) > 0
            -- AND 
        --	t_city_with_noname.start_day = DATE_FORMAT(NOW(), '%Y-%m-%d')
        -- ORDER BY t_city_with_noname.start_day DESC
        ) AS res
    WHERE res.start_day = DATE_FORMAT(NOW(), '%Y-%m-%d')
    '''

    df_cities = pd.read_sql(select_vni_cities, engine_mysql)
    df_cities.to_sql("vni_cities", engine_postgresql, if_exists="append", index=False)

    print('Got it: VNI_cities')

    # # АКБ - начало

    select_df1_all = '''
    -- Собираю из БД MYSQL два склада и их модели
    WITH t_city_sklady AS (
    	SELECT *
    	FROM t_city
    	CROSS JOIN (
    		SELECT DISTINCT t_bike.model
    		FROM t_bike
    		-- WHERE t_bike.model LIKE '%freego%'
    	) AS models
    	-- WHERE t_city.id IN (11,12)
    ),
    uteryany AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    t_bike.model,
    	    COUNT(t_bike.id) AS uteryany
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 6 
        AND t_bike.bike_type = 2 
    	-- AND t_bike.city_id IN (11,12)
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status, t_bike.model
    ),
    v_ozhidanii_activacii AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    t_bike.model,
    	    COUNT(t_bike.id) AS v_ozhidanii_activacii
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 4 
        AND t_bike.bike_type = 2 
    	-- AND t_bike.city_id IN (11,12)
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status, t_bike.model
    ),
    slugebnyi_transport AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    t_bike.model,
    	    COUNT(t_bike.id) AS slugebnyi_transport
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 8 
        AND t_bike.bike_type = 2  
    	-- AND t_bike.city_id IN (11,12)
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status, t_bike.model
    ),
    remont AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    t_bike.model,
    	    COUNT(t_bike.id) AS remont
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 5 
        AND t_bike.bike_type = 2  
    	-- AND t_bike.city_id IN (11,12)
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status, t_bike.model
    ),
    vyveden_iz_ekspluatacii AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    t_bike.model,
    	    COUNT(t_bike.id) AS vyveden_iz_ekspluatacii
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 3 
        AND t_bike.bike_type = 2 
    	-- AND t_bike.city_id IN (11,12) 
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status, t_bike.model
    ),
    mr_user AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    t_bike.model,
    	    COUNT(t_bike.id) AS mr_user
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 2 
        AND t_bike.bike_type = 2 
    	-- AND t_bike.city_id IN (11,12) 
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status, t_bike.model
    ),
    kvt AS (
    	SELECT
    	    t_bike.city_id,
    	    t_bike.model,
    	    COUNT(t_bike.id) AS kvt
    	FROM
    	    t_bike
    	WHERE
    	    TIMESTAMPDIFF(SECOND, FROM_UNIXTIME(t_bike.heart_time), NOW()) < 900
    	    AND t_bike.error_status IN (0, 7)
    	    AND t_bike.bike_type = 2
    	    -- AND t_bike.model LIKE '%freego%' 
    	    -- AND t_bike.city_id IN (11,12)
    	GROUP BY
    	    t_bike.city_id, t_bike.model
    	ORDER BY
    	    t_bike.city_id DESC
    ),
    kvt_offline AS (
    	SELECT
    	    t_bike.city_id,
    	    t_bike.model,
    	    COUNT(t_bike.id) AS kvt_offline
    	FROM
    	    t_bike
    	WHERE
    	    TIMESTAMPDIFF(SECOND, FROM_UNIXTIME(t_bike.heart_time), NOW()) >= 900
    	    AND t_bike.error_status IN (0, 7)
    	    AND t_bike.bike_type = 2
    	    -- AND t_bike.model LIKE '%freego%' 
    	    -- AND t_bike.city_id IN (11,12)
    	GROUP BY
    	    t_bike.city_id, t_bike.model
    	ORDER BY
    	    t_bike.city_id DESC
    ),
    mr_admin AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    t_bike.model,
    	    COUNT(t_bike.id) AS mr_admin
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 1 
        AND t_bike.bike_type = 2 
        -- AND t_bike.model LIKE '%freego%' 
        -- AND t_bike.city_id IN (11,12)
    	GROUP BY
    		t_bike.city_id, t_bike.error_status, t_bike.model
    )
    SELECT
    	t_city_sklady.id AS city_id,
    	t_city_sklady.name,
    	t_city_sklady.model,
    	COALESCE(uteryany.uteryany,0) AS uteryany,
    	COALESCE(v_ozhidanii_activacii.v_ozhidanii_activacii,0) AS v_ozhidanii_activacii,
    	COALESCE(slugebnyi_transport.slugebnyi_transport,0) AS slugebnyi_transport,
    	COALESCE(remont.remont,0) AS remont,
    	COALESCE(vyveden_iz_ekspluatacii.vyveden_iz_ekspluatacii,0) AS vyveden_iz_ekspluatacii,
    	COALESCE(mr_user.mr_user,0) AS mr_user,
    	COALESCE(kvt.kvt,0) AS kvt,
    	COALESCE(mr_admin.mr_admin,0) AS mr_admin,
    	COALESCE(mr_admin.mr_admin,0) + COALESCE(kvt.kvt,0) + COALESCE(kvt_offline.kvt_offline,0) AS fact_park,
    	COALESCE(v_ozhidanii_activacii.v_ozhidanii_activacii,0) + COALESCE(slugebnyi_transport.slugebnyi_transport,0) + COALESCE(remont.remont,0) + COALESCE(vyveden_iz_ekspluatacii.vyveden_iz_ekspluatacii,0) + COALESCE(mr_user.mr_user,0) + COALESCE(kvt.kvt,0) + COALESCE(mr_admin.mr_admin,0) + COALESCE(kvt_offline.kvt_offline,0) AS itogo_sim_for_stocks,
    	COALESCE(uteryany.uteryany,0) + COALESCE(v_ozhidanii_activacii.v_ozhidanii_activacii,0) + COALESCE(slugebnyi_transport.slugebnyi_transport,0) + COALESCE(remont.remont,0) + COALESCE(vyveden_iz_ekspluatacii.vyveden_iz_ekspluatacii,0) + COALESCE(mr_user.mr_user,0) + COALESCE(kvt.kvt,0) + COALESCE(mr_admin.mr_admin,0) + COALESCE(kvt_offline.kvt_offline,0) AS itogo_sim_for_city,
    	COALESCE(kvt_offline.kvt_offline,0) AS kvt_offline
    FROM t_city_sklady
    LEFT JOIN uteryany ON t_city_sklady.id = uteryany.city_id AND t_city_sklady.model = uteryany.model
    LEFT JOIN v_ozhidanii_activacii ON t_city_sklady.id = v_ozhidanii_activacii.city_id AND t_city_sklady.model = v_ozhidanii_activacii.model
    LEFT JOIN slugebnyi_transport ON t_city_sklady.id = slugebnyi_transport.city_id AND t_city_sklady.model = slugebnyi_transport.model
    LEFT JOIN remont ON t_city_sklady.id = remont.city_id AND t_city_sklady.model = remont.model
    LEFT JOIN vyveden_iz_ekspluatacii ON t_city_sklady.id = vyveden_iz_ekspluatacii.city_id AND t_city_sklady.model = vyveden_iz_ekspluatacii.model
    LEFT JOIN mr_user ON t_city_sklady.id = mr_user.city_id AND t_city_sklady.model = mr_user.model
    LEFT JOIN kvt ON t_city_sklady.id = kvt.city_id AND t_city_sklady.model = kvt.model
    LEFT JOIN kvt_offline ON t_city_sklady.id = kvt_offline.city_id AND t_city_sklady.model = kvt_offline.model
    LEFT JOIN mr_admin ON t_city_sklady.id = mr_admin.city_id AND t_city_sklady.model = mr_admin.model
    ORDER BY t_city_sklady.id DESC
    '''

    df1_all = pd.read_sql(select_df1_all, engine_mysql)
    df1_all = df1_all.loc[df1_all['city_id'].isin([13, 15, 18, 16, 17, 19, 20, 21, 22, 25, 12, 11, 28, 29, 14, 32])]
    df1_rabota = df1_all.loc[df1_all['city_id'].isin([13, 15, 18, 16, 17, 19, 20, 21, 22, 25, 28, 29, 14, 32])]
    df1_rabota_itog = df1_rabota.groupby(['city_id', 'name'], as_index=False) \
        .agg({'uteryany': 'sum',
              'v_ozhidanii_activacii': 'sum',
              'slugebnyi_transport': 'sum',
              'remont': 'sum',
              'vyveden_iz_ekspluatacii': 'sum',
              'mr_user': 'sum',
              'kvt': 'sum',
              'mr_admin': 'sum',
              'fact_park': 'sum',
              'itogo_sim_for_stocks': 'sum',
              'itogo_sim_for_city': 'sum',
              'kvt_offline': 'sum'})
    df1_sklady = df1_all.loc[df1_all['city_id'].isin([11, 12])].copy()
    df1_sklady_temp = df1_sklady.fillna('empty').replace('', 'empty')
    df1_sklady_temp_1 = df1_sklady_temp[df1_sklady_temp['model'].str.contains('freego')].copy()
    df1_sklady_temp_1['name'] = df1_sklady_temp_1['name'] + '_' + df1_sklady_temp_1['model']
    df1_sklady_temp_1 = df1_sklady_temp_1.drop('model', axis=1)
    df1 = pd.concat([df1_rabota_itog, df1_sklady_temp_1], axis=0)

    select_df2 = '''
        SELECT
            vni_cities_for_graph.id as city_id,
            vni_cities_for_graph."name",
            SUM(vni_cities_for_graph.poezdok) as poezdok_7day,
            SUM(vni_cities_for_graph.kvt) as kvt_7day,
            SUM(vni_cities_for_graph.poezdok) / NULLIF(SUM(vni_cities_for_graph.kvt), 0)::decimal(16,2) AS akb_na_park,
            CASE WHEN SUM(vni_cities_for_graph.poezdok) / NULLIF(SUM(vni_cities_for_graph.kvt), 0)::decimal(16,2) < 1 THEN 0.14
                WHEN SUM(vni_cities_for_graph.poezdok) / NULLIF(SUM(vni_cities_for_graph.kvt), 0)::decimal(16,2) >= 1 AND SUM(vni_cities_for_graph.poezdok) / NULLIF(SUM(vni_cities_for_graph.kvt), 0)::decimal(16,2) < 2 THEN 0.22
                WHEN SUM(vni_cities_for_graph.poezdok) / NULLIF(SUM(vni_cities_for_graph.kvt), 0)::decimal(16,2) >= 2 THEN 0.3 END AS akb_na_park_percent
        FROM vni_cities_for_graph
        WHERE vni_cities_for_graph."date" <= DATE(TO_CHAR(current_date - interval '1' DAY, 'YYYY-mm-dd')) 
            AND vni_cities_for_graph."date" > DATE(TO_CHAR(current_date - interval '8' DAY, 'YYYY-mm-dd'))
        GROUP BY vni_cities_for_graph.id, vni_cities_for_graph."name"
    '''

    df2 = pd.read_sql(select_df2, engine_postgresql).fillna(0)

    google_service_account_json = get_google_creds()
    with open('google_json.json', 'w') as fp:
        json.dump(json.loads(google_service_account_json, strict=False), fp)
    generated_json_file = './google_json.json'

    SERVICE_ACCOUNT_FILE = './google_json.json'
    SPREADSHEET_ID = '1BMH_HSxmK33SZvv3cIAH_SIgvm2NncSTTKI1aa7CoG8'
    RANGE_NAME = 'Плановое!A1:E16'
    service_account_file = generated_json_file
    sheets_service = get_sheets_service(SERVICE_ACCOUNT_FILE)
    df3 = read_sheet_data_to_pandas(sheets_service, SPREADSHEET_ID, RANGE_NAME)
    df3 = df3.replace('', '0')
    df3['city_id'] = df3['city_id'].astype(int)
    df3['planovoye'] = df3['planovoye'].fillna('0').replace('', '0').astype(int)
    df3['Batteries V4.6'] = df3['Batteries V4.6'].fillna('0').replace('', '0').astype(int)
    df3['Batteries numbers V3 PRO'] = df3['Batteries numbers V3 PRO'].fillna('0').replace('', '0').astype(int)

    # Соединяю данные для окончательного расчета
    df = df1.merge(df2[['city_id', 'poezdok_7day', 'kvt_7day', 'akb_na_park', 'akb_na_park_percent']], on='city_id',
                   how='left') \
        .merge(df3[['city_id', 'planovoye', 'Batteries V4.6', 'Batteries numbers V3 PRO']], on='city_id', how='left') \
        .fillna(0)

    df.fillna(0, inplace=True)
    df['svobodnyh_akb'] = df['Batteries V4.6'] + df['Batteries numbers V3 PRO']
    df['skolko_nugno_akb'] = df['planovoye'] * df['akb_na_park_percent']
    df['skolko_dovesti_sim'] = df['planovoye'] - df['fact_park']
    df['skolko_dovesti_akb'] = df['skolko_nugno_akb'] - df['svobodnyh_akb']
    df['timestamp'] = pd.Timestamp.now() + pd.Timedelta(hours=3)

    df_temp = df.copy()
    df_temp['planovoye'] = df_temp['planovoye'].astype(int)
    df_temp['planovoye'] = df_temp['planovoye'].astype(int)

    df = df_temp.copy()

    select_spisannye = '''
        SELECT
            spisannye.name,
            SUM(spisannye.spisannye) AS spisannye
        FROM 
            (SELECT
                CASE spisannye.user_group_id WHEN 5 THEN 'Kastoria_freego v3pro' WHEN 6 THEN 'Kastoria_freego v.4.6.' END AS name,
                spisannye.spisannye
            FROM 
                (SELECT
                    t_bike.city_id,
                    t_bike.model,
                    t_bike.user_group_id,
                    COUNT(t_bike.id) AS spisannye
                FROM t_bike
                WHERE t_bike.user_group_id IS NOT NULL
                GROUP BY t_bike.city_id, t_bike.model, t_bike.user_group_id
                ) AS spisannye) AS spisannye
        GROUP BY spisannye.name
        UNION 
        SELECT
            CASE total.name WHEN '' THEN 'Total' END AS name,
            total.spisannye
        FROM 
            (SELECT
                t_bike.model AS name,
                COUNT(t_bike.id) AS spisannye
            FROM t_bike
            WHERE t_bike.user_group_id IS NOT NULL 
                GROUP BY t_bike.model) AS total
    '''

    df_spisannye = pd.read_sql(select_spisannye, engine_mysql)

    df = df.merge(df_spisannye[['name', 'spisannye']], how='left', on='name')
    df.fillna(0, inplace=True)

    df['itogo_sim_for_stocks'] = df['spisannye'] + df['v_ozhidanii_activacii'] + df['slugebnyi_transport'] + df[
        'remont'] + df['vyveden_iz_ekspluatacii'] + df['mr_user'] + df['kvt'] + df['mr_admin'] + df['kvt_offline']
    df['itogo_sim_for_city'] = df['uteryany'] + df['v_ozhidanii_activacii'] + df['slugebnyi_transport'] + df['remont'] + df[
        'vyveden_iz_ekspluatacii'] + df['mr_user'] + df['kvt'] + df['mr_admin'] + df['kvt_offline']
    df = df.iloc[:, [25, 0, 1, 2, 3, 4, 5, 6, 7, 8, 13, 9, 10, 26, 18, 21, 22, 17, 23, 24, 12, 11]]

    df.loc[(df['city_id'] == 12) & (df['name'] == 'Argos Orestiko_freego v3pro'), 'svobodnyh_akb'] = int(
        df3.loc[df3['city_name'] == 'Аргос (склад Волоса)', 'Batteries numbers V3 PRO'].iloc[0])
    df.loc[(df['city_id'] == 12) & (df['name'] == 'Argos Orestiko_freego v.4.6.'), 'svobodnyh_akb'] = int(
        df3.loc[df3['city_name'] == 'Аргос (склад Волоса)', 'Batteries V4.6'].iloc[0])
    df.loc[(df['city_id'] == 11) & (df['name'] == 'Kastoria_freego v3pro'), 'svobodnyh_akb'] = int(
        df3.loc[df3['city_name'] == 'Кастория (склад)', 'Batteries numbers V3 PRO'].iloc[0])
    df.loc[(df['city_id'] == 11) & (df['name'] == 'Kastoria_freego v.4.6.'), 'svobodnyh_akb'] = int(
        df3.loc[df3['city_name'] == 'Кастория (склад)', 'Batteries V4.6'].iloc[0])

    df = df.replace('Argos Orestiko_freego v3pro', 'Малый склад_v3pro') \
        .replace('Argos Orestiko_freego v.4.6.', 'Малый склад_v.4.6') \
        .replace('Kastoria_freego v3pro', 'Главный склад_v3pro') \
        .replace('Kastoria_freego v.4.6.', 'Главный склад_v.4.6')

    df_cities = df.loc[df['city_id'].isin([13, 15, 18, 16, 17, 19, 20, 21, 22, 25])]
    total_row_work = {
        'timestamp': [pd.Timestamp.now() + pd.Timedelta(hours=3)],
        'city_id': [100],
        'name': ['Total_cities'],
        'uteryany': [df_cities['uteryany'].sum()],
        'v_ozhidanii_activacii': [df_cities['v_ozhidanii_activacii'].sum()],
        'slugebnyi_transport': [df_cities['slugebnyi_transport'].sum()],
        'remont': [df_cities['remont'].sum()],
        'vyveden_iz_ekspluatacii': [df_cities['vyveden_iz_ekspluatacii'].sum()],
        'mr_user': [df_cities['mr_user'].sum()],
        'kvt': [df_cities['kvt'].sum()],
        'kvt_offline': [df_cities['kvt_offline'].sum()],
        'mr_admin': [df_cities['mr_admin'].sum()],
        'fact_park': [df_cities['fact_park'].sum()],
        'planovoye': [df_cities['planovoye'].sum()],
        'svobodnyh_akb': [df_cities['svobodnyh_akb'].sum()],
        'skolko_nugno_akb': [df_cities['skolko_nugno_akb'].sum()],
        'akb_na_park_percent': [0],
        'skolko_dovesti_sim': [df_cities['skolko_dovesti_sim'].sum()],
        'skolko_dovesti_akb': [df_cities['skolko_dovesti_akb'].sum()],
        'itogo_sim_for_city': [df_cities['itogo_sim_for_city'].sum()],
        'spisannye': [df_cities['spisannye'].sum()],
        'itogo_sim_for_stocks': [df_cities['itogo_sim_for_stocks'].sum()]
    }

    df = pd.concat([df, pd.DataFrame.from_dict(total_row_work)])

    df_sklady = df.loc[df['city_id'].isin([11, 12])]

    total_row_sklady = {
        'timestamp': [pd.Timestamp.now() + pd.Timedelta(hours=3)],
        'city_id': [1000],
        'name': ['Total_stocks'],
        'uteryany': [df_sklady['uteryany'].sum()],
        'v_ozhidanii_activacii': [df_sklady['v_ozhidanii_activacii'].sum()],
        'slugebnyi_transport': [df_sklady['slugebnyi_transport'].sum()],
        'remont': [df_sklady['remont'].sum()],
        'vyveden_iz_ekspluatacii': [df_sklady['vyveden_iz_ekspluatacii'].sum()],
        'mr_user': [df_sklady['mr_user'].sum()],
        'kvt': [df_sklady['kvt'].sum()],
        'kvt_offline': [df_sklady['kvt_offline'].sum()],
        'mr_admin': [df_sklady['mr_admin'].sum()],
        'fact_park': [df_sklady['fact_park'].sum()],
        'planovoye': [df_sklady['planovoye'].sum()],
        'svobodnyh_akb': [df_sklady['svobodnyh_akb'].sum()],
        'skolko_nugno_akb': [df_sklady['skolko_nugno_akb'].sum()],
        'akb_na_park_percent': [0],
        'skolko_dovesti_sim': [df_sklady['skolko_dovesti_sim'].sum()],
        'skolko_dovesti_akb': [df_sklady['skolko_dovesti_akb'].sum()],
        'itogo_sim_for_city': [df_sklady['itogo_sim_for_city'].sum()],
        'spisannye': [df_sklady['spisannye'].sum()],
        'itogo_sim_for_stocks': [df_sklady['itogo_sim_for_stocks'].sum()]
    }

    df = pd.concat([df, pd.DataFrame.from_dict(total_row_sklady)])
    df['timestamp'] = pd.Timestamp.now() + pd.Timedelta(hours=3)

    # Волос свободные АКБ
    df.loc[(df['name'] == 'Volos') & (df['city_id'] == 17), 'svobodnyh_akb'] = int(
        df.loc[(df['name'] == 'Volos') & (df['city_id'] == 17), 'svobodnyh_akb'].iloc[0]) + int(
        df.loc[df['name'] == 'Малый склад_v3pro', 'svobodnyh_akb'].iloc[0]) + int(
        df.loc[df['name'] == 'Малый склад_v.4.6', 'svobodnyh_akb'].iloc[0])

    df.to_sql("akb_cities_and_stocks", engine_postgresql, if_exists="append", index=False)
    print('akb_cities_and_stocks UPDATED!')

    # # # АКБ - конец

    # АКБ с красными столбцами Начало
    select_akb_cities_and_stocks_result = '''
        SELECT *
        FROM 
            (SELECT
                raw.update_timestamp ,
                -- raw."name" ,
                -- raw."timestamp" ,
                raw.slomali ,
                raw.pochinili ,
                -- raw.pribylo_vybylo_goroda + raw.slomali  - LAG(raw.slomali) OVER (PARTITION BY raw.city_id, raw."name" ORDER BY raw.day_) AS pribylo_vybylo_goroda,
                raw.pribylo_vybylo_goroda,
                -- COALESCE(raw.pribylo_vybylo_sklady,0) + COALESCE(raw.pochinili,0) - COALESCE(LAG(raw.pochinili) OVER (PARTITION BY raw.city_id, raw."name" ORDER BY raw.day_),0) AS pribylo_vybylo_sklady,
                raw.pribylo_vybylo_sklady,
                raw.remont_posledniy_pred ,
                raw.vyveden_iz_ekspluatacii_posledniy_pred ,
                raw.v_ozhidanii_activacii_posledniy_pred ,
                raw.slugebnyi_transport_posledniy_pred ,
                raw.itogo_sim_for_city_posledniy_pred ,
                raw.itogo_sim_for_stocks_posledniy_pred ,
                raw."rank" ,
                raw.day_ ,
                raw.remont_posledniy ,
                raw.vyveden_iz_ekspluatacii_posledniy ,
                raw.v_ozhidanii_activacii_posledniy ,
                raw.slugebnyi_transport_posledniy ,
                raw.itogo_sim_for_city_posledniy ,
                raw.itogo_sim_for_stocks_posledniy ,
                raw."timestamp" ,
                raw.city_id ,
                raw."name" ,
                raw.uteryany ,
                raw.v_ozhidanii_activacii ,
                raw.slugebnyi_transport ,
                raw.remont,
                raw.vyveden_iz_ekspluatacii ,
                raw.mr_user ,
                raw.kvt,
                raw.kvt_offline ,
                raw.mr_admin ,
                raw.fact_park ,
                raw.spisannye ,
                raw.planovoye ,
                raw.svobodnyh_akb ,
                raw.skolko_nugno_akb ,
                raw.akb_na_park_percent ,
                raw.skolko_dovesti_sim ,
                raw.skolko_dovesti_akb ,
                raw.itogo_sim_for_city ,
                raw.itogo_sim_for_stocks
            FROM 
                (SELECT
                    NOW() AS update_timestamp,
                    res.remont_posledniy - res.remont_posledniy_pred - (res.vyveden_iz_ekspluatacii_posledniy - res.vyveden_iz_ekspluatacii_posledniy_pred) AS slomali,
                    res.v_ozhidanii_activacii_posledniy - res.v_ozhidanii_activacii_posledniy_pred + res.slugebnyi_transport_posledniy - res.slugebnyi_transport_posledniy_pred AS pochinili,
                    res.itogo_sim_for_city_posledniy - res.itogo_sim_for_city_posledniy_pred AS pribylo_vybylo_goroda,
                    res.itogo_sim_for_stocks_posledniy - res.itogo_sim_for_stocks_posledniy_pred AS pribylo_vybylo_sklady,
                    res.*
                FROM 
                    (SELECT
                        LAG(ranked.remont_posledniy) OVER (PARTITION BY ranked.city_id, ranked."name" ORDER BY ranked.day_) AS remont_posledniy_pred,
                        LAG(ranked.vyveden_iz_ekspluatacii_posledniy) OVER (PARTITION BY ranked.city_id, ranked."name" ORDER BY ranked.day_) AS vyveden_iz_ekspluatacii_posledniy_pred,
                        LAG(ranked.v_ozhidanii_activacii_posledniy) OVER (PARTITION BY ranked.city_id, ranked."name" ORDER BY ranked.day_) AS v_ozhidanii_activacii_posledniy_pred,
                        LAG(ranked.slugebnyi_transport_posledniy) OVER (PARTITION BY ranked.city_id, ranked."name" ORDER BY ranked.day_) AS slugebnyi_transport_posledniy_pred,
                        LAG(ranked.itogo_sim_for_city_posledniy) OVER (PARTITION BY ranked.city_id, ranked."name" ORDER BY ranked.day_) AS itogo_sim_for_city_posledniy_pred,
                        LAG(ranked.itogo_sim_for_stocks_posledniy) OVER (PARTITION BY ranked.city_id, ranked."name" ORDER BY ranked.day_) AS itogo_sim_for_stocks_posledniy_pred,
                        ranked.*
                    FROM 
                        (
                        SELECT  
                            RANK() OVER (PARTITION BY akb_cities_and_stocks.city_id, akb_cities_and_stocks."name", DATE(akb_cities_and_stocks."timestamp") ORDER BY akb_cities_and_stocks."timestamp" DESC) AS rank,
                            DATE(akb_cities_and_stocks."timestamp") AS day_,
                            FIRST_VALUE(akb_cities_and_stocks.remont) OVER (PARTITION BY akb_cities_and_stocks."name", DATE(akb_cities_and_stocks."timestamp") ORDER BY akb_cities_and_stocks."timestamp" DESC) AS remont_posledniy,
                            FIRST_VALUE(akb_cities_and_stocks.vyveden_iz_ekspluatacii) OVER (PARTITION BY akb_cities_and_stocks."name", DATE(akb_cities_and_stocks."timestamp") ORDER BY akb_cities_and_stocks."timestamp" DESC) AS vyveden_iz_ekspluatacii_posledniy,
                            FIRST_VALUE(akb_cities_and_stocks.v_ozhidanii_activacii) OVER (PARTITION BY akb_cities_and_stocks."name", DATE(akb_cities_and_stocks."timestamp") ORDER BY akb_cities_and_stocks."timestamp" DESC) AS v_ozhidanii_activacii_posledniy,
                            FIRST_VALUE(akb_cities_and_stocks.slugebnyi_transport) OVER (PARTITION BY akb_cities_and_stocks."name", DATE(akb_cities_and_stocks."timestamp") ORDER BY akb_cities_and_stocks."timestamp" DESC) AS slugebnyi_transport_posledniy,
                            FIRST_VALUE(akb_cities_and_stocks.itogo_sim_for_city) OVER (PARTITION BY akb_cities_and_stocks."name", DATE(akb_cities_and_stocks."timestamp") ORDER BY akb_cities_and_stocks."timestamp" DESC) AS itogo_sim_for_city_posledniy,
                            FIRST_VALUE(akb_cities_and_stocks.itogo_sim_for_stocks) OVER (PARTITION BY akb_cities_and_stocks."name", DATE(akb_cities_and_stocks."timestamp") ORDER BY akb_cities_and_stocks."timestamp" DESC) AS itogo_sim_for_stocks_posledniy,
                            akb_cities_and_stocks.*
                        FROM akb_cities_and_stocks
                        ORDER BY akb_cities_and_stocks."timestamp" DESC
                        ) AS ranked
                    WHERE ranked."rank" = 1 
                    ORDER BY ranked.day_ DESC
                            ) AS res 
                        -- WHERE res.day_ = DATE(NOW())
                        ) AS raw
            -- WHERE raw.day_ = DATE(NOW())
            ORDER BY raw."timestamp" DESC) AS res
        WHERE res.day_ = DATE(NOW())
    '''
    df_akb_cities_and_stocks_result = pd.read_sql(select_akb_cities_and_stocks_result, engine_postgresql)
    df_akb_cities_and_stocks_result.to_sql("akb_cities_and_stocks_result", engine_postgresql, if_exists="append",
                                           index=False)
    print('АКБ с красными столбцами UPDATED!')

    # АКБ с красными столбцами Конец

    # Выгрузка t_bike_history Начало
    select_df_t_bike = '''
       SELECT
            NOW() as 'timestamp',
            IFNULL(t_bike.id,0) AS id,
            IFNULL(t_bike.number,0) AS number,
            IFNULL(t_bike.imei,0) AS imei,
            IFNULL(t_bike.type_id,0) AS type_id,
            IFNULL(t_bike.g_time,0) AS g_time,
            IFNULL(t_bike.g_lat,0) AS g_lat,
            IFNULL(t_bike.g_lng,0) AS g_lng,
            IFNULL(t_bike.status,0) AS status,
            IFNULL(t_bike.use_status,0) AS use_status,
            IFNULL(t_bike.power,0) AS power,
            IFNULL(t_bike.gsm,0) AS gsm,
            IFNULL(t_bike.gps_number,'empty') AS gps_number,
            IFNULL(t_bike.city_id,0) AS city_id,
            IFNULL(t_bike.heart_time,0) AS heart_time,
            IFNULL(t_bike.version,0) AS version,
            IFNULL(t_bike.version_time,0) AS version_time,
            IFNULL(t_bike.readpack,0) AS readpack,
            IFNULL(t_bike.add_date, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS add_date,
            IFNULL(t_bike.error_status,0) AS error_status,
            IFNULL(t_bike.server_ip,'0.0.0.0') AS server_ip,
            IFNULL(t_bike.bike_status,0) AS bike_status,
            IFNULL(t_bike.sponsors_id,0) AS sponsors_id,
            IFNULL(t_bike.bike_no,0) AS bike_no,
            IFNULL(t_bike.bike_type,0) AS bike_type,
            IFNULL(t_bike.extend_info,0) AS extend_info,
            IFNULL(t_bike.area_id,0) AS area_id,
            IFNULL(t_bike.bike_power,0) AS bike_power,
            IFNULL(t_bike.bike_power_status,0) AS bike_power_status,
            IFNULL(t_bike.mac,0) AS mac,
            IFNULL(t_bike.iccid,'empty') AS iccid,
            IFNULL(t_bike.maintain_status,0) AS maintain_status,
            IFNULL(t_bike.extra_lock_status,0) AS extra_lock_status,
            IFNULL(t_bike.available,0) AS available,
            IFNULL(t_bike.model,'empty') AS model,
            IFNULL(t_bike.protocol,0) AS protocol,
            IFNULL(t_bike.frame_number,'empty') AS frame_number,
            IFNULL(t_bike.battery_key,0) AS battery_key,
            IFNULL(t_bike.release_time, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS release_time,
            IFNULL(t_bike.last_service_time, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS last_service_time,
            IFNULL(t_bike.industry_id,0) AS industry_id,
            IFNULL(t_bike.ble_key,'empty') AS ble_key,
            IFNULL(t_bike.user_group_id,0) AS user_group_id 
       FROM shamri.t_bike
    '''
    df_t_bike = pd.read_sql(select_df_t_bike, engine_mysql)
    df_t_bike.to_sql("t_bike_history", engine_postgresql, if_exists="append", index=False)
    print('t_bike_history UPDATED!')

    # Выгрузка t_bike_history Конец

    # Выгрузка по городам для графиков
    select_vni_cities_for_graph = '''
    SELECT *
    FROM (
        SELECT
            date(vni_cities.timestamp),
            vni_cities.*,
            rank() OVER (PARTITION BY vni_cities.id, date(vni_cities.timestamp) ORDER BY vni_cities.timestamp DESC) AS rank
        FROM vni_cities
        ORDER BY vni_cities."timestamp" DESC
        ) AS ranked
    WHERE ranked."rank" = 1 
    '''

    df_vni_cities_for_graph = pd.read_sql(select_vni_cities_for_graph, engine_postgresql)

    # Очистка таблиц
    truncate_vni_cities_for_graph = "TRUNCATE TABLE vni_cities_for_graph RESTART IDENTITY;"
    truncate_t_bike = "TRUNCATE TABLE t_bike RESTART IDENTITY;"
    truncate_t_city = "TRUNCATE TABLE t_city RESTART IDENTITY;"
    truncate_t_subscription = "TRUNCATE TABLE t_subscription RESTART IDENTITY;"

    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            print(f"Попытка очистить таблицы")
            connection.execute(sa.text(truncate_vni_cities_for_graph))
            # Очистка t_bike
            connection.execute(sa.text(truncate_t_bike))
            # Очистка t_city
            connection.execute(sa.text(truncate_t_city))
            # Очистка t_subscription
            connection.execute(sa.text(truncate_t_subscription))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица vni_cities_for_graph успешно очищена.")
            print(f"Таблица t_bike успешно очищена.")
            print(f"Таблица t_city успешно очищена.")
            print(f"Таблица t_subscription успешно очищена.")

    # Загрузка таблицы для графиков
    df_vni_cities_for_graph.to_sql("vni_cities_for_graph", engine_postgresql, if_exists="append", index=False)
    print('Table for graphs vni_cities_for_graph updated!')

    # Копирую t_bike, t_city, t_subscription
    select_t_bike = '''	SELECT
        NOW() as 'timestamp',
        IFNULL(t_bike.id,0) AS id,
        IFNULL(t_bike.number,0) AS number,
        IFNULL(t_bike.imei,0) AS imei,
        IFNULL(t_bike.type_id,0) AS type_id,
        IFNULL(t_bike.g_time,0) AS g_time,
        IFNULL(t_bike.g_lat,0) AS g_lat,
        IFNULL(t_bike.g_lng,0) AS g_lng,
        IFNULL(t_bike.status,0) AS status,
        IFNULL(t_bike.use_status,0) AS use_status,
        IFNULL(t_bike.power,0) AS power,
        IFNULL(t_bike.gsm,0) AS gsm,
        IFNULL(t_bike.gps_number,'empty') AS gps_number,
        IFNULL(t_bike.city_id,0) AS city_id,
        IFNULL(t_bike.heart_time,0) AS heart_time,
        IFNULL(t_bike.version,0) AS version,
        IFNULL(t_bike.version_time,0) AS version_time,
        IFNULL(t_bike.readpack,0) AS readpack,
        IFNULL(t_bike.add_date, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS add_date,
        IFNULL(t_bike.error_status,0) AS error_status,
        IFNULL(t_bike.server_ip,'0.0.0.0') AS server_ip,
        IFNULL(t_bike.bike_status,0) AS bike_status,
        IFNULL(t_bike.sponsors_id,0) AS sponsors_id,
        IFNULL(t_bike.bike_no,0) AS bike_no,
        IFNULL(t_bike.bike_type,0) AS bike_type,
        IFNULL(t_bike.extend_info,0) AS extend_info,
        IFNULL(t_bike.area_id,0) AS area_id,
        IFNULL(t_bike.bike_power,0) AS bike_power,
        IFNULL(t_bike.bike_power_status,0) AS bike_power_status,
        IFNULL(t_bike.mac,0) AS mac,
        IFNULL(t_bike.iccid,'empty') AS iccid,
        IFNULL(t_bike.maintain_status,0) AS maintain_status,
        IFNULL(t_bike.extra_lock_status,0) AS extra_lock_status,
        IFNULL(t_bike.available,0) AS available,
        IFNULL(t_bike.model,'empty') AS model,
        IFNULL(t_bike.protocol,0) AS protocol,
        IFNULL(t_bike.frame_number,'empty') AS frame_number,
        IFNULL(t_bike.battery_key,0) AS battery_key,
        IFNULL(t_bike.release_time, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS release_time,
        IFNULL(t_bike.last_service_time, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS last_service_time,
        IFNULL(t_bike.industry_id,0) AS industry_id,
        IFNULL(t_bike.ble_key,'empty') AS ble_key,
        IFNULL(t_bike.user_group_id,0) AS user_group_id FROM shamri.t_bike
	'''
    df_t_bike = pd.read_sql(select_t_bike, engine_mysql)

    select_t_city = '''	SELECT
    	NOW() as 'timestamp',
    	IFNULL(t_city.id,0) AS id,
    	IFNULL(t_city.name,'empty') AS name,
    	IFNULL(t_city.note,'City zone') AS note,
    	IFNULL(t_city.code,0) AS code,
    	IFNULL(t_city.area_detail,'Empty') AS area_detail,
    	IFNULL(t_city.area_lat,0) AS area_lat,
    	IFNULL(t_city.area_lng,0) AS area_lng,
    	IFNULL(t_city.currency,'Empty') AS currency,
    	IFNULL(t_city.start_time,'00:00') AS start_time,
    	IFNULL(t_city.end_time,'00:24') AS end_time,
    	IFNULL(t_city.invite_code,'Empty') AS invite_code,
    	IFNULL(t_city.extend_info,'Empty') AS extend_info,
    	IFNULL(t_city.industry_id,0) AS industry_id FROM shamri.t_city
    '''
    df_t_city = pd.read_sql(select_t_city, engine_mysql)

    select_t_subscription = '''
        SELECT 
            NOW() AS add_time ,
            IFNULL(ts.id, 0) AS id,
            IFNULL(ts.title, '0') AS title,
            IFNULL(ts.description, '0') AS description,
            IFNULL(ts.bike_type, 0) AS bike_type,
            IFNULL(ts.city_id, 0) AS city_id,
            IFNULL(ts.`type`, 0) AS type,
            IFNULL(ts.ride_duration, 0) AS ride_duration,
            IFNULL(ts.ride_count_per_interval, 0) AS ride_count_per_interval,
            IFNULL(ts.interval_unit, '0') AS interval_unit,
            IFNULL(ts.duration_between_rides, 0) AS duration_between_rides,
            IFNULL(ts.price, 0) AS price,
            IFNULL(ts.active, 0) AS active,
            IFNULL(ts.duration, 0) AS duration,
            IFNULL(ts.one_time, 0) AS one_time,
            IFNULL(ts.industry_id, 0) AS industry_id,
            IFNULL(ts.end_time, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS end_time 
        FROM shamri.t_subscription ts 
    '''
    df_t_subscription = pd.read_sql(select_t_subscription, engine_mysql)

    # Загрузка t_bike
    df_t_bike.to_sql("t_bike", engine_postgresql, if_exists="append", index=False)
    print('Table for graphs t_bike!')

    # Загрузка t_city
    df_t_city.to_sql("t_city", engine_postgresql, if_exists="append", index=False)
    print('Table for graphs t_city!')

    # Загрузка t_subscription
    df_t_subscription.to_sql("t_subscription", engine_postgresql, if_exists="append", index=False)
    print('Таблица t_subscription успешно обновлена!')

    # Цели по чекапам. Начало

    SERVICE_ACCOUNT_FILE = './google_json.json'
    SPREADSHEET_ID = '1dSOV9X2FV3mnOmnwWvTMJuCCZ-tVBf64DP90k3EYD90'
    RANGE_NAME = 'Цели по чекапам!A:E'
    service_account_file = generated_json_file

    df = read_sheet_data_to_pandas(sheets_service, SPREADSHEET_ID, RANGE_NAME)

    df = df[df['Worker Id'].notna()]
    df.fillna(0, inplace=True)
    df['Worker Id'] = df['Worker Id'].astype(int)
    df['Цель на смену'] = df['Цель на смену'].astype(int)
    df['add_time'] = pd.Timestamp.now() + pd.Timedelta(hours=3)

    # Очистка таблицы
    truncate_t_bike = "TRUNCATE TABLE checkup_goals_from_google RESTART IDENTITY;"
    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            print(f"Попытка очистить таблицу")
            # Очистка grafik_rabot_google
            connection.execute(sa.text(truncate_t_bike))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица checkup_goals_from_google успешно очищена!")

    df.to_sql("checkup_goals_from_google", engine_postgresql, if_exists="append", index=False)
    print('Таблица checkup_goals_from_google успешно обновлена!')

    # Цели по чекапам. Конец

    # Расчет зп. Начало 30.09.2025
    SERVICE_ACCOUNT_FILE = './google_json.json'
    SPREADSHEET_ID = '1dSOV9X2FV3mnOmnwWvTMJuCCZ-tVBf64DP90k3EYD90'
    RANGE_NAME = 'График работ!A:L'
    service_account_file = generated_json_file

    sheets_service = get_sheets_service(SERVICE_ACCOUNT_FILE)

    # Выгрузка для Каждый работник
    select_for_workers = '''
        SELECT 
            date_trunc('month', grg."Date")::date AS "Месяц",
            grg.* ,
            EXTRACT(EPOCH FROM (grg."Actual finish time" - grg."Actual start time"))::int AS "Actual working seconds"
        FROM  
        (
            SELECT
                grg."Date"::date ,
                grg."Worker id"::int ,
                grg."Worker username" ,
                grg."Worker nickname" ,
                to_timestamp(concat(grg."Date", ' ', grg."Actual start time"), 'YYYY-MM-DD HH24:MI:SS') AS "Actual start time" ,
                CASE 
                    WHEN grg."Actual start time" < grg."Actual finish time" THEN to_timestamp(concat(grg."Date", ' ', grg."Actual finish time"), 'YYYY-MM-DD HH24:MI:SS')
                    ELSE to_timestamp(concat(grg."Date", ' ', grg."Actual finish time"), 'YYYY-MM-DD HH24:MI:SS') + interval '1 day'
                END  AS "Actual finish time" ,
                grg."Finish odometer kilometers"::float - grg."Start odometer kilometers"::float AS distance
            FROM damir.grafik_rabot_google grg 
            WHERE grg."Actual start time" != '00:00:70'
                AND grg."Actual finish time" != '00:00:70'
                AND grg."Date" >= '2025-08-01'
            ) AS grg
    '''
    df_for_workers = pd.read_sql(select_for_workers, engine_postgresql)
    df_for_workers['Месяц'] = pd.to_datetime(df_for_workers['Месяц'], errors='coerce')

    # Скачиваю Таблица(ставки)
    RANGE_NAME_2 = 'Таблица(ставки)!A:BF'
    sheets_service = get_sheets_service(SERVICE_ACCOUNT_FILE)
    df_tabl_stavki = read_sheet_data_to_pandas(sheets_service, SPREADSHEET_ID, RANGE_NAME_2)

    new_columns = df_tabl_stavki.iloc[0]
    df_tabl_stavki.columns = new_columns
    df_tabl_stavki.drop(index=df_tabl_stavki.index[0], axis=0, inplace=True)
    df_tabl_stavki = df_tabl_stavki[df_tabl_stavki['Месяц'].notna()]
    df_tabl_stavki['Месяц'] = pd.to_datetime(df_tabl_stavki['Месяц'], errors='coerce')
    df_tabl_stavki['Дата нового условия ставки (час)'] = pd.to_datetime(
        df_tabl_stavki['Дата нового условия ставки (час)'], errors='coerce').replace('1970-01-01', 'NaT')
    df_tabl_stavki['Дата нового условия ставки (нед)'] = pd.to_datetime(
        df_tabl_stavki['Дата нового условия ставки (нед)'], errors='coerce').replace('1970-01-01', 'NaT')
    df_tabl_stavki['Дата нового условия ставки (мес)'] = pd.to_datetime(
        df_tabl_stavki['Дата нового условия ставки (мес)'], errors='coerce').replace('1970-01-01', 'NaT')
    df_tabl_stavki['Нач_дата_расч_бонуса_нов_сотр'] = pd.to_datetime(
        df_tabl_stavki['Нач_дата_расч_бонуса_нов_сотр'], errors='coerce').replace('1970-01-01', 'NaT')
    df_tabl_stavki['Оконч_дата_расч_бонус_нов_сотр'] = pd.to_datetime(
        df_tabl_stavki['Оконч_дата_расч_бонус_нов_сотр'], errors='coerce')
    df_tabl_stavki = df_tabl_stavki.replace('', '0')
    df_tabl_stavki['Worker id'] = df_tabl_stavki['Worker id'].astype(int)
    df_tabl_stavki['Количество отработанных часов'] = df_tabl_stavki['Количество отработанных часов'].fillna(0).astype(float)
    df_tabl_stavki['Ставка за час'] = df_tabl_stavki['Ставка за час'].fillna(0).astype(float)
    df_tabl_stavki['Ставка измененная за час'] = df_tabl_stavki['Ставка измененная за час'].fillna(0).astype(float)
    df_tabl_stavki['Ставка за неделю'] = df_tabl_stavki['Ставка за неделю'].fillna(0).astype(float)
    df_tabl_stavki['Ставка измененная за неделю'] = df_tabl_stavki['Ставка измененная за неделю'].fillna(0).astype(float)
    df_tabl_stavki['Ставка за месяц'] = df_tabl_stavki['Ставка за месяц'].fillna(0).astype(float)
    df_tabl_stavki['Норма рабочих часов за месяц'] = df_tabl_stavki['Норма рабочих часов за месяц'].fillna(0).astype(float)
    df_tabl_stavki['Ставка свыше нормы'] = df_tabl_stavki['Ставка свыше нормы'].fillna(0).astype(float)
    df_tabl_stavki['Количество отработанных часов свыше нормы'] = df_tabl_stavki[
        'Количество отработанных часов свыше нормы'].fillna(0).astype(float)
    df_tabl_stavki['Ставка измененная за месяц'] = df_tabl_stavki['Ставка измененная за месяц'].fillna(0).astype(float)
    df_tabl_stavki['Норма расхода топлива на 100 км, литры'] = df_tabl_stavki[
        'Норма расхода топлива на 100 км, литры'].fillna(0).astype(float)
    df_tabl_stavki['Цена топлива за 1 литр, евро'] = df_tabl_stavki['Цена топлива за 1 литр, евро'].fillna(0).astype(float)
    df_tabl_stavki['Количество часов работы на складе'] = df_tabl_stavki['Количество часов работы на складе'].fillna(0).astype(
        float)
    df_tabl_stavki['Бонус за работу на складе'] = df_tabl_stavki['Бонус за работу на складе'].fillna(0).astype(float)
    df_tabl_stavki['Бонус за ремонт'] = df_tabl_stavki['Бонус за ремонт'].fillna(0).astype(float)
    df_tabl_stavki['Общее количество заряженных батарей'] = df_tabl_stavki[
        'Общее количество заряженных батарей'].replace('-', '0').fillna(0).astype(float)
    df_tabl_stavki['Сумма1'] = df_tabl_stavki['Сумма1'].fillna(0).astype(float)
    df_tabl_stavki['Сумма2'] = df_tabl_stavki['Сумма2'].fillna(0).astype(float)
    df_tabl_stavki['Сумма3'] = df_tabl_stavki['Сумма3'].fillna(0).astype(float)
    df_tabl_stavki['Сумма4'] = df_tabl_stavki['Сумма4'].fillna(0).astype(float)
    df_tabl_stavki['Сумма5'] = df_tabl_stavki['Сумма5'].fillna(0).astype(float)
    df_tabl_stavki['Сумма6'] = df_tabl_stavki['Сумма6'].fillna(0).astype(float)
    df_tabl_stavki['Сумма7'] = df_tabl_stavki['Сумма7'].fillna(0).astype(float)
    df_tabl_stavki['Штраф'] = df_tabl_stavki['Штраф'].fillna(0).astype(float)
    df_tabl_stavki['Аванс'] = df_tabl_stavki['Аванс'].fillna(0).astype(float)
    df_tabl_stavki['Сумма8'] = df_tabl_stavki['Сумма8'].fillna(0).astype(float)
    df_tabl_stavki['Сумма9'] = df_tabl_stavki['Сумма9'].fillna(0).astype(float)
    df_tabl_stavki['Сумма10'] = df_tabl_stavki['Сумма10'].fillna(0).astype(float)
    df_tabl_stavki['Процент_от_зп_бонус_прогр'] = df_tabl_stavki[
        'Процент_от_зп_бонус_прогр'].fillna(0).astype(float)
    df_tabl_stavki['id_invited_worker_nickname_бонусная_программа'] = df_tabl_stavki[
        'id_invited_worker_nickname_бонусная_программа'].fillna(0).astype(int)
    df_for_workers_distance = df_for_workers.groupby(['Месяц', 'Worker id', 'Worker username', 'Worker nickname'],
                                                     as_index=False) \
        .agg({'distance': 'sum'})

    df_temp = df_tabl_stavki.merge(df_for_workers_distance,
                                   how='left', on=['Месяц', 'Worker id', 'Worker username', 'Worker nickname'])
    df_temp['Компенсация топлива'] = df_temp['Норма расхода топлива на 100 км, литры'] * df_temp['distance'] * df_temp[
        'Цена топлива за 1 литр, евро'] / 100
    df_temp['Зарядка АКБ'] = df_temp['Общее количество заряженных батарей'] * 0.2
    df_temp['Работа на складе'] = df_temp['Количество часов работы на складе'] * df_temp['Ставка за час']
    df_temp['Возмещаемые расходы'] = df_temp['Сумма1'] + df_temp['Сумма2'] + df_temp['Сумма3']
    df_temp['Офиц_зп'] = df_temp['Сумма4'] + df_temp['Сумма5'] + df_temp['Сумма6'] + df_temp['Сумма7']
    df_temp['Кол-во дней для расчета бонуса'] = (df_temp['Оконч_дата_расч_бонус_нов_сотр'] - df_temp[
        'Нач_дата_расч_бонуса_нов_сотр']) / pd.Timedelta(days=1)
    # df_temp['Бонус приведи друга'] = df_temp['Кол-во дней для расчета бонуса'] * 5 * df_temp[
    #     'Процент от зарплаты, % (бонусная программа)'] / 100
    df_temp['Бонус приведи друга'] = df_temp['Кол-во дней для расчета бонуса'].fillna(0) * 5 * df_temp[
        'Процент_от_зп_бонус_прогр'].fillna(0).astype(float) / 100
    df_temp['Всего удержано'] = df_temp['Сумма8'].fillna(0) + df_temp['Сумма9'].fillna(0) + df_temp['Сумма10'].fillna(0)
    df_temp['Конец месяца'] = df_temp['Месяц'] + pd.tseries.offsets.MonthEnd()

    # Проверка вида ставки
    def type_stavka(df):
        if df['Ставка за час'] != 0:
            return 'Ставка за час'
        elif df['Ставка за неделю'] != 0:
            return 'Ставка за неделю'
        elif df['Ставка за месяц'] != 0:
            return 'Ставка за месяц'
        else:
            return None

    df_temp['Тип ставки'] = df_temp.apply(type_stavka, axis=1)

    # Обработка столбцов Ставка за час
    # По дням расписываю работников
    df_temp['Ставка за час постоянно'] = df_temp['Дата нового условия ставки (час)'].fillna(0).apply(
        lambda x: 0 if x == 0 else 1)

    columns = ['Date', 'Worker id', 'Ставка за час']
    res = pd.DataFrame(columns=columns)

    columns_month = ['Месяц', 'Worker id']
    res_month = pd.DataFrame(columns=columns_month)

    for index, row in df_temp.iterrows():

        if (row['Ставка за час постоянно'] == 0) & (row['Тип ставки'] == 'Ставка за час'):

            #         print('Постоянно')

            dates1 = pd.date_range(start=row['Месяц'], end=row['Конец месяца'], freq='D')
            df1 = pd.DataFrame(dates1, columns=['Date'])
            df1['Ставка за час'] = row['Ставка за час']
            df1['Worker id'] = row['Worker id']
            df1['Worker username'] = row['Worker username']
            df1['Worker nickname'] = row['Worker nickname']
            df1['Worker role'] = row['Worker role']
            df1['city'] = row['city']
            res = pd.concat([res, df1], ignore_index=True)

        elif (row['Ставка за час постоянно'] == 1) & (row['Тип ставки'] == 'Ставка за час'):
            #         print('Не постоянно')
            # Диапазон до изменения ставки
            dates1 = pd.date_range(start=row['Месяц'],
                                   end=row['Дата нового условия ставки (час)'] - pd.Timedelta(hours=1), freq='D')
            df1 = pd.DataFrame(dates1, columns=['Date'])
            df1['Ставка за час'] = row['Ставка за час']
            df1['Worker id'] = row['Worker id']
            df1['Worker username'] = row['Worker username']
            df1['Worker nickname'] = row['Worker nickname']
            df1['Worker role'] = row['Worker role']
            df1['city'] = row['city']
            df1['Посменная зп'] = 0

            # Диапазон после изменения ставки
            dates2 = pd.date_range(start=row['Дата нового условия ставки (час)'], end=row['Конец месяца'], freq='D')
            df2 = pd.DataFrame(dates2, columns=['Date'])
            df2['Ставка за час'] = row['Ставка измененная за час']
            df2['Worker id'] = row['Worker id']
            df2['Worker username'] = row['Worker username']
            df2['Worker nickname'] = row['Worker nickname']
            df2['Worker role'] = row['Worker role']
            df2['city'] = row['city']
            df2['Посменная зп'] = 0
            res = pd.concat([res, df1, df2], ignore_index=True)
        elif row['Тип ставки'] == 'Ставка за месяц':
            dates3 = pd.date_range(start=row['Месяц'], end=row['Месяц'], freq='D')
            df3 = pd.DataFrame(dates3, columns=['Месяц'])
            df3['Worker id'] = row['Worker id']
            df3['Worker username'] = row['Worker username']
            df3['Worker nickname'] = row['Worker nickname']
            df3['Worker role'] = row['Worker role']
            df3['city'] = row['city']
            res_month = pd.concat([res_month, df3], ignore_index=True)
            res_month['Сдельная зп'] = 0
            res_month['Посменная зп'] = row['Ставка за месяц']

    res_month['Месяц'] = pd.to_datetime(res_month['Месяц'], errors='coerce')

    # Работаю для Христоса
    # Удаляю Христоса
    res = res.drop(res[res['Worker id'] == 35].index)

    res['Месяц'] = res['Date'].apply(lambda x: x.strftime('%Y-%m-01'))
    res['Месяц'] = pd.to_datetime(res['Месяц'], errors='coerce')
    res['Date'] = pd.to_datetime(res['Date'], errors='coerce')
    res['Ставка за час'] = res['Ставка за час'].astype(float)
    res['Worker id'] = res['Worker id'].astype(int)

    # Выгрузка Ставка за час для Христоса
    select_min_efficiency = '''
        SELECT
            ds."Date",
            ds."Worker id",
            ds."Nickname" AS "Worker nickname",
            -- COALESCE(SUM(ds."Minutes"), 0) AS Minutes,
            -- COALESCE(SUM(ds."Actual duration (hours)"), 0) AS "Actual duration (hours)",
            CASE 
                WHEN COALESCE( SUM(ds."Minutes") / NULLIF(SUM(ds."Actual duration (hours)" ), 0) / 60, 0) < 0.36 THEN 5
                ELSE 6
            END AS "Ставка за час"
        FROM public.daily_shifts ds
        WHERE ds."Date" >= '2025-08-01' AND ds."Worker id" = 35
        GROUP BY ds."Date", ds."Worker id", ds."Nickname"
        -- ORDER BY ds."Date" DESC
    '''

    df_c = pd.read_sql(select_min_efficiency, engine_postgresql)
    df_c['Date'] = pd.to_datetime(df_c['Date'], errors='coerce')

    # Генерирую календарь для Христоса
    columns = ['Date', 'Worker id']
    res_c = pd.DataFrame(columns=columns)

    for index, row in df_temp[df_temp['Worker id'] == 35].iterrows():

        if (row['Ставка за час постоянно'] == 0) & (row['Тип ставки'] == 'Ставка за час'):

            #         print('Постоянно')

            dates1 = pd.date_range(start=row['Месяц'], end=row['Конец месяца'], freq='D')
            df1 = pd.DataFrame(dates1, columns=['Date'])
            #         df1['Ставка за час'] = row['Ставка за час']
            df1['Worker id'] = row['Worker id']
            df1['Worker username'] = row['Worker username']
            df1['Worker nickname'] = row['Worker nickname']
            df1['Worker role'] = row['Worker role']
            df1['city'] = row['city']
            res_c = pd.concat([res_c, df1], ignore_index=True)

        elif (row['Ставка за час постоянно'] == 1) & (row['Тип ставки'] == 'Ставка за час'):
            #         print('Не постоянно')
            # Диапазон до изменения ставки
            dates1 = pd.date_range(start=row['Месяц'],
                                   end=row['Дата нового условия ставки (час)'] - pd.Timedelta(hours=1), freq='D')
            df1 = pd.DataFrame(dates1, columns=['Date'])
            #         df1['Ставка за час'] = row['Ставка за час']
            df1['Worker id'] = row['Worker id']
            df1['Worker username'] = row['Worker username']
            df1['Worker nickname'] = row['Worker nickname']
            df1['Worker role'] = row['Worker role']
            df1['city'] = row['city']
            df1['Посменная зп'] = 0

            # Диапазон после изменения ставки
            dates2 = pd.date_range(start=row['Дата нового условия ставки (час)'], end=row['Конец месяца'], freq='D')
            df2 = pd.DataFrame(dates2, columns=['Date'])
            #         df2['Ставка за час'] = row['Ставка измененная за час']
            df2['Worker id'] = row['Worker id']
            df2['Worker username'] = row['Worker username']
            df2['Worker nickname'] = row['Worker nickname']
            df2['Worker role'] = row['Worker role']
            df2['city'] = row['city']
            df2['Посменная зп'] = 0
            res_c = pd.concat([res_c, df1, df2], ignore_index=True)

    res_c['Месяц'] = res_c['Date'].apply(lambda x: x.strftime('%Y-%m-01'))
    res_c['Месяц'] = pd.to_datetime(res_c['Месяц'], errors='coerce')
    res_c['Date'] = pd.to_datetime(res_c['Date'], errors='coerce')
    # res['Ставка за час'] = res['Ставка за час'].astype(float)
    res_c['Worker id'] = res_c['Worker id'].astype(int)
    res_c['Посменная зп'] = 0

    res_c = res_c.merge(df_c, on=['Date', 'Worker id', 'Worker nickname'])
    res = pd.concat([res, res_c])

    df_for_workers['Date'] = pd.to_datetime(df_for_workers['Date'])
    res['Date'] = pd.to_datetime(res['Date'])

    # Учет дат выезда_. Начало
    df_vyezd = df_tabl_stavki[['Месяц', 'Worker id',
                               'Ставка за выезд в другие города', 'Дата выезда 1',
                               'Дата выезда 2', 'Дата выезда3', 'Дата выезда4', 'Дата выезда5']]

    df_vyezd = df_vyezd[df_vyezd['Ставка за выезд в другие города'].notna()]
    df_vyezd['Ставка за выезд в другие города'] = df_vyezd['Ставка за выезд в другие города'].astype(float)
    df_vyezd['Дата выезда 1'] = pd.to_datetime(df_vyezd['Дата выезда 1'], errors='coerce')
    df_vyezd['Дата выезда 2'] = pd.to_datetime(df_vyezd['Дата выезда 2'], errors='coerce')
    df_vyezd['Дата выезда3'] = pd.to_datetime(df_vyezd['Дата выезда3'], errors='coerce')
    df_vyezd['Дата выезда4'] = pd.to_datetime(df_vyezd['Дата выезда4'], errors='coerce')
    df_vyezd['Дата выезда5'] = pd.to_datetime(df_vyezd['Дата выезда5'], errors='coerce')

    for index, row in df_vyezd.iterrows():
        if not pd.isnull(df_vyezd.loc[index]['Дата выезда 1']):
            res.loc[(res['Worker id'] == df_vyezd.loc[index]['Worker id']) & \
                    (res['Date'] == df_vyezd.loc[index]['Дата выезда 1']), 'Ставка за час'] = \
                float(df_vyezd.loc[index]['Ставка за выезд в другие города'])
        if not pd.isnull(df_vyezd.loc[index]['Дата выезда 2']):
            res.loc[(res['Worker id'] == df_vyezd.loc[index]['Worker id']) & \
                    (res['Date'] == df_vyezd.loc[index]['Дата выезда 2']), 'Ставка за час'] = \
                float(df_vyezd.loc[index]['Ставка за выезд в другие города'])
        if not pd.isnull(df_vyezd.loc[index]['Дата выезда3']):
            res.loc[(res['Worker id'] == df_vyezd.loc[index]['Worker id']) & \
                    (res['Date'] == df_vyezd.loc[index]['Дата выезда3']), 'Ставка за час'] = \
                float(df_vyezd.loc[index]['Ставка за выезд в другие города'])
        if not pd.isnull(df_vyezd.loc[index]['Дата выезда4']):
            res.loc[(res['Worker id'] == df_vyezd.loc[index]['Worker id']) & \
                    (res['Date'] == df_vyezd.loc[index]['Дата выезда4']), 'Ставка за час'] = \
                float(df_vyezd.loc[index]['Ставка за выезд в другие города'])
        if not pd.isnull(df_vyezd.loc[index]['Дата выезда5']):
            res.loc[(res['Worker id'] == df_vyezd.loc[index]['Worker id']) & \
                    (res['Date'] == df_vyezd.loc[index]['Дата выезда5']), 'Ставка за час'] = \
                float(df_vyezd.loc[index]['Ставка за выезд в другие города'])

    res = df_for_workers.merge(res, on=['Месяц', 'Date', 'Worker id', 'Worker username', 'Worker nickname'], how='left')
    res['Сдельная зп'] = res['Ставка за час'] * res['Actual working seconds'] / 3600

    res_workers_month = res.groupby(['Месяц', 'Worker id', 'Worker username', 'Worker nickname', 'Worker role', 'city']) \
        .agg({'Actual working seconds': 'sum',
              'Сдельная зп': 'sum', 'Посменная зп': 'sum'}) \
        .reset_index()
    res_month['Actual worked seconds'] = 0
    res_month['Сдельная зп'] = res_month['Сдельная зп'].astype(float)
    res_month['Actual worked seconds'] = res_month['Actual worked seconds'].astype(float)
    res_month['Worker id'] = res_month['Worker id'].astype(int)
    res_month['Date'] = res_month['Месяц']
    res_month['Actual start time'] = res_month['Месяц']
    res_month['Actual finish time'] = res_month['Месяц']


    df_zp = pd.concat([res, res_month]).fillna(0)

    res_for_workers = df_zp[['Месяц', 'Date', 'Worker id', 'Worker username', 'Worker nickname',
                             'Actual start time', 'Actual finish time', 'distance',
                             'Actual working seconds', 'Ставка за час']]

    # Очистка таблицы
    truncate_t_bike = "TRUNCATE TABLE salary_outer_1 RESTART IDENTITY;"
    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            print(f"Попытка очистить таблицу")
            # Очистка salary_inner
            connection.execute(sa.text(truncate_t_bike))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица salary_outer_1 успешно очищена!")

    res_for_workers.to_sql("salary_outer_1", engine_postgresql, if_exists="append", index=False)
    print('Таблица salary_outer_1 успешно обновлена!')

    df_zp_month = df_zp.groupby(['Месяц', 'Worker id', 'Worker username', 'Worker nickname'], as_index=False) \
        .agg({'distance': 'sum',
              'Actual working seconds': 'sum',
              'Посменная зп': 'sum',
              'Сдельная зп': 'sum',
              'Actual worked seconds': 'sum'})

    df = df_zp_month.merge(df_temp,
                           on=['Месяц', 'Worker id', 'Worker username', 'Worker nickname'],
                           how='right')
    df = df[['Месяц', 'Worker id', 'Worker username', 'Worker nickname',
             'Worker role', 'city', 'Сдельная зп', 'Посменная зп',
             'Компенсация топлива', 'Зарядка АКБ', 'Работа на складе',
             'Бонус за работу на складе', 'Бонус за ремонт', 'Возмещаемые расходы',
             'Бонус приведи друга', 'Штраф', 'Аванс', 'Всего удержано', 'Офиц_зп',
             'Actual working seconds',
             'Actual worked seconds',
             'Количество отработанных часов', 'Ставка за час',
             'Ставка измененная за час', 'Дата нового условия ставки (час)',
             'Ставка за неделю', 'Ставка измененная за неделю',
             'Дата нового условия ставки (нед)', 'Ставка за месяц',
             'Норма рабочих часов за месяц', 'Ставка свыше нормы',
             'Количество отработанных часов свыше нормы',
             'Ставка измененная за месяц', 'Дата нового условия ставки (мес)',
             'Норма расхода топлива на 100 км, литры',
             'Цена топлива за 1 литр, евро', 'Количество часов работы на складе',
             'Общее количество заряженных батарей', 'Сумма1', 'Возмещаемые расходы1',
             'Сумма2', 'Возмещаемые расходы2', 'Сумма3', 'Возмещаемые расходы3',
             'Сумма4', 'официальная зп 1', 'Сумма5', 'официальная зп 2', 'Сумма6',
             'официальная зп 3', 'Сумма7', 'официальная зп 4',
             'Сумма8', 'Удерживаемые расходы1', 'Сумма9', 'Удерживаемые расходы2',
             'Сумма10', 'Удерживаемые расходы3',
             'id_invited_worker_nickname_бонусная_программа',
             'invited_worker_nickname_бонусная_программа',
             'Процент_от_зп_бонус_прогр',
             'Нач_дата_расч_бонуса_нов_сотр',
             'Оконч_дата_расч_бонус_нов_сотр',
             'Кол-во дней для расчета бонуса',
             'Конец месяца', 'Тип ставки',
             'Ставка за час постоянно']]

    df['Дата нового условия ставки (час)'] = df['Дата нового условия ставки (час)'].fillna(df['Месяц'])
    df['Дата нового условия ставки (нед)'] = df['Дата нового условия ставки (нед)'].fillna(df['Месяц'])
    df['Дата нового условия ставки (мес)'] = df['Дата нового условия ставки (мес)'].fillna(df['Месяц'])
    df['Нач_дата_расч_бонуса_нов_сотр'] = df['Нач_дата_расч_бонуса_нов_сотр'].fillna(df['Месяц'])
    df['Оконч_дата_расч_бонус_нов_сотр'] = df['Оконч_дата_расч_бонус_нов_сотр'].fillna(df['Месяц'])
    df = df.round(2).fillna(0)

    # Бонусы за декады
    select_decades_bonus = '''
        SELECT 
            ds."month" AS "Месяц",
            ds."decade",
            ds."nomer_decada",
            ds."Worker id",
            ds."Nickname" AS "Worker nickname",
            CASE
                WHEN ds."Prediction minutes" >= 1440 AND ds."Prediction minutes" <= 1920
                    THEN
                        CASE
                            WHEN ds.min_eff >= 0.25 AND ds.min_eff < 0.35 THEN round((0.5 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.35 AND ds.min_eff < 0.4 THEN round((0.6 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.4 AND ds.min_eff < 0.5 THEN round((0.75 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.5 AND ds.min_eff < 0.6 THEN round((1 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.6 THEN round((2 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                        END
                WHEN ds."Prediction minutes" > 1920 AND ds."Prediction minutes" <= 2880
                    THEN
                        CASE
                            WHEN ds.min_eff >= 0.25 AND ds.min_eff < 0.35 THEN round((0.6 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.35 AND ds.min_eff < 0.4 THEN round((0.7 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.4 AND ds.min_eff < 0.5 THEN round((1 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.5 AND ds.min_eff < 0.6 THEN round((1.25 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.6 THEN round((2.1 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                        END
                WHEN ds."Prediction minutes" > 2880 AND ds."Prediction minutes" <= 3840
                    THEN
                        CASE
                            WHEN ds.min_eff >= 0.25 AND ds.min_eff < 0.35 THEN round((0.7 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.35 AND ds.min_eff < 0.4 THEN round((0.75 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.4 AND ds.min_eff < 0.5 THEN round((1 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.5 AND ds.min_eff < 0.6 THEN round((1.5 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.6 THEN round((2.25 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                        END
                WHEN ds."Prediction minutes" > 3840
                    THEN
                        CASE
                            WHEN ds.min_eff >= 0.25 AND ds.min_eff < 0.35 THEN round((0.75 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.35 AND ds.min_eff < 0.4 THEN round((1 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.4 AND ds.min_eff < 0.5 THEN round((1.5 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.5 AND ds.min_eff < 0.6 THEN round((2 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                            WHEN ds.min_eff >= 0.6 THEN round((2.5 * ds."Prediction minutes" / 60)::NUMERIC, 2)
                        END
                ELSE 0
            END AS bonus
        FROM 
            (SELECT 
                ds.*,
                CASE
                    WHEN ds."nomer_decada" IN (1,2) THEN 
                        CASE 
                            WHEN CURRENT_DATE - ds."decade" > 10 THEN ds."Actual duration (hours)" * 60
                            ELSE ds."Actual duration (hours)" * 60 / GREATEST(CURRENT_DATE - ds."decade", 1) * 10
                        END
                    WHEN ds."nomer_decada" IN (3) THEN
                        CASE 
                            WHEN EXTRACT(MONTH FROM ds."decade") IN (1,3,5,7,8,10,12) THEN 
                                CASE
                                    WHEN CURRENT_DATE - ds."decade" > 11 THEN ds."Actual duration (hours)" * 60
                                    ELSE ds."Actual duration (hours)" * 60 / GREATEST(CURRENT_DATE - ds."decade", 1) * ds."kol_vo_dney_3_decada"
                                END
                            WHEN EXTRACT(MONTH FROM ds."decade") IN (4,6,9,11) THEN 
                                CASE 
                                    WHEN CURRENT_DATE - ds."decade" > 10 THEN ds."Actual duration (hours)" * 60
                                    ELSE ds."Actual duration (hours)" * 60 / GREATEST(CURRENT_DATE - ds."decade", 1) * ds."kol_vo_dney_3_decada"
                                END
                            WHEN EXTRACT(MONTH FROM ds."decade") IN (2) THEN 
                                CASE 
                                    WHEN CURRENT_DATE - ds."decade" > 8 THEN ds."Actual duration (hours)" * 60
                                    ELSE ds."Actual duration (hours)" * 60 / GREATEST(CURRENT_DATE - ds."decade", 1) * ds."kol_vo_dney_3_decada"
                                END
                        END
                END AS "Prediction minutes"
            FROM 
                (
                SELECT
                    ds."month",
                    ds."decade", 
                    ds."nomer_decada", 
                    ds."kol_vo_dney_3_decada", 
                    ds."Worker id", 
                    ds."Nickname",
                    SUM(ds."Actual duration (hours)") * 60 AS "Actual minutes",
                    SUM(ds."Actual duration (hours)") AS "Actual duration (hours)",
                    ROUND((SUM(ds."Minutes") / NULLIF(SUM(ds."Actual duration (hours)"), 0))::numeric / 60, 2) AS "min_eff"
                FROM 
                    (
                    SELECT
                        date_trunc('month', ds."Date") :: date AS "month",
                        ds.*,
                        (CONCAT(EXTRACT(YEAR FROM "Date"), '-', 
                                EXTRACT(MONTH FROM "Date"), '-', 
                                CASE 
                                    WHEN EXTRACT(DAY FROM "Date") <= 10 THEN 1
                                    WHEN EXTRACT(DAY FROM "Date") <= 20 THEN 11
                                    ELSE 21
                                END	
                        )) :: date AS decade,
                        CASE 
                            WHEN EXTRACT(DAY FROM "Date") <= 10 THEN 1
                            WHEN EXTRACT(DAY FROM "Date") <= 20 THEN 2
                            WHEN EXTRACT(DAY FROM "Date") <= 31 THEN 3
                        END AS "nomer_decada",
                        CASE
                            WHEN EXTRACT(MONTH FROM "Date") IN (1,3,5,7,8,10,12) THEN 11
                            WHEN EXTRACT(MONTH FROM "Date") IN (4,6,9,11) THEN 10
                            ELSE 8
                        END AS "kol_vo_dney_3_decada"
                    FROM public.daily_shifts ds
                    WHERE "Date" >= '2025-08-01'
                    ) ds
                GROUP BY ds."month", ds."decade", ds."nomer_decada", ds."kol_vo_dney_3_decada", ds."Worker id", ds."Nickname"
                ) ds
            ORDER BY ds."decade", ds."Nickname") ds
    '''

    df_decades_bonus_temp = pd.read_sql(select_decades_bonus, engine_postgresql).fillna(0)
    df_decades_bonus = df_decades_bonus_temp.pivot_table(index=['Месяц', 'Worker id', 'Worker nickname'],
                                                         columns='nomer_decada',
                                                         values='bonus') \
        .reset_index().fillna(0) \
        .rename(columns={1: 'Бонус за 1 декаду',
                         2: 'Бонус за 2 декаду',
                         3: 'Бонус за 3 декаду'})
    df_decades_bonus['Месяц'] = pd.to_datetime(df_decades_bonus['Месяц'], errors='coerce')
    df1 = df.merge(df_decades_bonus, on=['Месяц', 'Worker id', 'Worker nickname'], how='left')

    # Бонусы за месяц
    select_month_bonus = '''
        SELECT 
            ds."month" AS "Месяц",
            ds."Worker id",
            ds."Nickname" AS "Worker nickname",
            -- ds."rn",
            CASE 
                WHEN ds."rn" = 1 THEN 100
                WHEN ds."rn" = 2 THEN 75
                WHEN ds."rn" IN (3,4,5) THEN 50
            END AS "Бонус за месяц"
        FROM
            (
            SELECT 
                ds.*,
                rank() OVER (PARTITION BY ds."month" ORDER BY ds."min_eff" DESC) AS rn
            FROM 
                (SELECT 
                    ds.*,
                    CASE 
                        WHEN EXTRACT(MONTH FROM ds."month") IN (1,3,5,7,8,10,12) THEN 
                            CASE
                                WHEN CURRENT_DATE - ds."month" > 31 THEN ds."Actual duration (hours)" * 60
                                ELSE ds."Actual duration (hours)" * 60 / GREATEST(CURRENT_DATE - ds."month", 1) * ds."kol_vo_dney_month"
                            END
                        WHEN EXTRACT(MONTH FROM ds."month") IN (4,6,9,11) THEN 
                            CASE 
                                WHEN CURRENT_DATE - ds."month" > 30 THEN ds."Actual duration (hours)" * 60
                                ELSE ds."Actual duration (hours)" * 60 / GREATEST(CURRENT_DATE - ds."month", 1) * ds."kol_vo_dney_month"
                            END
                        WHEN EXTRACT(MONTH FROM ds."month") IN (2) THEN 
                            CASE 
                                WHEN CURRENT_DATE - ds."month" > 28 THEN ds."Actual duration (hours)" * 60
                                ELSE ds."Actual duration (hours)" * 60 / GREATEST(CURRENT_DATE - ds."month", 1) * ds."kol_vo_dney_month"
                            END
                    END AS "Prediction minutes"
                FROM 
                    (SELECT 
                        ds."month", 
                        ds."kol_vo_dney_month", 
                        ds."Worker id", 
                        ds."Nickname",
                        SUM(ds."Actual duration (hours)") * 60 AS "Actual minutes",
                        SUM(ds."Actual duration (hours)") AS "Actual duration (hours)",
                        ROUND((SUM(ds."Minutes") / NULLIF(SUM(ds."Actual duration (hours)"), 0) / 60)::numeric, 2) AS "min_eff"
                    FROM 
                        (SELECT
                            date_trunc('month', ds."Date") :: date AS "month",
                            CASE
                                WHEN EXTRACT(MONTH FROM "Date") IN (1,3,5,7,8,10,12) THEN 31
                                WHEN EXTRACT(MONTH FROM "Date") IN (4,6,9,11) THEN 30
                                ELSE 28
                            END AS "kol_vo_dney_month",
                            ds.*
                        FROM daily_shifts ds
                        WHERE "Date" >= '2025-08-01') AS ds
                    GROUP BY ds."month", ds."kol_vo_dney_month", ds."Worker id", ds."Nickname"
                    ) ds
                ) ds
            WHERE ds."Prediction minutes" > 4000
            ORDER BY ds."min_eff" DESC
            ) ds
        WHERE rn <= 5
    '''

    df_month_bonus_temp = pd.read_sql(select_month_bonus, engine_postgresql).fillna(0)
    df_month_bonus_temp['Месяц'] = pd.to_datetime(df_month_bonus_temp['Месяц'], errors='coerce')

    df_res = df1.merge(df_month_bonus_temp, on=['Месяц', 'Worker id', 'Worker nickname'], how='left').fillna(0)
    df_res['add_time'] = pd.Timestamp.now() + pd.Timedelta(hours=3)

    # Очистка таблицы
    truncate_t_bike = "TRUNCATE TABLE salary_inner RESTART IDENTITY;"

    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            print(f"Попытка очистить таблицу")
            # Очистка salary_inner
            connection.execute(sa.text(truncate_t_bike))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица salary_inner успешно очищена!")

    df_res.to_sql("salary_inner", engine_postgresql, if_exists="append", index=False)
    print('Таблица salary_inner успешно обновлена!')

    # Расчет зп. Конец 30.09.2025

    #   Обновление таблицы t_bike_use в Postgres. Начало
    # Максимальный id записи в принимающей таблице
    select_max_id_t_bike_use = '''
    SELECT 
    	MAX(id)
    FROM damir.t_bike_use
    '''
    df_max_id_t_bike_use_postgres = pd.read_sql(select_max_id_t_bike_use, engine_postgresql)
    max_id_t_bike_use = int(df_max_id_t_bike_use_postgres.iloc[0].iloc[0])

    select_fresh_t_bike_use_mysql = '''
            SELECT
                NOW() AS add_time,
                IfNULL(t_bike_use.id,0) AS id,
                IfNULL(t_bike_use.uid,0) AS uid,
                IfNULL(t_bike_use.bid,0) AS bid,
                IfNULL(t_bike_use.start_time,0) AS start_time,
                IfNULL(t_bike_use.end_time,0) AS end_time,
                IfNULL(t_bike_use.duration,0) AS duration ,
                IfNULL(t_bike_use.distance,0) AS distance ,
                IfNULL(t_bike_use.orbit,0) AS orbit,
                IfNULL(t_bike_use.start_lat,0) AS start_lat,
                IfNULL(t_bike_use.start_lng,0) AS start_lng,
                IfNULL(t_bike_use.end_lat,0) AS end_lat,
                IfNULL(t_bike_use.end_lng,0) AS end_lng,
                IfNULL(t_bike_use.ispay,0) AS ispay,
                IfNULL(t_bike_use.`date`,0) AS `date`,
                IfNULL(t_bike_use.lock_location,0) AS lock_location,
                IfNULL(t_bike_use.out_area,0) AS out_area,
                IfNULL(t_bike_use.open_way,0) AS open_way,
                IfNULL(t_bike_use.ride_amount,0) AS ride_amount,
                IfNULL(t_bike_use.ride_status,0) AS ride_status,
                IfNULL(t_bike_use.old_date,0) AS old_date,
                IfNULL(t_bike_use.close_way,0) AS close_way,
                IfNULL(t_bike_use.old_duration,0) AS old_duration,
                IfNULL(t_bike_use.admin_id,0) AS admin_id,
                IfNULL(t_bike_use.update_time,0) AS update_time,
                IfNULL(t_bike_use.lock_time,0) AS lock_time, 
                IfNULL(t_bike_use.host_id,0) AS host_id,
                IfNULL(t_bike_use.ride_user,0) AS ride_user,
                IfNULL(t_bike_use.group_ride,0) AS group_ride,
                IfNULL(t_bike_use.start_area,0) AS start_area,
                IfNULL(t_bike_use.end_area,0) AS end_area,
                IfNULL(t_bike_use.stripe_charge,0) AS stripe_charge,
                IfNULL(t_bike_use.stripe_refund,0) AS stripe_refund,
                IfNULL(t_bike_use.pause_duration,0) AS pause_duration,
                IfNULL(t_bike_use.discount,0) AS discount,
                IfNULL(t_bike_use.subscription_id,0) AS subscription_id,
                IfNULL(t_bike_use.subscription_mapping_id,0) AS subscription_mapping_id,
                IfNULL(t_bike_use.route_image,0) AS route_image,
                IfNULL(t_bike_use.parking_image,0) AS parking_image,
                IfNULL(t_bike_use.force_stop,0) AS force_stop,
                IfNULL(t_bike_use.force_stop_comment,0) AS force_stop_comment,
                IfNULL(t_bike_use.lights,0) AS lights,
                IfNULL(t_bike_use.gear,0) AS gear,
                IfNULL(t_bike_use.sent_unlock_time,0) AS sent_unlock_time,
                IfNULL(t_bike_use.recalculated,0) AS recalculated,
                IfNULL(t_bike_use.notified,0) AS notified,
                IfNULL(t_bike_use.notified_time,0) AS notified_time,
                IfNULL(t_bike_use.speed_zone_id,0) AS speed_zone_id,
                IfNULL(t_bike_use.admin_note,0) AS admin_note,
                IfNULL(t_bike_use.subscription_payment_id,0) AS subscription_payment_id,
                IfNULL(t_bike_use.subscr_paid_before_ride_balance,0) AS subscr_paid_before_ride_balance
            FROM shamri.t_bike_use
            WHERE t_bike_use.id > {max_id_t_bike_use}
                    '''.format(max_id_t_bike_use=max_id_t_bike_use)

    df_fresh_t_bike_use_mysql = pd.read_sql(select_fresh_t_bike_use_mysql, engine_mysql)
    df_fresh_t_bike_use_mysql.replace('', '0').to_sql("t_bike_use", engine_postgresql, if_exists="append", index=False)
    print('Added {x} records to t_bike_use in Postgres!'.format(x=df_fresh_t_bike_use_mysql.shape[0]))

    #   Обновление таблицы t_bike_use в Postgres. Конец

    #   Обновление таблицы t_trade в Postgres. Начало
    # Максимальный id записи в принимающей таблице
    select_max_id_t_trade = '''
    SELECT 
        MAX(id)
    FROM damir.t_trade
    '''
    df_max_id_t_trade_postgres = pd.read_sql(select_max_id_t_trade, engine_postgresql)
    max_id_t_trade = int(df_max_id_t_trade_postgres.iloc[0].iloc[0])

    select_fresh_t_trade_mysql = '''
        SELECT
            NOW() AS add_time ,
            tt.id ,
            COALESCE(tt.record_id, '0') AS record_id,
            COALESCE(tt.status, 0) AS status,
            COALESCE(tt.way, 0) AS way,
            tt.`date` ,
            COALESCE(tt.amount, 0) AS amount,
            COALESCE(tt.`type`, 0) AS type,
            COALESCE(tt.uid, 0) AS uid,
            COALESCE(tt.out_pay_id, 0) AS out_pay_id,
            COALESCE(tt.notify, 0) AS notify,
            COALESCE(tt.out_trade_no, '0') AS out_trade_no,
            COALESCE(tt.balance, 0) AS balance,
            tt.city_id ,
            COALESCE(tt.account_pay_amount, 0) AS account_pay_amount,
            COALESCE(tt.gift_pay_amount, 0) AS gift_pay_amount,
            COALESCE(tt.prefix, '0') AS prefix,
            COALESCE(tt.card_type, '0') AS card_type,
            COALESCE(tt.card_issue_bank, '0') AS card_issue_bank,
            COALESCE(tt.card_number, '0') AS card_number,
            COALESCE(tt.industry_id, 0) AS industry_id,
            COALESCE(tt.admin_id, 0) AS admin_id,
            COALESCE(tt.error_message, '0') AS error_message,
            COALESCE(tt.discount, 0) AS discount
        FROM shamri.t_trade tt
        WHERE tt.id > {max_id_t_trade}
                    '''.format(max_id_t_trade=max_id_t_trade)

    df_fresh_t_trade_mysql = pd.read_sql(select_fresh_t_trade_mysql, engine_mysql)
    df_fresh_t_trade_mysql.replace('', '0').to_sql("t_trade", engine_postgresql, if_exists="append", index=False)
    print('Added {x} records to t_trade in Postgres!'.format(x=df_fresh_t_trade_mysql.shape[0]))
    #   Обновление таблицы t_trade в Postgres. Конец

    #   Обновление таблицы t_subscription_mapping в Postgres. Начало
    # Максимальный id записи в принимающей таблице
    select_max_id_t_subscription_mapping = '''
        SELECT 
            MAX(id)
        FROM damir.t_subscription_mapping
    '''
    df_max_id_t_subscription_mapping_postgres = pd.read_sql(select_max_id_t_subscription_mapping, engine_postgresql)
    max_id_t_subscription_mapping = int(df_max_id_t_subscription_mapping_postgres.iloc[0].iloc[0])

    select_fresh_t_subscription_mapping_mysql = '''
        SELECT
            NOW() AS add_time ,
            tsm.id ,
            tsm.user_id ,
            tsm.subscription_id ,
            tsm.spent ,
            tsm.end_time ,
            tsm.start_time 
        FROM shamri.t_subscription_mapping tsm 
            WHERE tsm.id > {max_id_t_subscription_mapping}
                    '''.format(max_id_t_subscription_mapping=max_id_t_subscription_mapping)
    df_fresh_t_subscription_mapping_mysql = pd.read_sql(select_fresh_t_subscription_mapping_mysql, engine_mysql)
    df_fresh_t_subscription_mapping_mysql.replace('', '0').to_sql("t_subscription_mapping", engine_postgresql,
                                                                  if_exists="append", index=False)
    print('Added {x} records to t_subscription_mapping in Postgres!'.format(
        x=df_fresh_t_subscription_mapping_mysql.shape[0]))
    #   Обновление таблицы t_subscription_mapping в Postgres. Конец





    #   Обновление таблицы t_ride_event_log в Postgres. Начало
    # Максимальный id записи в принимающей таблице
    select_max_id_t_ride_event_log = '''
        SELECT 
            MAX(id)
        FROM damir.t_ride_event_log
        '''
    df_max_id_t_ride_event_log_postgres = pd.read_sql(select_max_id_t_ride_event_log, engine_postgresql)
    max_id_t_ride_event_log = int(df_max_id_t_ride_event_log_postgres.iloc[0].iloc[0])

    select_fresh_t_ride_event_log_mysql = '''
                SELECT
                    NOW() AS add_time,
                    t_ride_event_log.id,
                    t_ride_event_log.ride_id,
                    t_ride_event_log.event,
                    t_ride_event_log.description,
                    t_ride_event_log.created 
                FROM shamri.t_ride_event_log
                WHERE t_ride_event_log.id > {max_id_t_ride_event_log}
                        '''.format(max_id_t_ride_event_log=max_id_t_ride_event_log)

    df_fresh_t_ride_event_log_mysql = pd.read_sql(select_fresh_t_ride_event_log_mysql, engine_mysql)

    df_fresh_t_ride_event_log_mysql.replace('', '0').to_sql("t_ride_event_log", engine_postgresql, if_exists="append",
                                                            index=False)
    print('Added {x} records to t_ride_event_log in Postgres!'.format(x=df_fresh_t_ride_event_log_mysql.shape[0]))

    #   Обновление таблицы t_ride_event_log в Postgres. Конец

    #   Обновление таблицы t_payment_details в Postgres. Начало
    #   Максимальный id записи в принимающей таблице
    select_max_id_t_payment_details = '''
            SELECT 
                MAX(id)
            FROM damir.t_payment_details
        '''
    df_max_id_t_payment_details_postgres = pd.read_sql(select_max_id_t_payment_details, engine_postgresql)

    max_id_t_payment_details = int(df_max_id_t_payment_details_postgres.iloc[0].iloc[0])

    select_fresh_t_payment_details_mysql = '''
                SELECT 
                    NOW() AS add_time ,
                    IFNULL(tpd.id, 0) AS id ,
                    IFNULL(tpd.user_id, 0) AS user_id ,
                    IFNULL(tpd.ride_id, 0) AS ride_id ,
                    IFNULL(tpd.price, 0) AS price ,
                    IFNULL(tpd.unlock_price, 0) AS unlock_price ,
                    IFNULL(tpd.unit_count, 0) AS unit_count ,
                    IFNULL(tpd.unit_type, 0) AS unit_type ,
                    IFNULL(tpd.currency, 'EUR') AS currency ,
                    IFNULL(tpd.hold_price, 0) AS hold_price ,
                    IFNULL(tpd.hold_count, 0) AS hold_count ,
                    IFNULL(tpd.hold_unit_type, 0) AS hold_unit_type ,
                    IFNULL(tpd.total_ride_cost, 0) AS total_ride_cost ,
                    IFNULL(tpd.pause_cost, 0) AS pause_cost,
                    IFNULL(tpd.debit_balance, 0) AS debit_balance,
                    IFNULL(tpd.debit_gift_amount, 0) AS debit_gift_amount,
                    IFNULL(tpd.debit_cash, 0) AS debit_cash,
                    IFNULL(tpd.subscription_pause_sec, 0) AS subscription_pause_sec ,
                    IFNULL(tpd.subscription_cost, 0) AS subscription_cost ,
                    IFNULL(tpd.subscription_id, 0) AS subscription_id ,
                    IFNULL(tpd.subscription_mapping_id, 0) AS subscription_mapping_id ,
                    IFNULL(tpd.subscription_paid_before_ride, 0) AS subscription_paid_before_ride ,
                    IFNULL(tpd.created, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS created ,
                    IFNULL(tpd.unlock_sec_included, 0) AS unlock_sec_included,
                    IFNULL(tpd.area_bonus_type, 0) AS area_bonus_type,
                    IFNULL(tpd.area_bonus_value, 0) AS area_bonus_value,
                    IFNULL(tpd.area_bonus_amount, 0) AS area_bonus_amount,
                    IFNULL(tpd.bike_discount_id, 0) AS bike_discount_id,
                    IFNULL(tpd.bike_discount_amount, 0) AS bike_discount_amount,
                    IFNULL(tpd.bike_discount_value, 0) AS bike_discount_value,
                    IFNULL(tpd.bike_discount_type, 0) AS bike_discount_type
                FROM shamri.t_payment_details tpd 
                WHERE tpd.id > {max_id_t_payment_details}
                        '''.format(max_id_t_payment_details=max_id_t_payment_details)
    df_fresh_t_payment_details_mysql = pd.read_sql(select_fresh_t_payment_details_mysql, engine_mysql)
    df_fresh_t_payment_details_mysql.replace('', '0').to_sql("t_payment_details", engine_postgresql, if_exists="append",
                                                             index=False)
    print('Added {x} records to t_payment_details in Postgres!'.format(x=df_fresh_t_payment_details_mysql.shape[0]))
    #   Обновление таблицы t_payment_details в Postgres. Конец



    # История чекапов. Начало
    select_checkups_history = r'''
        WITH work_tab AS (
            SELECT 
                res_tab."Date" ,
                res_tab."Worker id" ,
                res_tab."Worker username" ,
                res_tab."Worker nickname"
            FROM 
                (
                SELECT 
                    grg."Date"::date ,
                    grg."Worker id"::int ,
                    grg."Worker username" ,
                    grg."Worker nickname" ,
                    RANK() OVER (PARTITION BY grg."Date", grg."Worker id" ORDER BY grg."Actual start time" DESC) AS rn
                FROM damir.grafik_rabot_google grg
                WHERE grg."Date" != ''
                    AND grg."Date"::date = current_date
                    AND grg."Actual start time" != '00:00:70'
                    ) AS res_tab
            WHERE res_tab.rn = 1
        ),
        planned_tab AS (
            SELECT 
                tb_all.release_time ,
                tb_all.id ,
                tb_all."number" ,
                tc."name" AS "City" ,
                COALESCE(rides.number_of_rides,0) AS number_of_rides
            FROM 
                (
                SELECT
                    tb.id ,
                    tb."number" ,
                    tb.model ,
                    tb.release_time ,
                    tb.city_id 
                FROM damir.t_bike tb
                ) AS tb_all
                LEFT JOIN (
                        SELECT
                            tb.id ,
                            tb.release_time,
                            tb.model ,
                            tb."number",
                            count(tbu.ride_amount) AS number_of_rides
                        FROM damir.t_bike_use tbu 
                        LEFT JOIN damir.t_bike tb ON tbu.bid = tb.id 
                        LEFT JOIN damir.t_city tc ON tb.city_id = tc.id
                        WHERE 
                            to_timestamp(tbu.start_time) > tb.release_time
                            AND 
                            to_timestamp(tbu.start_time) >= '2024-07-21'
                            AND 
                            tbu.ride_status!=5
                        GROUP BY tb.id, tb.release_time, tb.model, tb."number", tc."name"
                    ) AS rides ON tb_all.id = rides.id 
            LEFT JOIN damir.t_city tc ON tb_all.city_id = tc.id
            ORDER BY tb_all."number" ASC
        ),
        completed_tab AS (
            SELECT 
                tb_all.release_time ,
                tb_all.id ,
                tb_all."number" ,
                tc."name" AS "City" ,
                COALESCE(rides.number_of_rides,0) AS number_of_rides
            FROM 
                (
                SELECT
                    tb.id ,
                    tb."number" ,
                    tb.model ,
                    tb.release_time ,
                    tb.city_id 
                FROM damir.t_bike tb
                ) AS tb_all
                LEFT JOIN (
                        SELECT
                            tb.id ,
                            tb.release_time,
                            tb.model ,
                            tb."number",
                            count(tbu.ride_amount) AS number_of_rides
                        FROM damir.t_bike_use tbu 
                        LEFT JOIN damir.t_bike tb ON tbu.bid = tb.id 
                        LEFT JOIN damir.t_city tc ON tb.city_id = tc.id
                        WHERE 
                            to_timestamp(tbu.start_time) > tb.release_time
                            AND 
                            to_timestamp(tbu.start_time) >= '2024-07-21'
                            AND 
                            tbu.ride_status!=5
                        GROUP BY tb.id, tb.release_time, tb.model, tb."number", tc."name"
                    ) AS rides ON tb_all.id = rides.id 
            LEFT JOIN damir.t_city tc ON tb_all.city_id = tc.id
            ORDER BY tb_all."number" ASC
        )
        SELECT
            --target_tab."City" ,
			COALESCE(target_tab."City", planned_tab."City" , completed_tab."City") AS "City" ,
            COALESCE(target_tab."target", 0) AS target,
            COALESCE(planned_tab.planned, 0) AS planned,
            COALESCE(completed_tab.completed, 0) AS completed ,
            NOW() + INTERVAL '3 hours' AS add_time
        FROM
            (
            SELECT 
                temp_tab."City" ,
                SUM(temp_tab.target_count) AS target 
            FROM 
                (
                SELECT 
                    cgfg."City" ,
                    cgfg."Worker Id" ,
                    cgfg.user_name ,
                    cgfg.nickname ,
                    cgfg."Цель на смену" ,
                    work_tab.* ,
                    CASE 
                        WHEN work_tab."Worker id" IS NOT NULL THEN cgfg."Цель на смену"
                        ELSE 0
                    END	AS target_count
                FROM damir.checkup_goals_from_google cgfg 
                LEFT JOIN work_tab ON cgfg."Worker Id" = work_tab."Worker id"
                ) AS temp_tab 
            GROUP BY temp_tab."City"
            ) AS target_tab
        FULL JOIN 
            (
                SELECT 
                    temp_tab."City" ,
                    COUNT(temp_tab."number") AS planned
                FROM 
                    (
                    SELECT 
                        tc."name" AS "City" ,
                        tb."number"
                    FROM public.tasks t 
                    LEFT JOIN planned_tab ON t."Bike id" = planned_tab.id 
                    LEFT JOIN damir.t_bike tb ON t."Bike id" = tb.id 
                    LEFT JOIN damir.t_city tc ON tb.city_id = tc.id
                    WHERE 
                        t."Note" ~ '(^|\s)ch([34])?(\*(1[.,]5|2))?([,/\s]|$)'
                        AND 
                        DATE(t."Created") = current_date
                        AND 
                        t."Status" IN ('in progress', 'unassigned')
                        --t."Status" IN ('resolved')
                        ) AS temp_tab
                GROUP BY temp_tab."City"
            ) AS planned_tab ON target_tab."City" = planned_tab."City" 
        FULL JOIN 
            (
                SELECT 
                    temp_tab."City" ,
                    COUNT(temp_tab."number") AS completed
                FROM 
                    (
                    SELECT 
                        tc."name" AS "City" ,
                        tb."number"
                    FROM public.tasks t 
                    LEFT JOIN completed_tab ON t."Bike id" = completed_tab.id 
                    LEFT JOIN damir.t_bike tb ON t."Bike id" = tb.id 
                    LEFT JOIN damir.t_city tc ON tb.city_id = tc.id
                    WHERE 
                        t."Note" ~ '(^|\s)ch([34])?(\*(1[.,]5|2))?([,/\s]|$)'
                        AND 
                        DATE(t."Created") = current_date
                        AND 
                        --t."Status" IN ('in progress', 'unassigned')
                        t."Status" IN ('resolved')
                        ) AS temp_tab
                GROUP BY temp_tab."City"
            ) AS completed_tab ON target_tab."City" = completed_tab."City"
    '''

    df_checkups_history = pd.read_sql(select_checkups_history, engine_postgresql)

    df_checkups_history.to_sql("checkups_history", engine_postgresql, if_exists="append", index=False)

    print('Added {x} records to checkups_history in Postgres!'.format(x=df_checkups_history.shape[0]))
    # История чекапов. Конец

    # Процент исполнения чекапов. Начало
    # Выгрузка старых чекапов
    select_old_checkups = '''
        SELECT 
            tcs.add_time::date AS current_date ,
            tcs.*
        FROM damir.today_checkup_scooters tcs
    '''

    df_old_checkups = pd.read_sql(select_old_checkups, engine_postgresql)
    df_old_checkups['add_time'] = pd.to_datetime(df_old_checkups['add_time'])
    df_old_checkups['current_date'] = pd.to_datetime(df_old_checkups['current_date'])
    df_old_checkups['release_time'] = pd.to_datetime(df_old_checkups['release_time'])
    df_old_checkups['number_of_rides'] = df_old_checkups['number_of_rides'].astype(int)

    # Выгрузка новых чекапов
    select_new_checkups = r'''
        WITH number_of_rides AS (
            SELECT 
                tb_all.release_time ,
                tb_all.id ,
                tb_all."number" ,
                tc."name" AS "City" ,
                COALESCE(rides.number_of_rides,0) AS number_of_rides
            FROM 
                (
                SELECT
                    tb.id ,
                    tb."number" ,
                    tb.model ,
                    tb.release_time ,
                    tb.city_id 
                FROM damir.t_bike tb
                ) AS tb_all
                LEFT JOIN (
                        SELECT
                            tb.id ,
                            tb.release_time,
                            tb.model ,
                            tb."number",
                            count(tbu.ride_amount) AS number_of_rides
                        FROM damir.t_bike_use tbu 
                        LEFT JOIN damir.t_bike tb ON tbu.bid = tb.id 
                        LEFT JOIN damir.t_city tc ON tb.city_id = tc.id
                        WHERE 
                            to_timestamp(tbu.start_time) > tb.release_time
                            AND 
                            to_timestamp(tbu.start_time) >= '2024-07-21'
                            AND 
                            tbu.ride_status!=5
                        GROUP BY tb.id, tb.release_time, tb.model, tb."number", tc."name"
                    ) AS rides ON tb_all.id = rides.id 
            LEFT JOIN damir.t_city tc ON tb_all.city_id = tc.id
            ORDER BY tb_all."number" ASC
        )
        SELECT 
            (now() + INTERVAL '3 hours')::date AS current_date ,
            now() + INTERVAL '3 hours' AS add_time ,
            tb."number"  AS "Scooter ID",
            tc."name" AS "City",
            number_of_rides.release_time ,
            number_of_rides.number_of_rides
        FROM public.tasks t 
        LEFT JOIN number_of_rides ON t."Bike id" = number_of_rides.id 
        LEFT JOIN damir.t_bike tb ON t."Bike id" = tb.id 
        LEFT JOIN damir.t_city tc ON tb.city_id = tc.id
        WHERE 
            t."Note" ~ '(^|\s)ch([34])?(\*(1[.,]5|2))?([,/\s]|$)'
            AND 
            DATE(t."Created") = current_date
            AND 
            t."Status" IN ('in progress', 'unassigned')
            --t."Status" IN ('resolved')
        ORDER BY coalesce(number_of_rides.number_of_rides,0) DESC
    '''

    df_new_checkups = pd.read_sql(select_new_checkups, engine_postgresql)
    df_new_checkups['add_time'] = pd.to_datetime(df_new_checkups['add_time'])
    df_new_checkups['add_time'] = df_new_checkups['add_time'].dt.tz_localize(None)
    df_new_checkups['current_date'] = pd.to_datetime(df_new_checkups['current_date'])
    df_new_checkups['release_time'] = pd.to_datetime(df_new_checkups['release_time'])
    df_new_checkups['number_of_rides'] = df_new_checkups['number_of_rides'].fillna(0).astype(int)

    # Соединение старых и новых чекапов
    df_temp = pd.concat([df_old_checkups, df_new_checkups])

    # Оставляю только за сегодня
    df_temp = df_temp[df_temp['current_date'] == pd.Timestamp('today').strftime("%Y-%m-%d")]

    # Удаляю дубликаты чекапов
    df_temp.drop_duplicates(subset=['Scooter ID'], inplace=True)
    df_temp.drop(columns=['current_date'], inplace=True)

    # Очистка today_checkup_scooters

    # Очистка таблицы
    truncate_t_bike = "TRUNCATE TABLE today_checkup_scooters RESTART IDENTITY;"

    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            print(f"Попытка очистить таблицу")
            # Очистка grafik_rabot_google
            connection.execute(sa.text(truncate_t_bike))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица today_checkup_scooters успешно очищена!")

    # Загрузка обновленного списка чекапов
    df_temp.to_sql("today_checkup_scooters", engine_postgresql, if_exists="append", index=False)
    print('Таблица today_checkup_scooters успешно обновлена!')

    # Процент исполнения чекапов. Конец

    # Обновление t_area в Postgresql. Начало
    # Копирую t_area
    select_t_area = '''
        SELECT 
            NOW() AS add_time ,
            IFNULL(ta.id, 0) AS id ,
            IFNULL(ta.note, '0') AS note ,
            IFNULL(ta.name, '0') AS name ,
            IFNULL(ta.detail, '0') AS detail ,
            ta.lat AS lat ,
            ta.lng AS lng ,
            ta.`type` AS "type",
            ta.city_id AS city_id ,
            IFNULL(ta.`date`, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS "date" ,
            ta.ride_start_count AS ride_start_count ,
            ta.ride_end_count AS ride_end_count ,
            ta.speed_limit AS speed_limit ,
            ta.is_gift_area AS is_gift_area ,
            ta.bike_type AS bike_type ,
            ta.definition_type AS definition_type ,
            ta.radius AS radius ,
            IFNULL(ta.address, '0') AS address ,
            ta.active AS active ,
            IFNULL(ta.user_group_id, 0) AS user_group_id ,
            ta.bonus_type AS bonus_type ,
            ta.bonus_value AS bonus_value
        FROM shamri.t_area ta
    '''

    df_t_area = pd.read_sql(select_t_area, engine_mysql)

    # Очистка таблицы
    truncate_t_bike = "TRUNCATE TABLE t_area RESTART IDENTITY;"

    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            print(f"Попытка очистить таблицу")
            # Очистка t_area
            connection.execute(sa.text(truncate_t_bike))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица t_area успешно очищена!")

    df_t_area.to_sql("t_area", engine_postgresql, if_exists="append", index=False)
    print('Таблица t_area успешно обновлена!')
    # Обновление t_area в Postgresql. Конец

    # Обновление t_areas_parkings в Postgresql. Начало
    select_active_parkings = '''
        SELECT 
            ta.id AS parking_id ,
            ta."name" AS parking_name ,
            ta.detail AS parking_detail
        FROM damir.t_area ta
        WHERE ta.active = 1
            AND ta.detail != ''
    '''
    df_active_parkings = pd.read_sql(select_active_parkings, engine_postgresql)
    select_areas = '''
        SELECT
            ta.id AS area_id ,
            ta."name" AS area_name ,
            ta.detail AS area_detail
        FROM damir.t_area ta
        WHERE ta."name" LIKE '%%| Area |%%'
    '''
    df_areas = pd.read_sql(select_areas, engine_postgresql)

    # Декодирование полигонов
    df_active_parkings['parking_detail_tuple'] = df_active_parkings['parking_detail'].apply(decode_polyline_to_tuples)
    df_areas['area_detail_tuple'] = df_areas['area_detail'].apply(decode_polyline_to_tuples)

    # Соединение areas и parkings
    df_areas_parkings = df_active_parkings[['parking_id', 'parking_name', 'parking_detail_tuple']] \
        .merge(df_areas[['area_id', 'area_name', 'area_detail_tuple']], how='cross')
    df_areas_parkings['res'] = df_areas_parkings.apply(poly_contains, axis=1)
    df_areas_parkings['add_time'] = pd.Timestamp.now()
    res = df_areas_parkings[df_areas_parkings['res'] == True]
    res = res[['add_time', 'area_id', 'area_name', 'parking_id', 'parking_name']]

    # Очистка таблицы t_areas_parkings
    truncate_t_areas_parkings = "TRUNCATE TABLE t_areas_parkings RESTART IDENTITY;"
    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            print(f"Попытка очистить таблицу")
            # Очистка t_areas_parkings
            connection.execute(sa.text(truncate_t_areas_parkings))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица truncate_t_areas_parkings успешно очищена!")

    res.to_sql("t_areas_parkings", engine_postgresql, if_exists="append", index=False)
    print('Таблица t_areas_parkings успешно обновлена!')

    # Обновление t_areas_parkings в Postgresql. Конец


    # Обновление t_parking_stats в Postgresql. Начало

    # id последней поездки(заказа)
    select_max_id_order = '''
        SELECT 
            MAX(t.id)
        FROM damir.t_orders t
    '''

    df_max_id_order = pd.read_sql(select_max_id_order, engine_postgresql)
    max_id_order = int(df_max_id_order.iloc[0].iloc[0])

    select_fresh_parking_stats = '''
    WITH t_res AS 
    (
        SELECT 
            --now() AS add_time ,
            tbu.id ,
            --tbu.uid ,
            --tbu.bid ,
            --tbu.start_time ,
            --tbu.end_time ,
            tbu.start_lat ,
            tbu.start_lng ,
            tbu.end_lat ,
            tbu.end_lng ,
            ta.lat AS  area_lat,
            ta.lng AS area_lng ,
            ta.city_id ,
            --tc.id AS city_id ,
            ta.name AS area,
            ta.id AS area_id ,
                6371000 * acos(
                cos(radians(tbu.start_lat)) * 
                cos(radians(ta.lat)) * 
                cos(radians(ta.lng) - radians(tbu.start_lng)) + 
                sin(radians(tbu.start_lat)) * 
                sin(radians(ta.lat))
            ) AS start_distance ,
                6371000 * acos(
                cos(radians(tbu.end_lat)) * 
                cos(radians(ta.lat)) * 
                cos(radians(ta.lng) - radians(tbu.end_lng)) + 
                sin(radians(tbu.end_lat)) * 
                sin(radians(ta.lat))
            ) AS end_distance ,
            rank() OVER (PARTITION BY tbu.id ORDER BY 6371000 * acos(
                                                                    cos(radians(tbu.start_lat)) * 
                                                                    cos(radians(ta.lat)) * 
                                                                    cos(radians(ta.lng) - radians(tbu.start_lng)) + 
                                                                    sin(radians(tbu.start_lat)) * 
                                                                    sin(radians(ta.lat))
                                                                )) AS rn_start_distance	,
            rank() OVER (PARTITION BY tbu.id ORDER BY 6371000 * acos(
                                                            cos(radians(tbu.end_lat)) * 
                                                            cos(radians(ta.lat)) * 
                                                            cos(radians(ta.lng) - radians(tbu.end_lng)) + 
                                                            sin(radians(tbu.end_lat)) * 
                                                            sin(radians(ta.lat))
                                                        )) AS rn_end_distance	
        FROM damir.t_bike_use tbu
        CROSS JOIN damir.t_area ta 
        WHERE tbu.ride_status != 5
            AND ta.active = 1
            AND tbu.id > {max_id_order}
            )
    SELECT 
        NOW() + INTERVAL '3 hours' AS add_time ,
        COALESCE(t_starts_res.city_id, t_ends_res.city_id) AS city_id ,
        COALESCE(t_starts_res.area , t_ends_res.area) AS parking_name ,
        COALESCE(t_starts_res.area_id , t_ends_res.area_id) AS parking_id ,
        COALESCE(t_ends_res.count_end_under_15m, 0) AS count_end_under_15m ,
        COALESCE(t_ends_res.count_end_under_15m_50m, 0) AS count_end_under_15m_50m ,
        COALESCE(t_ends_res.count_end_more_50m, 0) AS count_end_more_50m ,
        COALESCE(t_starts_res.count_start_under_15m, 0) AS count_start_under_15m ,
        COALESCE(t_starts_res.count_start_under_15m_50m, 0) AS count_start_under_15m_50m ,
        COALESCE(t_starts_res.count_start_more_50m , 0) AS count_start_more_50m ,
        COALESCE(t_instant.instant_scooter_count_under_15m, 0) AS instant_scooter_count_under_15m,
        COALESCE(t_instant.instant_scooter_count_under_15m_50m, 0) AS instant_scooter_count_under_15m_50m,
        COALESCE(t_instant.instant_scooter_count_end_more_50m, 0) AS instant_scooter_count_end_more_50m
    FROM
        (
            SELECT
                t_res.city_id ,
                t_res.area ,
                t_res.area_id ,
                COUNT(t_res.id) FILTER (WHERE t_res.start_distance <= 15) AS count_start_under_15m ,
                COUNT(t_res.id) FILTER (WHERE (t_res.start_distance > 15 AND t_res.start_distance <= 50)) AS count_start_under_15m_50m ,
                COUNT(t_res.id) FILTER (WHERE t_res.start_distance > 50) AS count_start_more_50m
            FROM t_res
            WHERE t_res.rn_start_distance = 1
            GROUP BY t_res.city_id , t_res.area , t_res.area_id
        ) AS t_starts_res
    FULL JOIN 
        (
            SELECT
                t_res.city_id ,
                t_res.area ,
                t_res.area_id ,
                COUNT(t_res.id) FILTER (WHERE t_res.end_distance <= 15) AS count_end_under_15m ,
                COUNT(t_res.id) FILTER (WHERE (t_res.end_distance > 15 AND t_res.end_distance <= 50)) AS count_end_under_15m_50m ,
                COUNT(t_res.id) FILTER (WHERE t_res.end_distance > 50) AS count_end_more_50m
            FROM t_res
            WHERE t_res.rn_end_distance = 1
            GROUP BY t_res.city_id , t_res.area , t_res.area_id
        ) AS t_ends_res ON t_starts_res.city_id = t_ends_res.city_id AND t_starts_res."area" = t_ends_res."area" AND t_starts_res.area_id = t_ends_res.area_id
    LEFT JOIN 
        (
        SELECT 
            t.city_id ,
            t.area AS parking_name ,
            t.area_id AS parking_id ,
            COUNT(t.id) FILTER (WHERE t.distance <= 15) AS instant_scooter_count_under_15m ,
            COUNT(t.id) FILTER (WHERE (t.distance > 15 AND t.distance <= 50)) AS instant_scooter_count_under_15m_50m ,
            COUNT(t.id) FILTER (WHERE t.distance > 50) AS instant_scooter_count_end_more_50m
        FROM 
            (
            SELECT 
                tb.id ,
                tb."number" ,
                tb.g_lat ,
                tb.g_lng ,
                ta.lat AS  area_lat,
                ta.lng AS area_lng ,
                ta.city_id ,
                ta.name AS area,
                ta.id AS area_id ,
                    6371000 * acos(
                        cos(radians(tb.g_lat)) * 
                        cos(radians(ta.lat)) * 
                        cos(radians(ta.lng) - radians(tb.g_lng)) + 
                        sin(radians(tb.g_lat)) * 
                        sin(radians(ta.lat))) AS distance ,
                rank() OVER (PARTITION BY tb.id ORDER BY 6371000 * acos(
                                                                    cos(radians(tb.g_lat)) * 
                                                                    cos(radians(ta.lat)) * 
                                                                    cos(radians(ta.lng) - radians(tb.g_lng)) + 
                                                                    sin(radians(tb.g_lat)) * 
                                                                    sin(radians(ta.lat)))) AS rn
            FROM damir.t_bike tb
            CROSS JOIN damir.t_area ta
            WHERE tb.error_status IN (0,7)
                AND TO_TIMESTAMP(tb.g_time) > NOW() - INTERVAL '900 seconds' 
                AND TO_CHAR(TO_TIMESTAMP(tb.heart_time), 'YYYY-MM-DD') = TO_CHAR(NOW(), 'YYYY-MM-DD')
                AND tb.status != 1
                AND ta.active = 1
                ) AS t
        WHERE t.rn = 1
        GROUP BY t.city_id , t.area , t.area_id
        ) AS t_instant ON COALESCE(t_starts_res.city_id, t_ends_res.city_id) = t_instant.city_id 
                        AND COALESCE(t_starts_res.area , t_ends_res.area) = t_instant.parking_name 
                        AND COALESCE(t_starts_res.area_id , t_ends_res.area_id) = t_instant.parking_id
            '''.format(max_id_order=max_id_order)

    df_fresh_parking_stats = pd.read_sql(select_fresh_parking_stats, engine_postgresql)
    df_fresh_parking_stats.replace('', '0').to_sql("t_parking_stats", engine_postgresql, if_exists="append",
                                                   index=False)
    print('Added {x} records to t_parking_stats in Postgres!'.format(x=df_fresh_parking_stats.shape[0]))


    #
    select_fresh_parking_stats1 = '''
        WITH t_res AS 
        (
            SELECT 
                --now() AS add_time ,
                tbu.id ,
                --tbu.uid ,
                --tbu.bid ,
                --tbu.start_time ,
                --tbu.end_time ,
                tbu.start_lat ,
                tbu.start_lng ,
                tbu.end_lat ,
                tbu.end_lng ,
                ta.lat AS  area_lat,
                ta.lng AS area_lng ,
                ta.city_id ,
                --tc.id AS city_id ,
                ta.name AS area,
                ta.id AS area_id ,
                    6371000 * acos(
                    cos(radians(tbu.start_lat)) * 
                    cos(radians(ta.lat)) * 
                    cos(radians(ta.lng) - radians(tbu.start_lng)) + 
                    sin(radians(tbu.start_lat)) * 
                    sin(radians(ta.lat))
                ) AS start_distance ,
                    6371000 * acos(
                    cos(radians(tbu.end_lat)) * 
                    cos(radians(ta.lat)) * 
                    cos(radians(ta.lng) - radians(tbu.end_lng)) + 
                    sin(radians(tbu.end_lat)) * 
                    sin(radians(ta.lat))
                ) AS end_distance ,
                rank() OVER (PARTITION BY tbu.id ORDER BY 6371000 * acos(
                                                                        cos(radians(tbu.start_lat)) * 
                                                                        cos(radians(ta.lat)) * 
                                                                        cos(radians(ta.lng) - radians(tbu.start_lng)) + 
                                                                        sin(radians(tbu.start_lat)) * 
                                                                        sin(radians(ta.lat))
                                                                    )) AS rn_start_distance	,
                rank() OVER (PARTITION BY tbu.id ORDER BY 6371000 * acos(
                                                                cos(radians(tbu.end_lat)) * 
                                                                cos(radians(ta.lat)) * 
                                                                cos(radians(ta.lng) - radians(tbu.end_lng)) + 
                                                                sin(radians(tbu.end_lat)) * 
                                                                sin(radians(ta.lat))
                                                            )) AS rn_end_distance	
            FROM damir.t_bike_use tbu
            CROSS JOIN damir.t_area ta 
            WHERE tbu.ride_status != 5
                AND ta.active = 1
                AND tbu.id > {max_id_order}
                )
        SELECT 
            NOW() + INTERVAL '3 hours' AS add_time ,
            COALESCE(t_starts_res.city_id, t_ends_res.city_id) AS city_id ,
            tap.area_id ,
            tap.area_name ,
            --p.parking_id AS area_id ,
            --p.area AS area_name ,
            COALESCE(t_starts_res.area , t_ends_res.area) AS parking_name ,
            COALESCE(t_starts_res.area_id , t_ends_res.area_id) AS parking_id ,
            COALESCE(t_ends_res.count_end_under_15m, 0) AS count_end_under_15m ,
            COALESCE(t_ends_res.count_end_under_15m_50m, 0) AS count_end_under_15m_50m ,
            COALESCE(t_ends_res.count_end_more_50m, 0) AS count_end_more_50m ,
            COALESCE(t_starts_res.count_start_under_15m, 0) AS count_start_under_15m ,
            COALESCE(t_starts_res.count_start_under_15m_50m, 0) AS count_start_under_15m_50m ,
            COALESCE(t_starts_res.count_start_more_50m , 0) AS count_start_more_50m ,
            COALESCE(t_instant.instant_scooter_count_under_15m, 0) AS instant_scooter_count_under_15m,
            COALESCE(t_instant.instant_scooter_count_under_15m_50m, 0) AS instant_scooter_count_under_15m_50m,
            COALESCE(t_instant.instant_scooter_count_end_more_50m, 0) AS instant_scooter_count_end_more_50m
        FROM
            (
                SELECT
                    t_res.city_id ,
                    t_res.area ,
                    t_res.area_id ,
                    COUNT(t_res.id) FILTER (WHERE t_res.start_distance <= 15) AS count_start_under_15m ,
                    COUNT(t_res.id) FILTER (WHERE (t_res.start_distance > 15 AND t_res.start_distance <= 50)) AS count_start_under_15m_50m ,
                    COUNT(t_res.id) FILTER (WHERE t_res.start_distance > 50) AS count_start_more_50m
                FROM t_res
                WHERE t_res.rn_start_distance = 1
                GROUP BY t_res.city_id , t_res.area , t_res.area_id
            ) AS t_starts_res
        FULL JOIN 
            (
                SELECT
                    t_res.city_id ,
                    t_res.area ,
                    t_res.area_id ,
                    COUNT(t_res.id) FILTER (WHERE t_res.end_distance <= 15) AS count_end_under_15m ,
                    COUNT(t_res.id) FILTER (WHERE (t_res.end_distance > 15 AND t_res.end_distance <= 50)) AS count_end_under_15m_50m ,
                    COUNT(t_res.id) FILTER (WHERE t_res.end_distance > 50) AS count_end_more_50m
                FROM t_res
                WHERE t_res.rn_end_distance = 1
                GROUP BY t_res.city_id , t_res.area , t_res.area_id
            ) AS t_ends_res ON t_starts_res.city_id = t_ends_res.city_id AND t_starts_res."area" = t_ends_res."area" AND t_starts_res.area_id = t_ends_res.area_id
        LEFT JOIN 
            (
            SELECT 
                t.city_id ,
                t.area AS parking_name ,
                t.area_id AS parking_id ,
                COUNT(t.id) FILTER (WHERE t.distance <= 15) AS instant_scooter_count_under_15m ,
                COUNT(t.id) FILTER (WHERE (t.distance > 15 AND t.distance <= 50)) AS instant_scooter_count_under_15m_50m ,
                COUNT(t.id) FILTER (WHERE t.distance > 50) AS instant_scooter_count_end_more_50m
            FROM 
                (
                SELECT 
                    tb.id ,
                    tb."number" ,
                    tb.g_lat ,
                    tb.g_lng ,
                    ta.lat AS  area_lat,
                    ta.lng AS area_lng ,
                    ta.city_id ,
                    ta.name AS area,
                    ta.id AS area_id ,
                        6371000 * acos(
                            cos(radians(tb.g_lat)) * 
                            cos(radians(ta.lat)) * 
                            cos(radians(ta.lng) - radians(tb.g_lng)) + 
                            sin(radians(tb.g_lat)) * 
                            sin(radians(ta.lat))) AS distance ,
                    rank() OVER (PARTITION BY tb.id ORDER BY 6371000 * acos(
                                                                        cos(radians(tb.g_lat)) * 
                                                                        cos(radians(ta.lat)) * 
                                                                        cos(radians(ta.lng) - radians(tb.g_lng)) + 
                                                                        sin(radians(tb.g_lat)) * 
                                                                        sin(radians(ta.lat)))) AS rn
                FROM damir.t_bike tb
                CROSS JOIN damir.t_area ta
                WHERE tb.error_status IN (0,7)
                    AND TO_TIMESTAMP(tb.g_time) > NOW() - INTERVAL '900 seconds' 
                    AND TO_CHAR(TO_TIMESTAMP(tb.heart_time), 'YYYY-MM-DD') = TO_CHAR(NOW(), 'YYYY-MM-DD')
                    AND tb.status != 1
                    AND ta.active = 1
                    ) AS t
            WHERE t.rn = 1
            GROUP BY t.city_id , t.area , t.area_id
            ) AS t_instant ON COALESCE(t_starts_res.city_id, t_ends_res.city_id) = t_instant.city_id 
                            AND COALESCE(t_starts_res.area , t_ends_res.area) = t_instant.parking_name 
                            AND COALESCE(t_starts_res.area_id , t_ends_res.area_id) = t_instant.parking_id
        LEFT JOIN damir.t_areas_parkings tap ON COALESCE(t_starts_res.area_id , t_ends_res.area_id) = tap.parking_id
        --LEFT JOIN public.parking p ON COALESCE(t_starts_res.area_id , t_ends_res.area_id) = p.parking_id
                '''.format(max_id_order=max_id_order)

    df_fresh_parking_stats1 = pd.read_sql(select_fresh_parking_stats1, engine_postgresql)
    df_fresh_parking_stats1.replace('', '0').to_sql("t_parking_stats1", engine_postgresql, if_exists="append",
                                                   index=False)
    print('Added {x} records to t_parking_stats1 in Postgres!'.format(x=df_fresh_parking_stats1.shape[0]))

    #

    # Загрузка новых заказов в t_orders
    select_fresh_orders = '''
        SELECT 
            now() + INTERVAL '3 hours' AS add_time ,
            tbu.id ,
            tbu.uid ,
            tbu.bid ,
            tbu.start_time ,
            tbu.end_time ,
            tbu.start_lat ,
            tbu.start_lng ,
            tbu.end_lat ,
            tbu.end_lng 
        FROM damir.t_bike_use tbu 
        WHERE tbu.ride_status != 5
            AND tbu.id > {max_id_order}
            '''.format(max_id_order=max_id_order)

    df_fresh_orders = pd.read_sql(select_fresh_orders, engine_postgresql)

    df_fresh_orders.replace('', '0').to_sql("t_orders", engine_postgresql, if_exists="append", index=False)
    print('Added {x} records to t_orders in Postgres!'.format(x=df_fresh_orders.shape[0]))
    # Обновление t_parking_stats в Postgresql. Конец

    # Обновление t_daily_report в Postgresql. Начало

    select_df_t_daily_report = '''
    WITH vni AS (
        SELECT 
            vc.id ,
            vc.kvt ,
            vc.poezdok ,
            vc.srednyee_vremya_poezdki ,
            vc.vyruchka ,
            vc.vyruchka_bez_bonusov ,
            vc.vyruchka_s_abonementov ,
            vc.skidka ,
            vc.obcshee_vrmeya_min
            --vc.srednyaa_cena_poezdki 
        FROM 
            (
            SELECT
                vc."timestamp" ,
                vc.id ,
                vc.name ,
                vc.kvt ,
                vc.poezdok ,
                --FLOAT(vc.srednyee_vremya_poezdki,2) ,
                vc.srednyee_vremya_poezdki ,
                vc.vyruchka AS vyruchka,
                vc.vyruchka_bez_bonusov AS vyruchka_bez_bonusov,
                vc.vyruchka_s_abonementov AS vyruchka_s_abonementov,
                vc.skidka AS skidka,
                (vc.vyruchka_bez_bonusov + vc.vyruchka_s_abonementov - vc.skidka ) / vc.poezdok AS srednyaa_cena_poezdki , 
                vc.obcshee_vrmeya_min AS obcshee_vrmeya_min ,
                RANK() OVER (PARTITION BY vc.id ORDER BY vc."timestamp" DESC) AS rn
            FROM damir.vni_cities vc
            ) AS vc
        WHERE vc.rn = 1 AND vc."timestamp"::date = NOW()::date 
    ),
    avg_kvt_yesterday AS(
        SELECT
            fpr.city_id ,
            ROUND(AVG(fpr.fact_park -	fpr.remont - tbh.power_less_30), 2) AS avg_kvt_yesterday
        FROM
        (
            SELECT 
                res_tab."timestamp" ,
                res_tab."timestamp_hour" ,
                res_tab.city_id ,
                res_tab.fact_park ,
                res_tab.remont ,
                RANK() OVER (PARTITION BY res_tab."timestamp_hour" ORDER BY res_tab."timestamp" DESC) AS rn
            FROM
            (
                SELECT
                    acasr."timestamp" ,
                    date_trunc('hour', acasr."timestamp") + CASE
                                WHEN EXTRACT(minute FROM acasr."timestamp") > 0 THEN INTERVAL '1 hour'
                                ELSE INTERVAL '0 hour' 
                            END AS "timestamp_hour",
                    acasr.city_id ,
                    acasr.fact_park ,
                    acasr.remont
                FROM damir.akb_cities_and_stocks_result acasr
                WHERE acasr."timestamp"::date = NOW()::date - INTERVAL '1 days' 
                        AND acasr.city_id NOT IN (0,47,100,1000,12,11)
            ) AS res_tab
        ) AS fpr
        INNER JOIN (
            SELECT 
                tbh."timestamp_hour" ,
                tbh.city_id ,
                COUNT(tbh.id) AS power_less_30
            FROM
            (
                SELECT 
                    res_tab."timestamp" ,
                    res_tab."timestamp_hour" ,
                    res_tab.city_id ,
                    res_tab.id ,
                    RANK() OVER (PARTITION BY res_tab."timestamp_hour" ORDER BY res_tab."timestamp" DESC) AS rn
                FROM 
                    (
                    SELECT
                        tbh."timestamp",
                        date_trunc('hour', tbh."timestamp") + CASE
                            WHEN EXTRACT(minute FROM tbh."timestamp") > 0 THEN INTERVAL '1 hour'
                            ELSE INTERVAL '0 hour' 
                        END AS "timestamp_hour",
                        tbh.city_id ,
                        tbh.id 
                    FROM damir.t_bike_history tbh 
                    WHERE tbh."timestamp"::date = NOW()::date - INTERVAL '1 days'
                        AND tbh.power < 30
                    ) AS res_tab
            ) AS tbh
            WHERE tbh.rn = 1
            GROUP BY tbh."timestamp_hour" , tbh.city_id
        ) AS tbh ON fpr."timestamp_hour" = tbh."timestamp_hour" AND fpr.city_id = tbh.city_id
        WHERE fpr.rn = 1
        GROUP BY fpr.city_id
    ),
    avg_kvt_yesterday_1 AS (
        SELECT
            vc.id ,
            AVG(vc.kvt)::float AS avg_kvt_yesterday 
        FROM damir.vni_cities vc 
        WHERE vc."timestamp"::date = NOW()::date - INTERVAL '1 days'
        GROUP BY vc.id
    ),
    scooters_7_17 AS (
        SELECT 
            tbh.city_id ,
            count(tbh.id) FILTER (WHERE tbh."timestamp" = (CURRENT_DATE + INTERVAL '8 hours')::timestamp) AS scooters_charge_less_30_not_rent_at_7_00 ,
            count(tbh.id) FILTER (WHERE tbh."timestamp" = (CURRENT_DATE + INTERVAL '18 hours')::timestamp) AS scooters_charge_less_30_not_rent_at_17_00
        FROM 
        (
            SELECT 
                date_trunc('hour', tbh."timestamp")  + 
                                               CASE 
                                                 WHEN EXTRACT(minute FROM tbh."timestamp") > 0 THEN INTERVAL '1 hour'
                                                 ELSE INTERVAL '0 hour'
                                               END AS "timestamp" ,
                tbh.id ,
                tbh.city_id ,
                RANK() OVER (PARTITION BY date_trunc('hour', tbh."timestamp")  + 
                                               CASE 
                                                 WHEN EXTRACT(minute FROM tbh."timestamp") > 0 THEN INTERVAL '1 hour'
                                                 ELSE INTERVAL '0 hour'
                                               END ORDER BY tbh."timestamp" DESC) AS rn
            FROM damir.t_bike_history tbh
            WHERE tbh.timestamp >= current_date
                AND
                tbh.error_status NOT IN (0, 7) 
                AND 
                tbh.power < 30
            ) AS tbh
        WHERE tbh.rn = 1
        GROUP BY tbh.city_id
    ),
    scooters_without_rides_yesterday AS (
        SELECT
            tb.city_id ,
            count(tb.id) AS scooters_without_rides_yesterday
        FROM damir.t_bike tb
        WHERE tb.id NOT IN (SELECT 
                                tbu.bid 
                            FROM damir.t_bike_use tbu 
                            WHERE tbu.ride_status!=5 AND to_timestamp(tbu."date") >= NOW() - INTERVAL '1 days')
                AND 
                tb.error_status IN (0, 7)
        GROUP BY tb.city_id
    ),
    scooters_without_rides_more_2_days AS (
        SELECT
            tb.city_id ,
            count(tb.id) AS scooters_without_rides_more_2_days
        FROM damir.t_bike tb
        WHERE tb.id NOT IN (SELECT 
                                tbu.bid 
                            FROM damir.t_bike_use tbu 
                            WHERE tbu.ride_status!=5 AND (to_timestamp(tbu."date") >= NOW() - INTERVAL '2 days')
                AND 
                tb.error_status IN (0, 7)
                            )
        GROUP BY tb.city_id
    ),
    svobodnyh_akb AS (
        SELECT
            res.city_id ,
            res.svobodnyh_akb ,
            res.fact_park,
            res.mr_admin
        FROM 
        (
            SELECT
                acasr."timestamp"::date ,
                acasr."timestamp" ,
                acasr.city_id ,
                acasr."name" ,
                acasr.svobodnyh_akb,
                acasr.fact_park,
                acasr.mr_admin,
                RANK() OVER (PARTITION BY acasr.city_id ORDER BY acasr."timestamp" DESC) AS rn
            FROM damir.akb_cities_and_stocks_result acasr
            WHERE acasr."timestamp" >= current_date 
                AND acasr.city_id NOT IN (0,47,100,1000,12,11)
            ) AS res
        WHERE res.rn = 1
    ),
    poezdki_promo AS (
        SELECT
            tb.city_id ,
            count(tbu.bid) AS poezdki_promo
        FROM damir.t_bike_use tbu 
        LEFT JOIN damir.t_payment_details tpd ON tbu.id = tpd.id
        LEFT JOIN damir.t_bike tb ON tbu.bid = tb.id 
        WHERE tpd.created >= current_date
            AND tpd.bike_discount_amount > 0
        GROUP BY tb.city_id
    ),
    uteryany AS (
        SELECT 
            tb.city_id ,
            COUNT(tb.id) FILTER (WHERE tb.error_status = 6) AS uteryany,
            COUNT(tb.id) FILTER (WHERE tb.error_status = 1) AS mr_admin,
            COUNT(tb.id) FILTER (WHERE (tb.error_status IN (0, 7) AND tb.power > 30 AND EXTRACT(EPOCH FROM (NOW() - TO_TIMESTAMP(tb.g_time))) < 900)) AS for_rent_power_more_30 
        FROM damir.t_bike tb 
        GROUP BY tb.city_id
    ),
    scooters_without_rides_over_1_month AS (
        SELECT
            tb.city_id ,
            count(tb.id) AS scooters_without_rides_over_1_month
        FROM damir.t_bike tb
        WHERE tb.id NOT IN (SELECT 
                                tbu.bid 
                            FROM damir.t_bike_use tbu 
                            WHERE tbu.ride_status!=5 AND to_timestamp(tbu."date") >= current_date - INTERVAL '1 month')
            AND tb.error_status NOT IN (0, 7)
        GROUP BY tb.city_id
    )
    SELECT 
        NOW() + INTERVAL '3 hours' AS add_time ,
        tc.id AS city_id,
        COALESCE(vni.kvt,0) AS kvt,
        COALESCE(svobodnyh_akb.fact_park,0) AS fact_park,
        COALESCE(vni.poezdok,0) AS poezdok,
        COALESCE(vni.srednyee_vremya_poezdki,0) AS srednyee_vremya_poezdki,
        COALESCE(vni.vyruchka,0) AS vyruchka,
        COALESCE(vni.vyruchka_bez_bonusov,0) AS vyruchka_bez_bonusov,
        COALESCE(vni.vyruchka_s_abonementov,0) AS vyruchka_s_abonementov,
        COALESCE(vni.skidka,0) AS skidka,
        COALESCE(avg_kvt_yesterday.avg_kvt_yesterday,0) AS avg_kvt_yesterday,
        COALESCE(scooters_7_17.scooters_charge_less_30_not_rent_at_7_00,0) AS scooters_charge_less_30_not_rent_at_7_00,
        COALESCE(scooters_7_17.scooters_charge_less_30_not_rent_at_17_00,0) AS scooters_charge_less_30_not_rent_at_17_00,
        COALESCE(scooters_without_rides_yesterday.scooters_without_rides_yesterday,0) AS scooters_without_rides_yesterday,
        COALESCE(scooters_without_rides_more_2_days.scooters_without_rides_more_2_days,0) AS scooters_without_rides_more_2_days,
        COALESCE(svobodnyh_akb.svobodnyh_akb,0) AS svobodnyh_akb,
        COALESCE(poezdki_promo.poezdki_promo,0) AS poezdki_promo,
        COALESCE(uteryany.uteryany,0) AS uteryany,
        COALESCE(svobodnyh_akb.mr_admin,0) AS mr_admin,
        COALESCE(scooters_without_rides_over_1_month.scooters_without_rides_over_1_month,0) AS scooters_without_rides_over_1_month,
        COALESCE(tkp.kvt_plan,0) AS kvt_plan,
        COALESCE(vni.obcshee_vrmeya_min,0) AS obcshee_vrmeya_min,
		COALESCE(uteryany.for_rent_power_more_30,0) AS for_rent_power_more_30
    FROM damir.t_city tc
    LEFT JOIN vni ON tc.id = vni.id
    LEFT JOIN avg_kvt_yesterday ON tc.id = avg_kvt_yesterday.city_id
    LEFT JOIN scooters_7_17 ON tc.id = scooters_7_17.city_id
    LEFT JOIN scooters_without_rides_yesterday ON tc.id = scooters_without_rides_yesterday.city_id
    LEFT JOIN scooters_without_rides_more_2_days ON tc.id = scooters_without_rides_more_2_days.city_id
    LEFT JOIN svobodnyh_akb ON tc.id = svobodnyh_akb.city_id
    LEFT JOIN poezdki_promo ON tc.id = poezdki_promo.city_id
    LEFT JOIN uteryany ON tc.id = uteryany.city_id
    LEFT JOIN scooters_without_rides_over_1_month ON tc.id = scooters_without_rides_over_1_month.city_id
    LEFT JOIN damir.t_kvt_plan tkp ON tc.id = tkp.city_id 
    '''

    df_t_daily_report = pd.read_sql(select_df_t_daily_report, engine_postgresql)
    df_t_daily_report.to_sql("t_daily_report", engine_postgresql, if_exists="append", index=False)
    print('Got it!: t_daily_report')
    # Обновление t_daily_report в Postgresql. Конец

    # Выгрузка t_daily_report_result. Начало
    # Выгрузка для t_daily_report_result
    select_t_daily_report_result = '''
        SELECT
            tdr.add_time ,
            tdr.city_id ,
            tdr.kvt ,
            tdr.poezdok ,
            tdr.srednyee_vremya_poezdki ,
            tdr.vyruchka ,
            tdr.vyruchka_bez_bonusov ,
            tdr.vyruchka_s_abonementov ,
            tdr.skidka ,
            tdr.avg_kvt_yesterday ,
            tdr.scooters_charge_less_30_not_rent_at_7_00 ,
            tdr.scooters_charge_less_30_not_rent_at_17_00 ,
            tdr.scooters_without_rides_yesterday ,
            tdr.scooters_without_rides_more_2_days ,
            tdr.svobodnyh_akb ,
            tdr.poezdki_promo ,
            tdr.uteryany ,
            tdr.scooters_without_rides_over_1_month ,
            tdr.kvt_plan ,
            tdr.obcshee_vrmeya_min ,
            tdr.mr_admin ,
            tdr.fact_park ,
            tdr.for_rent_power_more_30 
        FROM 
            (
            SELECT
                tdr.* ,
                RANK() OVER (PARTITION BY tdr.add_time::date, tdr.city_id ORDER BY tdr.add_time DESC) AS rn
            FROM damir.t_daily_report tdr 
            ) AS tdr
        WHERE tdr.rn = 1
    '''
    df_t_daily_report_result = pd.read_sql(select_t_daily_report_result, engine_postgresql)

    # Очистка таблицы
    truncate_t_daily_report_result = "TRUNCATE TABLE t_daily_report_result RESTART IDENTITY;"
    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(sa.text(truncate_t_daily_report_result))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица t_daily_report_result успешно очищена!")

    df_t_daily_report_result.to_sql("t_daily_report_result", engine_postgresql, if_exists="append", index=False)
    print('Таблица t_daily_report_result успешно обновлена!')
    # Выгрузка t_daily_report_result. Конец

    #   Выгрузка t_parking_revenue_stats. Начало
    # Удаление заказов за последние 2 часа
    truncate_orders_revenue = '''
        DELETE FROM damir.t_orders_revenue 
        WHERE t_orders_revenue."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    '''
    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(sa.text(truncate_orders_revenue))
            transaction.commit()
            print(f"Таблица t_orders_revenue успешно очищена!")

    # Загрузка новых orders_revenue
    select_new_orders_revenue = '''
        SELECT
            NOW() AS add_time ,
            from_unixtime(tbu.`date`) AS "timestamp" ,
            tbu.id ,
            tbu.start_lat ,
            tbu.start_lng ,
            IFNULL(tbu.ride_amount,0) AS ride_amount,
            IFNULL(tbu.discount,0) AS discount,
            IFNULL(tpd.bike_discount_amount,0) AS bike_discount_amount,
            IFNULL(ts.price , 0) AS subscription_price ,
			tbu.bid
        FROM shamri.t_bike_use tbu
        LEFT JOIN shamri.t_payment_details tpd ON tbu.id = tpd.ride_id
        LEFT JOIN shamri.t_subscription ts ON tbu.subscription_id = ts.id 
        WHERE tbu.ride_status != 5
             AND 
            from_unixtime(tbu.`date`) >= STR_TO_DATE(DATE_FORMAT(NOW(), '%Y-%m-%d %H:00:00'), "%Y-%m-%d %H:%i:%s") - INTERVAL 2 HOUR
    '''
    df_new_orders_revenue = pd.read_sql(select_new_orders_revenue, engine_mysql)
    df_new_orders_revenue.to_sql("t_orders_revenue", engine_postgresql, if_exists="append", index=False)

    # Удаление в parking_revenue_stats за последние 2 часа
    # truncate_parking_revenue_stats = '''
    #     DELETE FROM t_parking_revenue_stats
    #     WHERE t_parking_revenue_stats."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    # '''
    #
    # with engine_postgresql.connect() as connection:
    #     with connection.begin() as transaction:
    #         connection.execute(sa.text(truncate_parking_revenue_stats))
    #         transaction.commit()
    #         print(f"Таблица t_parking_revenue_stats успешно очищена!")

    truncate_parking_revenue_stats1 = '''
        DELETE FROM t_parking_revenue_stats1 
        WHERE t_parking_revenue_stats1."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    '''

    truncate_parking_revenue_stats2 = '''
        DELETE FROM t_parking_revenue_stats2 
        WHERE t_parking_revenue_stats2."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    '''

    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(sa.text(truncate_parking_revenue_stats1))
            connection.execute(sa.text(truncate_parking_revenue_stats2))
            transaction.commit()
            print(f"Таблица t_parking_revenue_stats1 успешно очищена!")
            print(f"Таблица t_parking_revenue_stats2 успешно очищена!")


    # Загрузка новых данных для t_parking_revenue_stats
    # select_new_parking_revenue_stats = '''
    #     SELECT
    #         NOW() AS add_time ,
    #         date_trunc('hour', res_tab.timestamp) AS timestamp,
    #         res_tab.city_id ,
    #         res_tab.parking_id ,
    #         COUNT(res_tab.ride_amount) AS poezdok,
    #         SUM(res_tab.ride_amount) AS obzchaya_stoimost,
    #         SUM(res_tab.discount) AS oplacheno_bonusami,
    #         SUM(res_tab.bike_discount_amount) AS skidka ,
    #         SUM(res_tab.subscription_price) AS abon
    #     FROM
    #         (
    #         SELECT
    #             tor."timestamp" ,
    #             tor.start_lat ,
    #             tor.start_lng ,
    #             tor.ride_amount ,
    #             tor.discount ,
    #             tor.bike_discount_amount ,
    #             tor.subscription_price ,
    #             ta.city_id ,
    #             ta.id AS parking_id ,
    #             6371000 * acos(
    #                             cos(radians(tor.start_lat)) *
    #                             cos(radians(ta.lat)) *
    #                             cos(radians(ta.lng) - radians(tor.start_lng)) +
    #                             sin(radians(tor.start_lat)) *
    #                             sin(radians(ta.lat))) AS distance ,
    #             RANK() OVER (PARTITION BY tor.id ORDER BY 6371000 * acos(
    #                                                                     cos(radians(tor.start_lat)) *
    #                                                                     cos(radians(ta.lat)) *
    #                                                                     cos(radians(ta.lng) - radians(tor.start_lng)) +
    #                                                                     sin(radians(tor.start_lat)) *
    #                                                                     sin(radians(ta.lat)))
    #                                                                     ) AS rn
    #         FROM damir.t_orders_revenue tor
    #         CROSS JOIN damir.t_area ta
    #         WHERE tor."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    #         ) AS res_tab
    #     WHERE res_tab.rn = 1
    #     GROUP BY date_trunc('hour', res_tab.timestamp), res_tab.city_id , res_tab.parking_id
    #     ORDER BY date_trunc('hour', res_tab.timestamp) ASC
    # '''

    # select_new_parking_revenue_stats = '''
    # WITH dolgi AS (
    #     SELECT
    #         dolgi.city_id ,
    #         SUM(dolgi.debit_cash) AS dolgi
    #     FROM
    #     (
    #         SELECT
    #             dolgi.city_id ,
    #             tpd.debit_cash
    #         FROM damir.t_payment_details tpd
    #         RIGHT JOIN (
    #             SELECT
    #                 tbu."date" ,
    #                 tbu.id,
    #                 tbu.uid,
    #                 tbu.bid ,
    #                 tb.city_id ,
    #                 tbu.ride_amount
    #             FROM damir.t_bike_use tbu
    #             LEFT JOIN t_bike tb ON tbu.bid = tb.id
    #             WHERE tbu.ride_status = 2
    #                 AND to_timestamp( tbu.start_time)::date >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    #             ) AS dolgi ON tpd.ride_id = dolgi.id
    #         ) AS dolgi
    #     GROUP BY dolgi.city_id
    # ),
    # vyruchka_s_abonementov AS (
    #     SELECT
    #         distr_poezdki_po_gorodam.city_id,
    #         sum_uspeh_abon.vyruchka_s_abonementov * distr_poezdki_po_gorodam.coef_goroda AS vyruchka_s_abonementov
    #     FROM (
    #         -- высчитываю пропорции по поездкам
    #         SELECT
    #             distr_poezdki_po_gorodam.start_time,
    #             distr_poezdki_po_gorodam.city_id,
    #             distr_poezdki_po_gorodam.poezdok,
    #             -- Приведение к numeric важно, чтобы результат деления не был 0
    #             distr_poezdki_po_gorodam.poezdok::numeric / SUM(distr_poezdki_po_gorodam.poezdok) OVER (PARTITION BY distr_poezdki_po_gorodam.start_time) AS coef_goroda
    #         FROM
    #             (
    #             SELECT
    #                 TO_CHAR(TO_TIMESTAMP(t_bike_use.start_time), 'YYYY-MM-DD') AS start_time,
    #                 t_bike.city_id,
    #                 COUNT(t_bike_use.ride_amount) AS poezdok
    #             FROM damir.t_bike_use
    #             LEFT JOIN damir.t_bike ON t_bike_use.bid = t_bike.id
    #             WHERE t_bike_use.ride_status != 5
    #                 AND TO_TIMESTAMP(t_bike_use.start_time) >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    #             GROUP BY 1, 2
    #             )
    #             AS distr_poezdki_po_gorodam
    #     ) AS distr_poezdki_po_gorodam
    #     LEFT JOIN (
    #         SELECT
    #             TO_CHAR(t_trade.date, 'YYYY-MM-DD') AS start_time,
    #             SUM(COALESCE(t_trade.amount, 0)) AS vyruchka_s_abonementov
    #         FROM damir.t_trade
    #         WHERE t_trade.type = 6
    #             AND t_trade.status = 1
    #             AND t_trade.date >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    #         GROUP BY 1
    #         ) AS sum_uspeh_abon
    #         ON distr_poezdki_po_gorodam.start_time = sum_uspeh_abon.start_time
    # ),
    # sum_mnogor_abon AS (
    #     SELECT
    #         distr_poezdki_po_gorodam.city_id,
    #         sum_mnogor_abon.sum_mnogor_abon * distr_poezdki_po_gorodam.coef_goroda AS sum_mnogor_abon
    #     FROM (
    #         -- высчитываю пропорции по поездкам
    #         SELECT
    #             dp.start_time,
    #             dp.city_id,
    #             dp.poezdok,
    #             dp.poezdok::numeric / NULLIF(SUM(COALESCE(dp.poezdok, 0)) OVER (PARTITION BY dp.start_time), 0) AS coef_goroda
    #         FROM (
    #             SELECT
    #                 TO_CHAR(TO_TIMESTAMP(t_bike_use.start_time), 'YYYY-MM-DD') AS start_time,
    #                 t_bike.city_id,
    #                 COUNT(t_bike_use.ride_amount) AS poezdok
    #             FROM damir.t_bike_use
    #             LEFT JOIN damir.t_bike ON t_bike_use.bid = t_bike.id
    #             WHERE t_bike_use.ride_status != 5
    #                 AND TO_TIMESTAMP(t_bike_use.start_time) >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    #             GROUP BY 1, 2
    #         ) AS dp
    #     ) AS distr_poezdki_po_gorodam
    #     LEFT JOIN (
    #         SELECT
    #             TO_CHAR(t_subscription_mapping.start_time, 'YYYY-MM-DD') AS start_time,
    #             SUM(COALESCE(t_subscription.price, 0)) AS sum_mnogor_abon
    #         FROM damir.t_subscription_mapping
    #         LEFT JOIN damir.t_subscription ON t_subscription_mapping.subscription_id = t_subscription.id
    #         WHERE t_subscription_mapping.start_time >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    #         GROUP BY 1
    #     ) AS sum_mnogor_abon
    #     ON distr_poezdki_po_gorodam.start_time = sum_mnogor_abon.start_time
    # )
    # SELECT
    #     NOW() AS add_time ,
    #     date_trunc('hour', res.timestamp) AS timestamp,
    #     res.city_id ,
    #     res.parking_id ,
    #     res.poezdok ,
    #     res.obzchaya_stoimost ,
    #     res.oplacheno_bonusami ,
    #     res.skidka ,
    #     res.abon ,
    #     ROUND((res.poezdok / SUM(res.poezdok) OVER (PARTITION BY res.city_id)) * COALESCE(dolgi.dolgi, 0), 2)::float AS dolgi ,
    #     ROUND(((res.poezdok / SUM(res.poezdok) OVER (PARTITION BY res.city_id)) * COALESCE(vyruchka_s_abonementov.vyruchka_s_abonementov, 0))::numeric, 2)::float AS vyruchka_s_abonementov ,
    #     ROUND((res.poezdok / SUM(res.poezdok) OVER (PARTITION BY res.city_id)) * COALESCE(sum_mnogor_abon.sum_mnogor_abon, 0), 2)::float AS sum_mnogor_abon
    # FROM
    # (
    #     SELECT
    #         date_trunc('hour', res_tab.timestamp) AS timestamp,
    #         res_tab.city_id ,
    #         res_tab.parking_id ,
    #         COUNT(res_tab.ride_amount) AS poezdok ,
    #         SUM(res_tab.ride_amount) AS obzchaya_stoimost,
    #         SUM(res_tab.discount) AS oplacheno_bonusami,
    #         SUM(res_tab.bike_discount_amount) AS skidka ,
    #         SUM(res_tab.subscription_price) AS abon
    #     FROM
    #         (
    #         SELECT
    #             tor."timestamp" ,
    #             tor.start_lat ,
    #             tor.start_lng ,
    #             tor.ride_amount ,
    #             tor.discount ,
    #             tor.bike_discount_amount ,
    #             tor.subscription_price ,
    #             ta.city_id ,
    #             ta.id AS parking_id ,
    #             6371000 * acos(
    #                             cos(radians(tor.start_lat)) *
    #                             cos(radians(ta.lat)) *
    #                             cos(radians(ta.lng) - radians(tor.start_lng)) +
    #                             sin(radians(tor.start_lat)) *
    #                             sin(radians(ta.lat))) AS distance ,
    #             RANK() OVER (PARTITION BY tor.id ORDER BY 6371000 * acos(
    #                                                                     cos(radians(tor.start_lat)) *
    #                                                                     cos(radians(ta.lat)) *
    #                                                                     cos(radians(ta.lng) - radians(tor.start_lng)) +
    #                                                                     sin(radians(tor.start_lat)) *
    #                                                                     sin(radians(ta.lat)))
    #                                                                     ) AS rn
    #         FROM damir.t_orders_revenue tor
    #         CROSS JOIN damir.t_area ta
    #         WHERE tor."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
    #         ) AS res_tab
    #     WHERE res_tab.rn = 1
    #     GROUP BY date_trunc('hour', res_tab.timestamp), res_tab.city_id , res_tab.parking_id
    # ) AS res
    # LEFT JOIN dolgi ON res.city_id = dolgi.city_id
    # LEFT JOIN vyruchka_s_abonementov ON res.city_id = vyruchka_s_abonementov.city_id
    # LEFT JOIN sum_mnogor_abon ON res.city_id = sum_mnogor_abon.city_id
    # '''
    #
    # df_new_parking_revenue_stats = pd.read_sql(select_new_parking_revenue_stats, engine_postgresql)
    #
    # df_new_parking_revenue_stats.to_sql("t_parking_revenue_stats", engine_postgresql, if_exists="append",
    #                                     index=False)

    select_new_parking_revenue_stats1 = '''
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
        NOW() AS add_time ,
        date_trunc('hour', res.timestamp) AS timestamp,
        res.city_id ,
        --tc.name AS city_name ,
        res.parking_id ,
        res.poezdok ,
        res.obzchaya_stoimost ,
        res.oplacheno_bonusami ,
        res.skidka ,
        res.abon ,
        ROUND((res.poezdok / SUM(res.poezdok) OVER (PARTITION BY res.city_id)) * COALESCE(dolgi.dolgi, 0), 2)::float AS dolgi ,
        ROUND(((res.poezdok / SUM(res.poezdok) OVER (PARTITION BY res.city_id)) * COALESCE(vyruchka_s_abonementov.vyruchka_s_abonementov, 0))::numeric, 2)::float AS vyruchka_s_abonementov ,
        ROUND((res.poezdok / SUM(res.poezdok) OVER (PARTITION BY res.city_id)) * COALESCE(sum_mnogor_abon.sum_mnogor_abon, 0), 2)::float AS sum_mnogor_abon
    FROM
    (
        SELECT
            date_trunc('hour', res_tab.timestamp) AS timestamp,
            res_tab.city_id ,
            res_tab.parking_id ,
            COUNT(res_tab.ride_amount) AS poezdok ,
            SUM(res_tab.ride_amount) AS obzchaya_stoimost,
            SUM(res_tab.discount) AS oplacheno_bonusami,
            SUM(res_tab.bike_discount_amount) AS skidka ,
            SUM(res_tab.subscription_price) AS abon
        FROM
            (
            SELECT
                tor."timestamp" ,
                tor.start_lat ,
                tor.start_lng ,
                tor.ride_amount ,
                tor.discount ,
                tor.bike_discount_amount ,
                tor.subscription_price ,
                ta.city_id ,
                ta.id AS parking_id ,
                6371000 * acos(
                                cos(radians(tor.start_lat)) * 
                                cos(radians(ta.lat)) * 
                                cos(radians(ta.lng) - radians(tor.start_lng)) + 
                                sin(radians(tor.start_lat)) * 
                                sin(radians(ta.lat))) AS distance ,
                RANK() OVER (PARTITION BY tor.id ORDER BY 6371000 * acos(
                                                                        cos(radians(tor.start_lat)) * 
                                                                        cos(radians(ta.lat)) * 
                                                                        cos(radians(ta.lng) - radians(tor.start_lng)) + 
                                                                        sin(radians(tor.start_lat)) * 
                                                                        sin(radians(ta.lat)))
                                                                        ) AS rn
            FROM damir.t_orders_revenue tor
            CROSS JOIN damir.t_area ta 
            WHERE 
                tor."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                AND ta.active = 1
                --tor."timestamp" >= '2025-12-18'::date
            ) AS res_tab
        WHERE res_tab.rn = 1
        GROUP BY date_trunc('hour', res_tab.timestamp), res_tab.city_id , res_tab.parking_id
        ) AS res
        LEFT JOIN dolgi ON res.city_id = dolgi.city_id
        LEFT JOIN vyruchka_s_abonementov ON res.city_id = vyruchka_s_abonementov.city_id
        LEFT JOIN sum_mnogor_abon ON res.city_id = sum_mnogor_abon.city_id
        LEFT JOIN damir.t_city tc ON res.city_id = tc.id
    ORDER BY date_trunc('hour', res.timestamp) ASC
    '''

    select_new_parking_revenue_stats2 = '''
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
        NOW() AS add_time ,
        date_trunc('hour', res.timestamp) AS timestamp,
        res.city_id ,
        tap.area_id ,
        tap.area_name ,
        --tc.name AS city_name ,
        res.parking_id ,
        res.parking_name ,
        res.poezdok ,
        res.obzchaya_stoimost ,
        res.oplacheno_bonusami ,
        res.skidka ,
        res.abon ,
        ROUND((res.poezdok / SUM(res.poezdok) OVER (PARTITION BY res.city_id)) * COALESCE(dolgi.dolgi, 0), 2)::float AS dolgi ,
        ROUND(((res.poezdok / SUM(res.poezdok) OVER (PARTITION BY res.city_id)) * COALESCE(vyruchka_s_abonementov.vyruchka_s_abonementov, 0))::numeric, 2)::float AS vyruchka_s_abonementov ,
        ROUND((res.poezdok / SUM(res.poezdok) OVER (PARTITION BY res.city_id)) * COALESCE(sum_mnogor_abon.sum_mnogor_abon, 0), 2)::float AS sum_mnogor_abon
    FROM
    (
        SELECT
            date_trunc('hour', res_tab.timestamp) AS timestamp,
            res_tab.city_id ,
            res_tab.parking_id ,
            res_tab.parking_name ,
            COUNT(res_tab.ride_amount) AS poezdok ,
            SUM(res_tab.ride_amount) AS obzchaya_stoimost,
            SUM(res_tab.discount) AS oplacheno_bonusami,
            SUM(res_tab.bike_discount_amount) AS skidka ,
            SUM(res_tab.subscription_price) AS abon
        FROM
            (
            SELECT
                tor."timestamp" ,
                tor.start_lat ,
                tor.start_lng ,
                tor.ride_amount ,
                tor.discount ,
                tor.bike_discount_amount ,
                tor.subscription_price ,
                ta.city_id ,
                ta.id AS parking_id ,
                ta.name AS parking_name ,
                6371000 * acos(
                                cos(radians(tor.start_lat)) * 
                                cos(radians(ta.lat)) * 
                                cos(radians(ta.lng) - radians(tor.start_lng)) + 
                                sin(radians(tor.start_lat)) * 
                                sin(radians(ta.lat))) AS distance ,
                RANK() OVER (PARTITION BY tor.id ORDER BY 6371000 * acos(
                                                                        cos(radians(tor.start_lat)) * 
                                                                        cos(radians(ta.lat)) * 
                                                                        cos(radians(ta.lng) - radians(tor.start_lng)) + 
                                                                        sin(radians(tor.start_lat)) * 
                                                                        sin(radians(ta.lat)))
                                                                        ) AS rn
            FROM damir.t_orders_revenue tor
            CROSS JOIN damir.t_area ta 
            WHERE 
                tor."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                AND ta.active = 1
                --tor."timestamp" >= '2025-12-18'::date
            ) AS res_tab
        WHERE res_tab.rn = 1
        GROUP BY date_trunc('hour', res_tab.timestamp), res_tab.city_id , res_tab.parking_id, res_tab.parking_name
        ) AS res
        LEFT JOIN dolgi ON res.city_id = dolgi.city_id
        LEFT JOIN vyruchka_s_abonementov ON res.city_id = vyruchka_s_abonementov.city_id
        LEFT JOIN sum_mnogor_abon ON res.city_id = sum_mnogor_abon.city_id
        LEFT JOIN damir.t_city tc ON res.city_id = tc.id
        LEFT JOIN damir.t_areas_parkings tap ON res.parking_id = tap.parking_id
    ORDER BY date_trunc('hour', res.timestamp) ASC
    '''
    df_new_parking_revenue_stats1 = pd.read_sql(select_new_parking_revenue_stats1, engine_postgresql)
    df_new_parking_revenue_stats2 = pd.read_sql(select_new_parking_revenue_stats2, engine_postgresql)

    df_new_parking_revenue_stats1.to_sql("t_parking_revenue_stats1", engine_postgresql, if_exists="append",
                                        index=False)
    df_new_parking_revenue_stats2.to_sql("t_parking_revenue_stats2", engine_postgresql, if_exists="append",
                                         index=False)


    #   Выгрузка t_parking_revenue_stats. Конец


    # Выгрузка t_parking_kvt. Начало

    # Выгрузка t_parking_kvt
    # select_t_parking_kvt = '''
    #        SELECT
    #             NOW() + INTERVAL '2 hour' AS add_time ,
    #             res.timestamp - INTERVAL '1 hour' AS timestamp ,
    #             res.city_id ,
    #             res.parking_id ,
    #             COUNT(res.id) AS kvt
    #         FROM
    #             (
    #             SELECT
    #                 res_tab.timestamp_hour AS "timestamp" ,
    #                 ta.city_id ,
    #                 ta.id AS parking_id ,
    #                 res_tab.id ,
    #                 6371000 * acos(
    #                                     cos(radians(res_tab.g_lat)) *
    #                                     cos(radians(ta.lat)) *
    #                                     cos(radians(ta.lng) - radians(res_tab.g_lng)) +
    #                                     sin(radians(res_tab.g_lat)) *
    #                                     sin(radians(ta.lat))) AS distance ,
    #                 RANK() OVER (PARTITION BY res_tab.timestamp_hour, res_tab.id ORDER BY 6371000 * acos(
    #                                                                                             cos(radians(res_tab.g_lat)) *
    #                                                                                             cos(radians(ta.lat)) *
    #                                                                                             cos(radians(ta.lng) - radians(res_tab.g_lng)) +
    #                                                                                             sin(radians(res_tab.g_lat)) *
    #                                                                                             sin(radians(ta.lat))) ASC) AS rn
    #             FROM
    #                 (
    #                 SELECT
    #                     date_trunc('hour', tbh."timestamp")  +
    #                                                            CASE
    #                                                              WHEN EXTRACT(minute FROM tbh."timestamp") > 0 THEN INTERVAL '1 hour'
    #                                                              ELSE INTERVAL '0 hour'
    #                                                            END AS "timestamp_hour" ,
    #                     tbh.id ,
    #                     tbh.g_lat ,
    #                     tbh.g_lng ,
    #                     RANK() OVER (PARTITION BY date_trunc('hour', tbh."timestamp")  +
    #                                                            CASE
    #                                                              WHEN EXTRACT(minute FROM tbh."timestamp") > 0 THEN INTERVAL '1 hour'
    #                                                              ELSE INTERVAL '0 hour'
    #                                                            END, tbh.id ORDER BY tbh."timestamp" DESC) AS rn
    #                 FROM damir.t_bike_history tbh
    #                 WHERE tbh.error_status IN (0,7)
    #                     AND tbh."timestamp" >= DATE(NOW() + INTERVAL '2 hours') - INTERVAL '1 hours'
    #                     AND tbh."timestamp" <= NOW() + INTERVAL '2 hour'
    #                     ) AS res_tab
    #             CROSS JOIN damir.t_area ta
    #             WHERE res_tab.rn = 1
    #             ) AS res
    #         WHERE res.rn = 1
    #         GROUP BY res.timestamp , res.city_id , res.parking_id
    #         ORDER BY res.timestamp ASC
    # '''

    select_t_parking_kvt1 = '''
    SELECT
        NOW() AS add_time ,
        res.timestamp AS timestamp ,
        res.city_id ,
        res.parking_id ,
        COUNT(res.id) AS kvt
    FROM 
        (
        SELECT 
            res_tab.timestamp_hour AS "timestamp" ,
            ta.city_id ,
            ta.id AS parking_id ,
            res_tab.id ,
            6371000 * acos(
                                cos(radians(res_tab.g_lat)) * 
                                cos(radians(ta.lat)) * 
                                cos(radians(ta.lng) - radians(res_tab.g_lng)) + 
                                sin(radians(res_tab.g_lat)) * 
                                sin(radians(ta.lat))) AS distance ,
            RANK() OVER (PARTITION BY res_tab.timestamp_hour, res_tab.id ORDER BY 6371000 * acos(
                                                                                        cos(radians(res_tab.g_lat)) * 
                                                                                        cos(radians(ta.lat)) * 
                                                                                        cos(radians(ta.lng) - radians(res_tab.g_lng)) + 
                                                                                        sin(radians(res_tab.g_lat)) * 
                                                                                        sin(radians(ta.lat))) ASC) AS rn
        FROM 
            (
            SELECT 
                tbh."timestamp" ,
                date_trunc('hour', tbh."timestamp") AS "timestamp_hour" ,
                tbh.id ,
                tbh.g_lat ,
                tbh.g_lng ,
                RANK() OVER (PARTITION BY date_trunc('hour', tbh."timestamp") ORDER BY tbh."timestamp" DESC) AS rn
            FROM damir.t_bike_history tbh
            WHERE tbh.error_status IN (0,7)
                AND tbh."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                --AND date_trunc('hour', tbh."timestamp") >= NOW()::date
                ) AS res_tab
        CROSS JOIN damir.t_area ta 
        WHERE res_tab.rn = 1
            AND ta.active = 1
        ) AS res
    WHERE res.rn = 1
    GROUP BY res.timestamp , res.city_id , res.parking_id
    ORDER BY res.timestamp ASC
    '''
    # df_t_parking_kvt = pd.read_sql(select_t_parking_kvt, engine_postgresql)
    df_t_parking_kvt1 = pd.read_sql(select_t_parking_kvt1, engine_postgresql)

    # # Проверка количества строк. Начало
    # select_df_parking_count = '''
    #         SELECT
    #             min("timestamp") ,
    #             max("timestamp") ,
    #             count(*)
    #         FROM damir.t_parking_kvt
    #         WHERE damir.t_parking_kvt."timestamp" >= DATE(NOW() + INTERVAL '2 hours') - INTERVAL '1 hours'
    #                AND damir.t_parking_kvt."timestamp" <= NOW() + INTERVAL '2 hour'
    #     '''
    # df_parking_count = pd.read_sql(select_df_parking_count, engine_postgresql)
    # print('Количество записей к удалению в parking_count_kvt: {}, {}, {}'.format(df_parking_count.iloc[0].iloc[2],
    #                                                                              df_parking_count.iloc[0].iloc[0],
    #                                                                              df_parking_count.iloc[0].iloc[1]))
    # # Проверка количества строк. Конец


    # # Очистка старых данных в t_parking_kvt
    # truncate_t_parking_kvt = '''DELETE FROM damir.t_parking_kvt
    #    WHERE damir.t_parking_kvt."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours';
    #    '''
    ' '
    # with engine_postgresql.connect() as connection:
    #     with connection.begin() as transaction:
    #         connection.execute(sa.text(truncate_t_parking_kvt))
    #         transaction.commit()
    #         print(f"Таблица t_parking_kvt успешно очищена!")

    # df_t_parking_kvt.to_sql("t_parking_kvt", engine_postgresql, if_exists="append", index=False)
    # print('Таблица df_t_parking_kvt успешно обновлена!')


    # Очистка старых данных в t_parking_kvt1
    truncate_t_parking_kvt1 = '''DELETE FROM damir.t_parking_kvt1
       WHERE damir.t_parking_kvt1."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours';
       '''

    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(sa.text(truncate_t_parking_kvt1))
            transaction.commit()
            print(f"Таблица t_parking_kvt1 успешно очищена!")

    df_t_parking_kvt1.to_sql("t_parking_kvt1", engine_postgresql, if_exists="append", index=False)
    print('Таблица df_t_parking_kvt1 успешно обновлена!')

    # # Проверка количества строк. Начало
    # select_df_parking_count111 = '''
    #             SELECT
    #                 min("timestamp") ,
    #                 max("timestamp") ,
    #                 count(*)
    #             FROM damir.t_parking_kvt
    #         '''
    # df_parking_count111 = pd.read_sql(select_df_parking_count111, engine_postgresql)
    # print('Всего записей в parking_count_kvt: {}, {}, {}'.format(df_parking_count111.iloc[0].iloc[2],
    #                                                                              df_parking_count111.iloc[0].iloc[0],
    #                                                                              df_parking_count111.iloc[0].iloc[1]))
    # # Проверка количества строк. Конец


    # Выгрузка t_parking_kvt. Конец

    # Выгрузка t_area_revenue_stats. Начало
    # Копирую t_area_revenue_stats
    # select_t_area_revenue_stats = '''
    #     SELECT
    #         NOW() AS add_time ,
    #         res."date" ,
    #         res.city_id ,
    #         res.area ,
    #         SUM(res.kvt)/24 AS kvt ,
    #         SUM(res.poezdok) AS poezdok ,
    #         SUM(res.obzchaya_stoimost) AS obzchaya_stoimost,
    #         SUM(res.oplacheno_bonusami) AS oplacheno_bonusami,
    #         SUM(res.skidka) AS skidka,
    #         SUM(res.abon) AS abon
    #     FROM
    #         (
    #         SELECT
    #             DATE_TRUNC('day', COALESCE(tpk."timestamp" , dtprs."timestamp" ))::date AS date ,
    #             COALESCE(tpk.city_id , dtprs.city_id ) AS city_id,
    #             p.area ,
    #             COALESCE(tpk.parking_id , dtprs.parking_id ) AS parking_id,
    #             COALESCE(tpk.kvt, 0)::int AS kvt,
    #             COALESCE(dtprs.poezdok, 0)::int AS poezdok,
    #             COALESCE(dtprs.obzchaya_stoimost, 0)::float AS obzchaya_stoimost,
    #             COALESCE(dtprs.oplacheno_bonusami, 0)::float AS oplacheno_bonusami,
    #             COALESCE(dtprs.skidka, 0)::float AS skidka,
    #             COALESCE(dtprs.abon, 0)::float AS abon
    #         FROM damir.t_parking_kvt tpk
    #         FULL JOIN damir.t_parking_revenue_stats dtprs ON tpk."timestamp" = dtprs."timestamp" AND tpk.city_id = dtprs.city_id AND tpk.parking_id = dtprs.parking_id
    #         LEFT JOIN public.parking p ON COALESCE(tpk.parking_id , dtprs.parking_id ) = p.parking_id
    #         ) AS res
    #     GROUP BY res."date" , res.city_id , res.area
    #     ORDER BY res."date" ASC
    # '''
    # select_t_area_revenue_stats = '''
    # WITH kvt AS (
    #     SELECT
    #         date_trunc('day', tpk."timestamp")::date AS date,
    #         tpk.city_id ,
    #         tpk.parking_id ,
    #         ROUND(AVG(tpk.kvt)) AS kvt
    #     FROM damir.t_parking_kvt tpk
    #     --WHERE tpk.city_id = 22
    #     GROUP BY date_trunc('day', tpk."timestamp")::date, tpk.city_id, tpk.parking_id
    # ),
    # revenue AS (
    #     SELECT
    #         date_trunc('day', dtprs."timestamp")::date AS date,
    #         dtprs.city_id ,
    #         dtprs.parking_id ,
    #         SUM(dtprs.poezdok) AS poezdok,
    #         SUM(dtprs.obzchaya_stoimost) AS obzchaya_stoimost,
    #         SUM(dtprs.oplacheno_bonusami) AS oplacheno_bonusami,
    #         SUM(dtprs.skidka) AS skidka,
    #         SUM(dtprs.abon) AS abon
    #     FROM damir.t_parking_revenue_stats dtprs
    #     GROUP BY date_trunc('day', dtprs."timestamp")::date , dtprs.city_id , dtprs.parking_id
    # )
    # SELECT
    #     NOW() AS add_time ,
    #     res."date" ,
    #     res.city_id ,
    #     res.area ,
    #     SUM(res.kvt) AS kvt ,
    #     SUM(res.poezdok) AS poezdok ,
    #     SUM(res.obzchaya_stoimost) AS obzchaya_stoimost,
    #     SUM(res.oplacheno_bonusami) AS oplacheno_bonusami,
    #     SUM(res.skidka) AS skidka,
    #     SUM(res.abon) AS abon
    # FROM
    # (
    #     SELECT
    #         kvt.date ,
    #         COALESCE(kvt.city_id, revenue.city_id) AS city_id,
    #         p.area ,
    #         COALESCE(kvt.parking_id, revenue.parking_id) AS parking_id ,
    #         COALESCE(kvt.kvt, 0) AS kvt,
    #         COALESCE(revenue.poezdok, 0) AS poezdok,
    #         COALESCE(revenue.obzchaya_stoimost, 0) AS obzchaya_stoimost,
    #         COALESCE(revenue.oplacheno_bonusami, 0) AS oplacheno_bonusami,
    #         COALESCE(revenue.skidka, 0) AS skidka,
    #         COALESCE(revenue.abon, 0) AS abon
    #     FROM kvt
    #     LEFT JOIN revenue ON kvt.date = revenue.date AND kvt.city_id = revenue.city_id AND kvt.parking_id = revenue.parking_id
    #     LEFT JOIN public.parking p ON COALESCE(kvt.parking_id , revenue.parking_id ) = p.parking_id
    # ) AS res
    # GROUP BY res."date"::date , res.city_id , res.area
    # ORDER BY res."date" ASC
    # '''

    select_t_area_revenue_stats1 = '''
    WITH revenue AS (
        SELECT
            tprs."timestamp"::date ,
            tprs.city_id ,
            tprs.parking_id ,
            SUM(tprs.poezdok) AS poezdok,
            --SUM(tprs.obzchaya_stoimost) - SUM(tprs.dolgi) - (SUM(tprs.vyruchka_s_abonementov) - SUM(tprs.sum_mnogor_abon)) - SUM(tprs.oplacheno_bonusami) + SUM(tprs.vyruchka_s_abonementov) - SUM(tprs.skidka) AS dd ,
            SUM(tprs.obzchaya_stoimost) AS obzchaya_stoimost,
            SUM(tprs.oplacheno_bonusami) AS oplacheno_bonusami,
            SUM(tprs.skidka) AS skidka,
            SUM(tprs.dolgi) AS dolgi,
            SUM(tprs.vyruchka_s_abonementov) AS vyruchka_s_abonementov,
            SUM(tprs.sum_mnogor_abon) AS sum_mnogor_abon 
        FROM damir.t_parking_revenue_stats1 tprs
        GROUP BY tprs."timestamp"::date, tprs.city_id, tprs.parking_id
    ),
    kvt AS (
        SELECT 
            tpk."timestamp"::date AS timestamp ,
            tpk.city_id ,
            tpk.parking_id ,
            ROUND(AVG(tpk.kvt)) AS kvt
        FROM damir.t_parking_kvt1 tpk
        GROUP BY tpk."timestamp"::date, tpk.city_id , tpk.parking_id
    )
    SELECT 
        NOW() AS add_time ,
        res.timestamp AS date,
        res.city_id ,
        res.area ,
        SUM(res.kvt)::int AS kvt,
        SUM(res.poezdok)::int AS poezdok,
        SUM(res.obzchaya_stoimost)::float AS obzchaya_stoimost,
        SUM(res.oplacheno_bonusami)::float AS oplacheno_bonusami,
        SUM(res.skidka)::float AS skidka,
        SUM(res.dolgi)::float AS dolgi,
        SUM(res.vyruchka_s_abonementov)::float AS vyruchka_s_abonementov,
        SUM(res.sum_mnogor_abon)::float AS sum_mnogor_abon
    FROM 
    (
        SELECT 
            COALESCE(kvt.timestamp, revenue.timestamp) AS timestamp,
            COALESCE(kvt.city_id, revenue.city_id) AS city_id,
            COALESCE(kvt.parking_id, revenue.parking_id) AS parking_id,
            p.area ,
            COALESCE(kvt.kvt, 0) AS kvt,
            COALESCE(revenue.poezdok, 0) AS poezdok ,
            COALESCE(revenue.obzchaya_stoimost, 0) AS obzchaya_stoimost,
            COALESCE(revenue.oplacheno_bonusami, 0) AS oplacheno_bonusami,
            COALESCE(revenue.skidka, 0) AS skidka,
            COALESCE(revenue.dolgi, 0) AS dolgi,
            COALESCE(revenue.vyruchka_s_abonementov, 0) AS vyruchka_s_abonementov,
            COALESCE(revenue.sum_mnogor_abon, 0) AS sum_mnogor_abon
        FROM kvt
        FULL JOIN revenue ON kvt.timestamp = revenue.timestamp AND kvt.parking_id = revenue.parking_id
        LEFT JOIN public.parking p ON COALESCE(kvt.parking_id , revenue.parking_id ) = p.parking_id
    ) AS res
    GROUP BY res.timestamp , res.city_id , res.area
    ORDER BY res.timestamp
    '''

    # df_t_area_revenue_stats = pd.read_sql(select_t_area_revenue_stats, engine_postgresql)
    df_t_area_revenue_stats1 = pd.read_sql(select_t_area_revenue_stats1, engine_postgresql)

    # Очистка таблицы
    # truncate_t_area_revenue_stats = "TRUNCATE TABLE t_area_revenue_stats RESTART IDENTITY;"
    # with engine_postgresql.connect() as connection:
    #     with connection.begin() as transaction:
    #         print(f"Попытка очистить таблицу")
    #         # Очистка t_area_revenue_stats
    #         connection.execute(sa.text(truncate_t_area_revenue_stats))
    #         # Если ошибок нет, транзакция фиксируется автоматически
    #         print(f"Таблица t_area_revenue_stats успешно очищена!")

    # df_t_area_revenue_stats.to_sql("t_area_revenue_stats", engine_postgresql, if_exists="append", index=False)
    # print('Таблица t_area_revenue_stats успешно обновлена!')

    # Очистка таблицы
    truncate_t_area_revenue_stats1 = "TRUNCATE TABLE t_area_revenue_stats1 RESTART IDENTITY;"
    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            print(f"Попытка очистить таблицу")
            # Очистка t_area_revenue_stats1
            connection.execute(sa.text(truncate_t_area_revenue_stats1))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица t_area_revenue_stats1 успешно очищена!")

    df_t_area_revenue_stats1.to_sql("t_area_revenue_stats1", engine_postgresql, if_exists="append", index=False)
    print('Таблица t_area_revenue_stats1 успешно обновлена!')

    # Выгрузка t_area_revenue_stats. Конец



    # Выгрузка t_area_revenue_stats2 pandas. Начало
    # Очистка старых данных t_area_revenue_stats2
    truncate_t_area_revenue_stats2 = '''
    DELETE FROM damir.t_area_revenue_stats2
    WHERE damir.t_area_revenue_stats2."timestamp_hour" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours';
    '''
    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(sa.text(truncate_t_area_revenue_stats2))
            transaction.commit()
            print(f"Таблица t_area_revenue_stats2 успешно очищена!")

    # КВТ
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
                AND tbh."timestamp" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
            ) AS res_tab
        WHERE res_tab.rn = 1
    '''
    df_kvt = pd.read_sql(select_kvt, engine_postgresql)

    # Выгрузка areas
    select_areas = '''
        SELECT
            ta.city_id ,
            ta.id AS area_id ,
            ta."name" AS area_name ,
            ta.detail AS area_detail
        FROM damir.t_area ta
        WHERE ta."name" LIKE '%%| Area |%%'
    '''
    df_areas = pd.read_sql(select_areas, engine_postgresql)

    # area в полигоны
    df_areas['area_poly'] = df_areas['area_detail'].apply(decode_polyline_to_tuples)

    # Соединяю КВТ и area
    df_kvt_area = df_kvt.merge(df_areas, how='cross')
    df_kvt_area['res'] = df_kvt_area.apply(poly_contains_point_kvt, axis=1)
    df_kvt_area_res = df_kvt_area[df_kvt_area['res'] == True]
    df_kvt_area_res = df_kvt.merge(df_kvt_area_res, how='left', on=['timestamp_hour', 'id'])
    df_kvt_area_res = df_kvt_area_res[['timestamp_hour', 'id', 'city_id', 'city_id_y', 'area_id', 'area_name']]
    df_kvt_area_res = df_kvt_area_res.groupby(['timestamp_hour', 'city_id', 'area_id', 'area_name'], dropna=False) \
        .agg({'id': 'count'}) \
        .rename(columns={'id': 'kvt'}) \
        .reset_index() \
        .fillna(0)

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

    # Соединяю orders и area
    df_orders_area = df_orders.merge(df_areas, how='cross')
    df_orders_area['res'] = df_orders_area.apply(poly_contains_point_orders, axis=1)
    df_orders_area = df_orders_area[df_orders_area['res'] == True]
    df_orders_areas_res = df_orders.merge(df_orders_area, on=['timestamp_hour', 'id'], how='left')
    df_orders_areas_res = df_orders_areas_res.groupby(['timestamp_hour', 'city_id', 'area_id', 'area_name'],
                                                      dropna=False) \
        .agg({'id': 'count',
              'ride_amount_x': 'sum',
              'discount_x': 'sum',
              'bike_discount_amount_x': 'sum',
              'subscription_price_x': 'sum'}) \
        .rename(columns={'id': 'poezdok',
                         'ride_amount_x': 'obzchaya_stoimost',
                         'discount_x': 'oplacheno_bonusami',
                         'bike_discount_amount_x': 'skidka',
                         'subscription_price_x': 'abon'}) \
        .reset_index() \
        .fillna(0)

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
    df_orders_kvt_area_res = df_kvt_area_res.merge(df_orders_areas_res, how='outer',
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
                         'sum_mnogor_abon_res': 'sum_mnogor_abon'}) \
        .fillna(0)

    # Загрузка свежих данных в t_area_revenue_stats2
    df_orders_kvt_area_res.to_sql("t_area_revenue_stats2", engine_postgresql, if_exists="append", index=False)
    print('Таблица t_area_revenue_stats2 успешно обновлена!')




    # Выгрузка t_area_revenue_stats2 pandas. Конец


if __name__ == "__main__":
    main()
