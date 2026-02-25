import os
import json

import pandas as pd
import sqlalchemy as sa
import google.oauth2.service_account
import googleapiclient.discovery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
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

class GoogleSheetsManager:
    """
    Класс для управления Google Sheets с функцией очистки и записи данных
    """

    def __init__(self, credentials_file, spreadsheet_id):
        """
        Инициализация менеджера Google Sheets

        Параметры:
        credentials_file (str): Путь к файлу с credentials сервисного аккаунта
        spreadsheet_id (str): ID Google таблицы
        """
        self.spreadsheet_id = spreadsheet_id
        self.credentials_file = credentials_file
        self.service = self._authenticate()

    def _authenticate(self):
        """
        Аутентификация и создание сервиса Google Sheets
        """
        try:
            credentials = google.oauth2.service_account.Credentials.from_service_account_file(
                self.credentials_file,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            service = build('sheets', 'v4', credentials=credentials)
            return service
        except Exception as e:
            print(f"Ошибка аутентификации: {e}")
            raise

    def get_sheet_metadata(self, sheet_name=None):
        """
        Получает метаданные листа, включая количество строк и столбцов

        Параметры:
        sheet_name (str): Название листа (если None, используется первый лист)

        Возвращает:
        dict: Метаданные листа
        """
        try:
            # Получаем метаданные таблицы
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id,
                ranges=[],
                includeGridData=False
            ).execute()

            # Находим нужный лист
            if sheet_name:
                sheet = next((s for s in spreadsheet['sheets']
                              if s['properties']['title'] == sheet_name), None)
                if not sheet:
                    raise ValueError(f"Лист '{sheet_name}' не найден")
            else:
                sheet = spreadsheet['sheets'][0]  # Берем первый лист

            return {
                'sheet_id': sheet['properties']['sheetId'],
                'title': sheet['properties']['title'],
                'row_count': sheet['properties'].get('gridProperties', {}).get('rowCount', 1000),
                'column_count': sheet['properties'].get('gridProperties', {}).get('columnCount', 26)
            }
        except Exception as e:
            print(f"Ошибка получения метаданных: {e}")
            return None

    def truncate_sheet(self, range_name=None):
        """
        Очищает все значения в указанном диапазоне (аналог TRUNCATE)

        Параметры:
        range_name (str): Диапазон для очистки (например, 'Sheet1!A1:Z1000')
                          Если None, очищает весь первый лист

        Возвращает:
        bool: True если успешно, False в случае ошибки
        """
        try:
            if range_name is None:
                # Если диапазон не указан, получаем метаданные и очищаем весь лист
                sheet_meta = self.get_sheet_metadata()
                if sheet_meta:
                    # Создаем диапазон для всего листа
                    last_column = self._get_column_letter(sheet_meta['column_count'])
                    range_name = f"{sheet_meta['title']}!A1:{last_column}{sheet_meta['row_count']}"
                else:
                    # Если не удалось получить метаданные, используем большой диапазон
                    range_name = 'Sheet1!A1:Z1000'

            # Очистка диапазона через метод clear
            result = self.service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()

            print(f"✅ Диапазон {range_name} успешно очищен")
            return True

        except HttpError as error:
            print(f"❌ Ошибка API при очистке: {error}")
            return False
        except Exception as e:
            print(f"❌ Ошибка при очистке: {e}")
            return False

    def truncate_sheet_batch(self, sheet_name=None):
        """
        Очищает лист с помощью batchUpdate (более эффективно для больших листов)

        Параметры:
        sheet_name (str): Название листа для очистки

        Возвращает:
        bool: True если успешно, False в случае ошибки
        """
        try:
            # Получаем метаданные листа
            sheet_meta = self.get_sheet_metadata(sheet_name)

            if not sheet_meta:
                print("❌ Не удалось получить метаданные листа")
                return False

            # Создаем запрос на очистку через repeatCell
            requests = [{
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_meta['sheet_id'],
                        "startRowIndex": 0,
                        "endRowIndex": sheet_meta['row_count'],
                        "startColumnIndex": 0,
                        "endColumnIndex": sheet_meta['column_count']
                    },
                    "cell": {
                        "userEnteredValue": None  # Очищаем значения
                    },
                    "fields": "userEnteredValue"
                }
            }]

            body = {
                'requests': requests
            }

            # Выполняем batchUpdate
            result = self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body
            ).execute()

            print(f"✅ Лист '{sheet_meta['title']}' успешно очищен (batchUpdate)")
            return True

        except HttpError as error:
            print(f"❌ Ошибка API при batch очистке: {error}")
            return False
        except Exception as e:
            print(f"❌ Ошибка при batch очистке: {e}")
            return False

    def _get_column_letter(self, column_number):
        """
        Преобразует номер колонки в буквенное обозначение (1 -> A, 26 -> Z, 27 -> AA)

        Параметры:
        column_number (int): Номер колонки (начиная с 1)

        Возвращает:
        str: Буквенное обозначение колонки
        """
        result = ""
        while column_number > 0:
            column_number -= 1
            result = chr(column_number % 26 + 65) + result
            column_number //= 26
        return result

    def write_dataframe(self, df, range_name, value_input_option='RAW', include_headers=True):
        """
        Записывает DataFrame в Google Sheets

        Параметры:
        df (pandas.DataFrame): DataFrame для записи
        range_name (str): Диапазон для записи (например, 'Sheet1!A1')
        value_input_option (str): 'RAW' или 'USER_ENTERED'
        include_headers (bool): Включать ли заголовки столбцов

        Возвращает:
        dict: Результат операции или None в случае ошибки
        """
        try:
            # Подготовка данных
            if include_headers:
                values = [df.columns.values.tolist()] + df.values.tolist()
            else:
                values = df.values.tolist()

            # Создание тела запроса
            body = {
                'values': values
            }

            # Запись данных
            result = self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body
            ).execute()

            print(f"✅ Записано {result.get('updatedCells')} ячеек")
            print(f"📊 Диапазон: {result.get('updatedRange')}")
            print(f"📝 Заголовки: {'включены' if include_headers else 'не включены'}")

            return result

        except HttpError as error:
            print(f"❌ Ошибка API при записи: {error}")
            return None
        except Exception as e:
            print(f"❌ Ошибка при записи: {e}")
            return None

    def truncate_and_write(self, df, range_name, sheet_name=None,
                           use_batch_clear=False, include_headers=True):
        """
        Выполняет очистку (TRUNCATE) и запись новых данных

        Параметры:
        df (pandas.DataFrame): DataFrame для записи
        range_name (str): Диапазон для записи (например, 'Sheet1!A1')
        sheet_name (str): Название листа для очистки
        use_batch_clear (bool): Использовать batchUpdate для очистки
        include_headers (bool): Включать ли заголовки столбцов

        Возвращает:
        bool: True если успешно, False в случае ошибки
        """
        print("🚀 Начало операции truncate and write...")
        print("=" * 50)

        # Шаг 1: Очистка данных
        print("📝 Шаг 1: Очистка существующих данных...")

        if use_batch_clear:
            success = self.truncate_sheet_batch(sheet_name)
        else:
            # Очищаем тот же диапазон, в который будем писать
            success = self.truncate_sheet(range_name)

        if not success:
            print("❌ Очистка не удалась. Операция прервана.")
            return False

        print("✅ Очистка выполнена успешно")
        print("-" * 50)

        # Шаг 2: Запись новых данных
        print("📝 Шаг 2: Запись новых данных...")

        result = self.write_dataframe(
            df=df,
            range_name=range_name,
            include_headers=include_headers
        )

        if result:
            print("✅ Данные успешно записаны")
            print("=" * 50)
            print("🎉 Операция truncate and write завершена успешно!")
            return True
        else:
            print("❌ Запись данных не удалась")
            return False

    def truncate_and_write_with_resize(self, df, sheet_name=None, start_cell='A1',
                                       include_headers=True):
        """
        Очищает лист и записывает данные, автоматически подгоняя размеры

        Параметры:
        df (pandas.DataFrame): DataFrame для записи
        sheet_name (str): Название листа
        start_cell (str): Начальная ячейка для записи
        include_headers (bool): Включать ли заголовки столбцов

        Возвращает:
        bool: True если успешно, False в случае ошибки
        """
        try:
            print("🚀 Начало операции truncate, resize and write...")
            print("=" * 50)

            # Получаем метаданные листа
            sheet_meta = self.get_sheet_metadata(sheet_name)

            if not sheet_meta:
                print("❌ Не удалось получить метаданные листа")
                return False

            # Определяем необходимые размеры
            required_rows = len(df) + (1 if include_headers else 0)
            required_cols = len(df.columns)

            print(f"📊 Требуется строк: {required_rows}, столбцов: {required_cols}")

            # Создаем запрос на изменение размеров и очистку
            requests = [
                # Очищаем все ячейки
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_meta['sheet_id'],
                            "startRowIndex": 0,
                            "endRowIndex": sheet_meta['row_count'],
                            "startColumnIndex": 0,
                            "endColumnIndex": sheet_meta['column_count']
                        },
                        "cell": {
                            "userEnteredValue": None
                        },
                        "fields": "userEnteredValue"
                    }
                },
                # Изменяем количество строк, если нужно
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_meta['sheet_id'],
                            "gridProperties": {
                                "rowCount": max(required_rows, sheet_meta['row_count']),
                                "columnCount": max(required_cols, sheet_meta['column_count'])
                            }
                        },
                        "fields": "gridProperties.rowCount,gridProperties.columnCount"
                    }
                }
            ]

            # Выполняем batchUpdate
            body = {'requests': requests}
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body
            ).execute()

            print("✅ Лист очищен и размеры скорректированы")

            # Записываем данные
            range_name = f"{sheet_meta['title']}!{start_cell}"
            result = self.write_dataframe(
                df=df,
                range_name=range_name,
                include_headers=include_headers
            )

            if result:
                print("=" * 50)
                print("🎉 Операция завершена успешно!")
                return True
            else:
                return False

        except Exception as e:
            print(f"❌ Ошибка: {e}")
            return False

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

    google_service_account_json = get_google_creds()

    with open('google_json.json', 'w') as fp:
        json.dump(json.loads(google_service_account_json, strict=False), fp)
    generated_json_file = './google_json.json'


    # Выгрузка t_area_revenue_stats2 pandas. Начало



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
    # Преобразование типов
    df_orders_areas_res['dolgi'] = pd.to_numeric(df_orders_areas_res['dolgi'], errors='coerce')
    df_orders_areas_res['vyruchka_s_abonementov'] = pd.to_numeric(df_orders_areas_res['vyruchka_s_abonementov'], errors='coerce')
    df_orders_areas_res['sum_mnogor_abon'] = pd.to_numeric(df_orders_areas_res['sum_mnogor_abon'], errors='coerce')
    # Преобразование типов
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

    truncate_t_area_revenue_stats2 = '''
        DELETE FROM damir.t_area_revenue_stats2
        WHERE damir.t_area_revenue_stats2."timestamp_hour" >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours';
        '''
    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(sa.text(truncate_t_area_revenue_stats2))
            transaction.commit()
            print(f"Таблица t_area_revenue_stats2 успешно очищена!")

    df_orders_kvt_area_res.to_sql("t_area_revenue_stats2", engine_postgresql, if_exists="append", index=False)
    print('Таблица t_area_revenue_stats2 успешно обновлена!')

    # Выгрузка t_area_revenue_stats2 pandas. Конец

    # Выгрузка t_area_revenue_stats3 pandas. Начало

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
                --AND tbh."timestamp" >= date_trunc('hour', NOW())
                AND tbh."timestamp" >= date_trunc('day', (NOW() + INTERVAL '2 hours')) - INTERVAL '1 days'
            ) AS res_tab
        WHERE res_tab.rn = 1
    '''

    df_kvt = pd.read_sql(select_kvt, engine_postgresql)

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

    # Собираю все в один день
    df_kvt_area_res['start_day'] = pd.to_datetime(df_kvt_area_res['timestamp_hour'].dt.date)
    df_kvt_area_res = df_kvt_area_res.groupby(['start_day', 'city_id', 'area_id', 'area_name'], as_index=False) \
        .agg({'kvt': 'mean'})

    # Выгрузка заказов
    select_orders = '''
        SELECT
            -- NOW() AS add_time ,
            STR_TO_DATE(DATE_FORMAT(from_unixtime(tbu.`date`), '%Y-%m-%d %H:00:00'), "%Y-%m-%d %H:%i:%s") AS timestamp_hour ,
            -- tbu.`date`,
            from_unixtime(tbu.`date`) AS "timestamp" ,
            tbu.id ,
            tb.city_id ,
            tbu.start_lat ,
            tbu.start_lng ,
            IFNULL(tbu.ride_amount,0) AS ride_amount,
            IFNULL(tbu.discount,0) AS discount,
            IFNULL(tpd.bike_discount_amount,0) AS bike_discount_amount,
            IFNULL(ts.price , 0) AS subscription_price
            -- tbu.bid
        FROM shamri.t_bike_use tbu
        LEFT JOIN shamri.t_payment_details tpd ON tbu.id = tpd.ride_id
        LEFT JOIN shamri.t_subscription ts ON tbu.subscription_id = ts.id 
        LEFT JOIN shamri.t_bike tb ON tb.id = tbu.bid
        WHERE tbu.ride_status != 5
             AND 
            -- from_unixtime(tbu.`date`) >= STR_TO_DATE(DATE_FORMAT(NOW(), '%Y-%m-%d %H:00:00'), "%Y-%m-%d %H:%i:%s") - INTERVAL 2 HOUR
             tbu.`date` >= UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
                -- AND tbu.`date` <= UNIX_TIMESTAMP(NOW())
        ORDER BY tbu.`date`ASC
    '''

    df_orders = pd.read_sql(select_orders, engine_mysql)

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

    # Собираю все в один день
    df_orders_areas_res['start_day'] = pd.to_datetime(df_orders_areas_res['timestamp_hour'].dt.date)
    df_orders_areas_res = df_orders_areas_res.groupby(['start_day', 'city_id', 'area_id', 'area_name'], as_index=False) \
        .agg({'poezdok': 'sum',
              'obzchaya_stoimost': 'sum',
              'oplacheno_bonusami': 'sum',
              'skidka': 'sum',
              'abon': 'sum'})

    # Выгрузка показателей для распределения
    select_distr = '''
        WITH dolgi AS (
            SELECT 
                dolgi.timestamp_day AS start_time ,
                dolgi.city_id ,
                SUM(dolgi.debit_cash) AS dolgi
            FROM 
            (
                SELECT
                    tpd.created::date AS timestamp_day,
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
                        --AND to_timestamp( tbu.start_time) >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                        AND to_timestamp( tbu.start_time) >= date_trunc('day', (NOW() + INTERVAL '2 hours')) - INTERVAL '1 days'
                    ) AS dolgi ON tpd.ride_id = dolgi.id
                ) AS dolgi 
            GROUP BY dolgi.timestamp_day , dolgi.city_id
        ),
        vyruchka_s_abonementov AS (
            SELECT 
                distr_poezdki_po_gorodam.start_time::date AS start_time,
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
                        --AND TO_TIMESTAMP(t_bike_use.start_time) >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                        AND TO_TIMESTAMP(t_bike_use.start_time) >= date_trunc('day', (NOW() + INTERVAL '2 hours')) - INTERVAL '1 days'
                    GROUP BY 1, 2
                    ) AS distr_poezdki_po_gorodam
            ) AS distr_poezdki_po_gorodam
            LEFT JOIN (
                SELECT 
                    TO_CHAR(t_trade.date, 'YYYY-MM-DD') AS start_time,
                    SUM(COALESCE(t_trade.amount, 0)) AS vyruchka_s_abonementov
                FROM damir.t_trade
                WHERE t_trade.type = 6 
                    AND t_trade.status = 1 
                    --AND t_trade.date >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                    AND t_trade.date >= date_trunc('day', (NOW() + INTERVAL '2 hours')) - INTERVAL '1 days'
                GROUP BY 1
                ) AS sum_uspeh_abon
                ON distr_poezdki_po_gorodam.start_time = sum_uspeh_abon.start_time
        ),
        sum_mnogor_abon AS (
            SELECT 
                distr_poezdki_po_gorodam.start_time::date AS start_time,
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
                        --AND TO_TIMESTAMP(t_bike_use.start_time) >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                        AND TO_TIMESTAMP(t_bike_use.start_time) >= date_trunc('day', (NOW() + INTERVAL '2 hours')) - INTERVAL '1 days'
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
                    --t_subscription_mapping.start_time >= date_trunc('hour', NOW() + INTERVAL '2 hours') - INTERVAL '2 hours'
                    t_subscription_mapping.start_time >= date_trunc('day', (NOW() + INTERVAL '2 hours')) - INTERVAL '1 days' 
                    GROUP BY 1
                ) AS sum_mnogor_abon
                ON distr_poezdki_po_gorodam.start_time = sum_mnogor_abon.start_time
        )
        SELECT 
            COALESCE(dolgi.start_time , vyruchka_s_abonementov.start_time , sum_mnogor_abon.start_time ) AS start_day ,
            COALESCE(dolgi.city_id , vyruchka_s_abonementov.city_id , sum_mnogor_abon.city_id ) AS city_id,
            COALESCE(dolgi.dolgi, 0) AS dolgi,
            vyruchka_s_abonementov.vyruchka_s_abonementov ,
            sum_mnogor_abon.sum_mnogor_abon
        FROM dolgi
        FULL JOIN vyruchka_s_abonementov ON dolgi.city_id = vyruchka_s_abonementov.city_id AND dolgi.start_time = vyruchka_s_abonementov.start_time
        FULL JOIN sum_mnogor_abon ON COALESCE(dolgi.city_id , vyruchka_s_abonementov.city_id ) = sum_mnogor_abon.city_id AND COALESCE(dolgi.start_time , vyruchka_s_abonementov.start_time ) = sum_mnogor_abon.start_time
    '''

    df_distr = pd.read_sql(select_distr, engine_postgresql)
    df_distr['start_day'] = pd.to_datetime(df_distr['start_day'])

    # Соединяю основную таблицу с таблицей distr
    df_orders_areas_res = df_orders_areas_res.merge(df_distr, how='left', on=['start_day', 'city_id'])
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
                                                   on=['start_day', 'city_id', 'area_id', 'area_name'])

    df_orders_kvt_area_res = df_orders_kvt_area_res[['start_day',
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

    df_orders_kvt_area_res.rename(columns={'start_day': 'timestamp_hour'}, inplace=True)

    truncate_t_area_revenue_stats3 = '''
        DELETE FROM damir.t_area_revenue_stats3
        WHERE damir.t_area_revenue_stats3."timestamp_hour" >= date_trunc('day', (NOW() + INTERVAL '2 hours')) - INTERVAL '1 days';
        '''
    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(sa.text(truncate_t_area_revenue_stats3))
            transaction.commit()
            print(f"Таблица t_area_revenue_stats3 успешно очищена!")


    df_orders_kvt_area_res.to_sql("t_area_revenue_stats3", engine_postgresql, if_exists="append", index=False)
    print('Таблица t_area_revenue_stats3 успешно обновлена!')

    # Выгрузка t_area_revenue_stats3 pandas. Конец

    # Выгрузка t_last_kvt pandas. Начало
    # Копирую t_last_kvt
    select_t_last_kvt = '''
        WITH kvt AS (
            SELECT 
                res.city_id ,
                res.parking_id ,
                res."name" AS parking_name ,
                COUNT(res.id) AS kvt
            FROM 
            (
                SELECT 
                    tb.city_id ,
                    tb.id ,
                    tb.g_lat ,
                    tb.g_lng ,
                    ta.id AS parking_id ,
                    ta."name" ,
                    ta.lat ,
                    ta.lng ,
                    RANK() OVER (PARTITION BY tb.id ORDER BY 6371000 * acos(
                                                                            cos(radians(tb.g_lat)) * 
                                                                            cos(radians(ta.lat)) * 
                                                                            cos(radians(ta.lng) - radians(tb.g_lng)) + 
                                                                            sin(radians(tb.g_lat)) * 
                                                                            sin(radians(ta.lat))) ASC) AS rn
                FROM damir.t_bike tb 
                CROSS JOIN damir.t_area ta
                WHERE tb.error_status IN (0,7)
                    AND ta.active = 1
                ) AS res
            WHERE res.rn = 1
            GROUP BY res.city_id , res.parking_id , res."name"
        ),
        parking_all AS (
            SELECT 
                ta.city_id ,
                ta.id AS parking_id ,
                ta."name" AS parking_name
            FROM damir.t_area ta
            WHERE ta.active = 1
        )
        SELECT
            NOW() AS add_time ,
            COALESCE(kvt.city_id , parking_all.city_id) AS city_id,
            COALESCE(tap.area_id, 0) AS area_id,
            COALESCE(tap.area_name, '0') AS area_name,
            COALESCE(parking_all.parking_id , kvt.parking_id) AS parking_id,
            COALESCE(parking_all.parking_name , kvt.parking_name) AS parking_name,
            COALESCE(kvt.kvt, 0) AS kvt 
        FROM parking_all
        LEFT JOIN kvt ON parking_all.parking_id = kvt.parking_id
        LEFT JOIN damir.t_areas_parkings tap ON COALESCE(parking_all.parking_id , kvt.parking_id) = tap.parking_id
    '''
    df_t_last_kvt = pd.read_sql(select_t_last_kvt, engine_postgresql)

    # Очистка таблицы
    truncate_t_last_kvt = "TRUNCATE TABLE t_last_kvt RESTART IDENTITY;"

    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            print(f"Попытка очистить таблицу")
            # Очистка t_bike
            connection.execute(sa.text(truncate_t_last_kvt))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица truncate_t_last_kvt успешно очищена!")

    df_t_last_kvt.to_sql("t_last_kvt", engine_postgresql, if_exists="append", index=False)
    print('Таблица t_last_kvt успешно обновлена!')
    # Выгрузка t_last_kvt pandas. Конец

    # Обновление в Parking metadata в Google Sheets. Начало
    SPREADSHEET_ID = '1b1lck8cPfqtBAOuzGjMYra6qnCs2dDn4TeUuB4FWHrU'
    CREDENTIALS_FILE = generated_json_file

    # Запрос последних данных из postgresql
    select_parking_metadata = '''
        SELECT 
            dtprs.parking_id ,
            ROUND(COALESCE(AVG(dtprs.poezdok) FILTER (WHERE (EXTRACT(HOUR FROM dtprs."timestamp") IN (6,7,8,9,10,11,12,13,14,15,16,17)) AND (EXTRACT(DOW FROM dtprs."timestamp") IN (1,2,3,4,5))), 0)) AS target_scooter_count_workday_6_to_18 ,
            ROUND(COALESCE(AVG(dtprs.poezdok) FILTER (WHERE (EXTRACT(HOUR FROM dtprs."timestamp") IN (18,19,20,21,22,23,0,1,2,3,4,5)) AND (EXTRACT(DOW FROM dtprs."timestamp") IN (1,2,3,4,5))), 0)) AS target_scooter_count_workday_18_to_6 ,
            ROUND(COALESCE(AVG(dtprs.poezdok) FILTER (WHERE EXTRACT(DOW FROM dtprs."timestamp") IN (6,7)), 0)) AS target_scooter_count_weekend
        FROM damir.t_parking_revenue_stats1 dtprs
        WHERE dtprs."timestamp"::date >= (NOW() + INTERVAL '2 HOURS')::date - INTERVAL '13 DAY'
        --WHERE dtprs."timestamp"::date >= (NOW())::date - INTERVAL '13 DAY'
        GROUP BY dtprs.parking_id
        ORDER BY dtprs.parking_id ASC
    '''

    df_parking_metadata = pd.read_sql(select_parking_metadata, engine_postgresql)

    # Создаем экземпляр менеджера
    manager = GoogleSheetsManager(CREDENTIALS_FILE, SPREADSHEET_ID)

    # Удаление + Загрузка в Google Sheets
    manager.truncate_and_write(
        df=df_parking_metadata,
        range_name='Sheet2!A:D',
        sheet_name='Sheet2',
        use_batch_clear=True,
        include_headers=True
    )

    # Обновление в Parking metadata в Google Sheets. Конец

if __name__ == "__main__":
    main()
