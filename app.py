import pandas as pd
import sqlite3
from collections import Counter

class DataProcessor:
    def __init__(self, excel_file, db_file):
        self.excel_file = excel_file
        self.db_file = db_file

    def process_excel_data(self):
        # Чтение данных из Excel-файла
        df = pd.read_excel(self.excel_file)

        # Создание и подключение к базе данных SQLite
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        # Создание таблицы COUNTRY
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS COUNTRY (
                ID_COUNTRY INTEGER PRIMARY KEY,
                NAME_COUNTRY TEXT
            )
        ''')

        # Создание таблицы ISG
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ISG (
                ID_ISG INTEGER PRIMARY KEY,
                NAME_ISG TEXT
            )
        ''')

        # Создание таблицы GOODS
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS GOODS (
                ID_TOVAR INTEGER PRIMARY KEY,
                NAME_TOVAR TEXT,
                BARCOD TEXT,
                ID_COUNTRY INTEGER,
                ID_ISG INTEGER
            )
        ''')

        # Заполняем таблицы COUNTRY и ISG уникальными значениями
        unique_countries = df['COUNTRY'].unique()
        for country in unique_countries:
            cursor.execute("INSERT INTO COUNTRY (NAME_COUNTRY) VALUES (?)", (country,))
        
        
        unique_isgs = df[['ID_ISG', 'ISG']].drop_duplicates()
        for row in unique_isgs.iterrows():
            row_all = row[1]
            id_isg = int(row_all[0])
            name_isg = row_all[1]
            cursor.execute("INSERT INTO ISG (ID_ISG, NAME_ISG) VALUES (?, ?)", (id_isg, name_isg))

        # Заполняем таблицу GOODS данными из Excel
        for index, row in df.iterrows():
            country = row['COUNTRY']
            isg = row['ISG']
            id_tovar = row['ID_TOVAR']
            if isinstance(id_tovar, str):
                id_tovar = int(id_tovar.replace('-', ''))
            cursor.execute("SELECT COUNT(*) FROM GOODS WHERE ID_TOVAR = ?", (row['ID_TOVAR'],))
            count = cursor.fetchone()[0]

            if count == 0:
                # Вставка новой записи
                cursor.execute("INSERT INTO GOODS (ID_TOVAR, NAME_TOVAR, BARCOD, ID_COUNTRY, ID_ISG) VALUES (?, ?, ?, (SELECT ID_COUNTRY FROM COUNTRY WHERE NAME_COUNTRY = ?), (SELECT ID_ISG FROM ISG WHERE NAME_ISG = ?))",
                               (id_tovar, row['TOVAR'], row['BARCOD'], country, isg))
            else:
                # Обновление существующей записи
                cursor.execute("UPDATE GOODS SET NAME_TOVAR = ?, BARCOD = ?, ID_COUNTRY = (SELECT ID_COUNTRY FROM COUNTRY WHERE NAME_COUNTRY = ?), ID_ISG = (SELECT ID_ISG FROM ISG WHERE NAME_ISG = ?) WHERE ID_TOVAR = ?",
                               (row['ID_TOVAR'], row['TOVAR'], row['BARCOD'], country, isg, ))

        
        conn.commit()
        conn.close()

    def count_and_save_by_country(self, output_file):
        # Подсчет количества товаров по каждой стране
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT COUNTRY.NAME_COUNTRY, COUNT(GOODS.ID_TOVAR)
            FROM COUNTRY
            LEFT JOIN GOODS ON COUNTRY.ID_COUNTRY = GOODS.ID_COUNTRY
            GROUP BY COUNTRY.NAME_COUNTRY
        ''')
        
        results = cursor.fetchall()

        
        with open(output_file, 'w', encoding='utf-8') as f:
            for result in results:
                country, count = result
                f.write(f'{country} - {count}\n')

if __name__ == "__main__":
    excel_file = "data.xlsx"
    db_file = "base.sqlite"
    output_file = "data.tsv"

    processor = DataProcessor(excel_file, db_file)
    processor.process_excel_data()
    processor.count_and_save_by_country(output_file)
