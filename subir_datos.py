import pandas as pd
import pyodbc
from datetime import datetime

server_name = r'localhost\APPWEB'
database_name = 'appWEB'
username = 'sa'
password = '12345'

def get_db_connection():
    conn = pyodbc.connect('Driver={SQL Server};'
                          f'Server={server_name};'
                          f'Database={database_name};'
                          f'UID={username};'
                          f'PWD={password};')
    return conn

def insert_data_to_db(df, conn):
    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO appWEB.dbo.EstimadosExportable (mes, finca, estimados_tallos_exp)
            VALUES (?, ?, ?)
            """, row['mes'], row['finca'], row['estimados_tallos_exp'])
    conn.commit()
    cursor.close()

meses_map = {
    'ENERO': '01', 'FEBRERO': '02', 'MARZO': '03',
    'ABRIL': '04', 'MAYO': '05', 'JUNIO': '06',
    'JULIO': '07', 'AGOSTO': '08', 'SEPTIEMBRE': '09',
    'OCTUBRE': '10', 'NOVIEMBRE': '11', 'DICIEMBRE': '12'
}

def convertir_mes(mes_texto):
    año = '2025'
    mes_numero = meses_map[mes_texto.upper()]
    return f"{año}-{mes_numero}"

file_path = 'presupuesto.xlsx'
df = pd.read_excel(file_path)

df['mes'] = df['mes'].apply(convertir_mes)

print(df.head())

conn = get_db_connection()
insert_data_to_db(df, conn)
conn.close()