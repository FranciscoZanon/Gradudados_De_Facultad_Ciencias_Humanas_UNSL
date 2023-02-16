# -*- coding: utf-8 -*-
from asyncio.windows_events import NULL
import tabula
import configparser
import pandas as pd
import pyodbc
from datetime import date
from fast_to_sql import fast_to_sql as fts

archivo = "761 - Graduados-FCH-Definitivo--2022.pdf"
           
#---------------------------------------------------------------------------
def SQL_conexion (server, database):
    SQLConn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server} ;"
                         "Server=" + server + ";"
                         "Database=" + database + ";"
                         "Trusted_Connection=yes;")
    return SQLConn  #.cursor()
#---------------------------------------------------------------------------
cp = configparser.ConfigParser()
cp.read("config.ini")
Server_Origen = cp["DEFAULT"]["server_origen"]
Base_Origen= cp["DEFAULT"]["base_origen"]

#---------------------------------------------------------------------------
# Se conecta a SQL
Conn = SQL_conexion(Server_Origen, Base_Origen)
cursor = Conn.cursor()

cursor.execute("SET ANSI_WARNINGS  OFF")
cursor.commit()

#---------------------------------------------------------------------------

# Lee el pdf

dfs = tabula.read_pdf(archivo, pages= 'all')

print(f"Found {len(dfs)} tables")

# Inicializa las variables
pagina = 0
Egresados = pd.DataFrame (columns=['Apellido y Nombre', 'Tipo', 'Documento'])
saltos = 0

# Recorre la lista de dataframes creada (1 DF por página del pdf)
for df in dfs:
    pagina +=1
    if df.shape[1] > 1: # cantidad de columnas
        for index, row in df.iterrows(): 
            if df.shape[1] == 3:
                if pd.isna(row[0]) == False:               
                    if row[0].find ('Apellido') == -1:
                        nombre =row[0]
                        tipo = row[1]
                        doc = row[2]
                        Egresados = pd.concat ([Egresados, pd.Series([nombre, tipo, doc],index = Egresados.columns).to_frame().T])	
                        print(nombre, tipo, doc)

            else:
                if pd.isna(row[0]) == False:               
                    if row[0].find ('Apellido') == -1:
                        nombre =row[0][: row[0].find(' DNI')]
                        tipo = 'DNI'
                        doc = row[1]
                        Egresados = pd.concat ([Egresados, pd.Series([nombre, tipo, doc],index = Egresados.columns).to_frame().T])	
                        print(nombre, tipo, doc)
                          
    else:
        saltos += 1

print (f'{saltos} Paginas salteadas')

create_statement = fts.fast_to_sql(Egresados, 'dbo.EgresadosFCH', Conn, if_exists='replace')
Conn.commit()