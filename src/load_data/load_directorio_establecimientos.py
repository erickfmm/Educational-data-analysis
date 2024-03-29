import pandas as pd
import numpy as np
from os.path import join
import logging
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, LongType, DoubleType
from src.load_data.helper import to_int, to_float, clean_row_forpgsql

BASE_FOLDER = "datosabiertos.mineduc.cl/establecimientos/directorio_establecimientos"

FILES_CSV = [
    ("2004.csv", 2004),
    ("2005.csv", 2005),
    ("2006.csv", 2006),
    ("2007.csv", 2007),
    ("2008.csv", 2008),
    ("2009.csv", 2009),
    ("2010.csv", 2010),
    ("2011.csv", 2011),
    ("2012.csv", 2012),
    ("Directorio_oficial_EE_2013.csv", 2013),
    ("Directorio_oficial_EE_2014.csv", 2014),
    ("Directorio_oficial_EE_2015.csv", 2015),
    ("Directorio_oficial_EE_2016.csv", 2016),
    ("Directorio_oficial_EE_2017.csv", 2017),
    ("Directorio_oficial_EE_2018.csv", 2018),
    ("Directorio_oficial_EE_2019.csv", 2019),
    ("Directorio_oficial_EE_2020.csv", 2020),
    ("Directorio_oficial_EE_2021.csv", 2021),
    ("20220914_Directorio_Oficial_EE_2022_20220430_WEB.csv", 2022)
]

FILES_XLS = [
    ("Directorio 1992.xls", 1992),
    ("Directorio 1993.xls", 1993),
    ("Directorio 1994.xls", 1994),
    ("Directorio 1995.xls", 1995),
    ("Directorio 1996.xls", 1996),
    ("Directorio 1997.xls", 1997),
    ("Directorio 1998.xls", 1998),
    ("Directorio 1999.xls", 1999),
    ("Directorio 2000.xls", 2000),
    ("Directorio 2001.xls", 2001),
    ("Directorio 2002.xls", 2002),
    ("Directoriooficial2003/directorio 2003.xlsx", 2003)
]
XLS_COLUMNS = {"1992": ["RBD","DGV_RBD","NOM_RBD","LET_RBD","NUM_RBD","COD_DEPE","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","NOM_LOCALIDAD","COD_AREA","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ENS_10","NADA1","ESTADO_ESTAB"],
"1993": ["RBD","DGV_RBD","NOM_RBD","LET_RBD","NUM_RBD","COD_DEPE","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","NOM_LOCALIDAD","COD_AREA","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ENS_10","ESTADO_ESTAB"],
"1994": ["RBD","DGV_RBD","COD_AREA","COD_DEPE","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","LET_RBD","NUM_RBD","NOM_RBD","NOM_LOCALIDAD","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ESTADO_ESTAB"],
"1995": ["RBD","DGV_RBD","NOM_RBD","LET_RBD","NUM_RBD","COD_DEPE","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","NOM_LOCALIDAD","COD_AREA","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ENS_10","NADA1","ESTADO_ESTAB"],
"1996": ["RBD","DGV_RBD","NOM_RBD","LET_RBD","NUM_RBD","COD_DEPE","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","NOM_LOCALIDAD","COD_AREA","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ENS_10","ESTADO_ESTAB"],
"1997": ["RBD","DGV_RBD","NOM_RBD","LET_RBD","NUM_RBD","COD_DEPE","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","COD_DEPROV_RBD","NOM_LOCALIDAD","COD_AREA","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ENS_10","ESTADO_ESTAB"],
"1998": ["RBD","DGV_RBD","NOM_RBD","LET_RBD","NUM_RBD","COD_DEPE","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","COD_DEPROV_RBD","NOM_LOCALIDAD","COD_AREA","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ENS_10","ESTADO_ESTAB"],
"1999": ["RBD","DGV_RBD","NOM_RBD","LET_RBD","NUM_RBD","COD_DEPE","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","COD_DEPROV_RBD","NOM_LOCALIDAD","COD_AREA","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ENS_10","ESTADO_ESTAB"],
"2000": ["AGNO","RBD","DGV_RBD","NOM_RBD","LET_RBD","NUM_RBD","COD_DEPE","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","NOM_COM_RBD","NOM_LOCALIDAD","COD_AREA","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ENS_10","ESTADO_ESTAB"],
"2001": ["AGNO","RBD","DGV_RBD","NOM_RBD","LET_RBD","NUM_RBD","COD_DEPE","NOM_DIRECTOR","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","NOM_COM_RBD","NOM_LOCALIDAD","COD_AREA","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ENS_10","ESTADO_ESTAB"],
"2002": ["AGNO","RBD","DGV_RBD","NOM_RBD","LET_RBD","NUM_RBD","COD_DEPE","NOM_DIRECTOR","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","NOM_COM_RBD","NOM_LOCALIDAD","COD_AREA","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ENS_10","ESTADO_ESTAB"],
"2003": ["AGNO","RBD","DGV_RBD","NOM_RBD","LET_RBD","NUM_RBD","COD_DEPE","NOM_DIRECTOR","COD_REG_RBD","COD_PRO_RBD","COD_COM_RBD","NOM_COM_RBD","NOM_LOCALIDAD","COD_AREA","ENS_01","ENS_02","ENS_03","ENS_04","ENS_05","ENS_06","ENS_07","ENS_08","ENS_09","ENS_10","ESTADO_ESTAB"]}

COMMON_COLUMNS = [
    "AGNO",
    "RBD",
    "DGV_RBD",
    "NOM_RBD",
    "LET_RBD",
    "NUM_RBD",
    "MRUN",
    "RUT_SOSTENEDOR",
    "P_JURIDICA",
    "COD_REG_RBD",
    "NOM_REG_RBD_A",
    "COD_PRO_RBD",
    "COD_COM_RBD",
    "NOM_COM_RBD",
    "COD_DEPROV_RBD",
    "NOM_DEPROV_RBD",
    "COD_DEPE",
    "COD_DEPE2",
    "RURAL_RBD",
    "LATITUD",
    "LONGITUD",
    "CONVENIO_PIE",
    "PACE",
    "ENS_01",
    "ENS_02",
    "ENS_03",
    "ENS_04",
    "ENS_05",
    "ENS_06",
    "ENS_07",
    "ENS_08",
    "ENS_09",
    "ENS_10",
    "ENS_11",
    "ENS_12",
    "MAT_TOTAL",
    "MATRICULA",
    "ESTADO_ESTAB",
    "ORI_RELIGIOSA",
    "ORI_OTRO_GLOSA",
    "PAGO_MATRICULA",
    "PAGO_MENSUAL",
    "NOM_DIRECTOR",
    "NOM_LOCALIDAD",
    "COD_AREA"
]


def insert_df(conn, bd: str):
    schema = StructType([
    StructField("AGNO",IntegerType(), True),
    StructField("RBD",IntegerType(), True),
    StructField("DGV_RBD",IntegerType(), True),
    StructField("NOM_RBD",StringType(), True),
    StructField("LET_RBD",StringType(), True),
    StructField("NUM_RBD",IntegerType(), True),
    StructField("MRUN",IntegerType(), True),
    StructField("RUT_SOSTENEDOR",IntegerType(), True),
    StructField("P_JURIDICA",IntegerType(), True),
    StructField("COD_REG_RBD",IntegerType(), True),
    StructField("NOM_REG_RBD_A",StringType(), True),
    StructField("COD_PRO_RBD",IntegerType(), True),
    StructField("COD_COM_RBD",IntegerType(), True),
    StructField("NOM_COM_RBD",StringType(), True),
    StructField("COD_DEPROV_RBD",IntegerType(), True),
    StructField("NOM_DEPROV_RBD",StringType(), True),
    StructField("COD_DEPE",IntegerType(), True),
    StructField("COD_DEPE2",IntegerType(), True),
    StructField("RURAL_RBD",IntegerType(), True),
    StructField("LATITUD",StringType(), True),
    StructField("LONGITUD",StringType(), True),
    StructField("CONVENIO_PIE",IntegerType(), True),
    StructField("PACE",IntegerType(), True),
    StructField("ENS_01",IntegerType(), True),
    StructField("ENS_02",IntegerType(), True),
    StructField("ENS_03",IntegerType(), True),
    StructField("ENS_04",IntegerType(), True),
    StructField("ENS_05",IntegerType(), True),
    StructField("ENS_06",IntegerType(), True),
    StructField("ENS_07",IntegerType(), True),
    StructField("ENS_08",IntegerType(), True),
    StructField("ENS_09",IntegerType(), True),
    StructField("ENS_10",IntegerType(), True),
    StructField("ENS_11",IntegerType(), True),
    StructField("ENS_12",IntegerType(), True),
    StructField("MAT_TOTAL",IntegerType(), True),
    StructField("MATRICULA",IntegerType(), True),
    StructField("ESTADO_ESTAB",IntegerType(), True),
    StructField("ORI_RELIGIOSA",IntegerType(), True),
    StructField("ORI_OTRO_GLOSA",StringType(), True),
    StructField("PAGO_MATRICULA",StringType(), True),
    StructField("PAGO_MENSUAL",StringType(), True)
    ])
    int_columns = [
        "AGNO",
        "RBD",
        "DGV_RBD",
        "NUM_RBD",
        "MRUN",
        "RUT_SOSTENEDOR",
        "P_JURIDICA",
        "COD_REG_RBD",
        "COD_PRO_RBD",
        "COD_COM_RBD",
        "COD_DEPROV_RBD",
        "COD_DEPE",
        "COD_DEPE2",
        "RURAL_RBD",
        "CONVENIO_PIE",
        "PACE",
        "ENS_01",
        "ENS_02",
        "ENS_03",
        "ENS_04",
        "ENS_05",
        "ENS_06",
        "ENS_07",
        "ENS_08",
        "ENS_09",
        "ENS_10",
        "ENS_11",
        "ENS_12",
        "MAT_TOTAL",
        "MATRICULA",
        "ESTADO_ESTAB",
        "ORI_RELIGIOSA",
        "COD_AREA"
    ]
    if bd == "spark":
        _ = conn.sql("DROP TABLE IF EXISTS establecimientos_directorio_establecimientos")
    if bd == "postgres":
        cur = conn.cursor()
        _ = cur.execute("DROP TABLE IF EXISTS establecimientos_directorio_establecimientos;")
        _ = cur.execute("""CREATE TABLE establecimientos_directorio_establecimientos(
                    FILE_YEAR int,
                        AGNO int,
RBD int,
DGV_RBD int,
NOM_RBD VARCHAR(255),
LET_RBD VARCHAR(100),
NUM_RBD int,
MRUN int,
RUT_SOSTENEDOR int,
P_JURIDICA int,
COD_REG_RBD int,
NOM_REG_RBD_A VARCHAR(100),
COD_PRO_RBD int,
COD_COM_RBD int,
NOM_COM_RBD VARCHAR(100),
COD_DEPROV_RBD int,
NOM_DEPROV_RBD VARCHAR(100),
COD_DEPE int,
COD_DEPE2 int,
RURAL_RBD int,
LATITUD real,
LONGITUD real,
CONVENIO_PIE int,
PACE int,
ENS_01 int,
ENS_02 int,
ENS_03 int,
ENS_04 int,
ENS_05 int,
ENS_06 int,
ENS_07 int,
ENS_08 int,
ENS_09 int,
ENS_10 int,
ENS_11 int,
ENS_12 int,
MAT_TOTAL int,
MATRICULA int,
ESTADO_ESTAB int,
ORI_RELIGIOSA int,
ORI_OTRO_GLOSA VARCHAR(100),
PAGO_MATRICULA VARCHAR(100),
PAGO_MENSUAL    VARCHAR(100),
NOM_DIRECTOR VARCHAR(100),
NOM_LOCALIDAD VARCHAR(100),
COD_AREA int
        )""")
        conn.commit()
        FILES_CSV.extend(FILES_XLS)
        list_files = FILES_CSV
    for file_path, fileyear in list_files:
        file_path : str = file_path
        print(file_path)
        full_path = join(BASE_FOLDER, file_path)
        #print(full_path)
        # Load the file into a DataFrame
        if file_path.endswith(".csv"):
            df = pd.read_csv(full_path, sep=";", on_bad_lines="warn", low_memory=False, encoding="iso 8859-1")
            mycols = list(df.columns)
            mycols[0] = "AGNO"
            mycols = [x.upper() for x in mycols]
            df.columns = mycols
        if file_path.endswith(".xls") or file_path.endswith(".xlsx"):
            cols = XLS_COLUMNS[str(fileyear)]
            df = pd.read_excel(full_path, 0)
            df.columns = cols
        print("to reindex")
        df = df.reindex(columns=COMMON_COLUMNS)
        df = df[COMMON_COLUMNS]

        for col in int_columns:
            df[col] = df[col].apply(to_int).astype('Int64')
        df["LATITUD"] = df["LATITUD"].apply(to_float).astype('Float64')
        df["LONGITUD"] = df["LONGITUD"].apply(to_float).astype('Float64')

        if bd == "spark":
            print("to create spark")
            sdf = conn.createDataFrame(data=df, schema=schema)
            sdf.printSchema()
            print("to write")
            #sdf.write.mode('append').saveAsTable('estudiantes_parvularia_matricula')
            sdf.write.mode('append').format('hive').saveAsTable('establecimientos_directorio_establecimientos')
        if bd == "postgres":
            print(len(df))
            i_rows = 0
            for index, row in df.iterrows():
                mirow = clean_row_forpgsql(row)
                mirow["FILE_YEAR"] = fileyear
                miinsert = f'INSERT INTO establecimientos_directorio_establecimientos(FILE_YEAR, AGNO,RBD,DGV_RBD,NOM_RBD,LET_RBD,NUM_RBD,MRUN,RUT_SOSTENEDOR,P_JURIDICA,COD_REG_RBD,NOM_REG_RBD_A,COD_PRO_RBD,COD_COM_RBD,NOM_COM_RBD,COD_DEPROV_RBD,NOM_DEPROV_RBD,COD_DEPE,COD_DEPE2,RURAL_RBD,LATITUD,LONGITUD,CONVENIO_PIE,PACE,ENS_01,ENS_02,ENS_03,ENS_04,ENS_05,ENS_06,ENS_07,ENS_08,ENS_09,ENS_10,ENS_11,ENS_12,MAT_TOTAL,MATRICULA,ESTADO_ESTAB,ORI_RELIGIOSA,ORI_OTRO_GLOSA,PAGO_MATRICULA,PAGO_MENSUAL,NOM_DIRECTOR,NOM_LOCALIDAD,COD_AREA) VALUES(\
{mirow["FILE_YEAR"]},\
{mirow["AGNO"]},\
    {mirow["RBD"]},\
    {mirow["DGV_RBD"]},\
    {mirow["NOM_RBD"]},\
    {mirow["LET_RBD"]},\
    {mirow["NUM_RBD"]},\
    {mirow["MRUN"]},\
    {mirow["RUT_SOSTENEDOR"]},\
    {mirow["P_JURIDICA"]},\
    {mirow["COD_REG_RBD"]},\
    {mirow["NOM_REG_RBD_A"]},\
    {mirow["COD_PRO_RBD"]},\
    {mirow["COD_COM_RBD"]},\
    {mirow["NOM_COM_RBD"]},\
    {mirow["COD_DEPROV_RBD"]},\
    {mirow["NOM_DEPROV_RBD"]},\
    {mirow["COD_DEPE"]},\
    {mirow["COD_DEPE2"]},\
    {mirow["RURAL_RBD"]},\
    {mirow["LATITUD"]},\
    {mirow["LONGITUD"]},\
    {mirow["CONVENIO_PIE"]},\
    {mirow["PACE"]},\
    {mirow["ENS_01"]},\
    {mirow["ENS_02"]},\
    {mirow["ENS_03"]},\
    {mirow["ENS_04"]},\
    {mirow["ENS_05"]},\
    {mirow["ENS_06"]},\
    {mirow["ENS_07"]},\
    {mirow["ENS_08"]},\
    {mirow["ENS_09"]},\
    {mirow["ENS_10"]},\
    {mirow["ENS_11"]},\
    {mirow["ENS_12"]},\
    {mirow["MAT_TOTAL"]},\
    {mirow["MATRICULA"]},\
    {mirow["ESTADO_ESTAB"]},\
    {mirow["ORI_RELIGIOSA"]},\
    {mirow["ORI_OTRO_GLOSA"]},\
    {mirow["PAGO_MATRICULA"]},\
    {mirow["PAGO_MENSUAL"]},\
    {mirow["NOM_DIRECTOR"]},\
    {mirow["NOM_LOCALIDAD"]},\
    {mirow["COD_AREA"]}\
                );'
                try:
                    _ = cur.execute(miinsert)
                except Exception as e:
                    print(mirow)
                    print()
                    print(miinsert)
                    print()
                    print(e)
                    return
                i_rows += 1
                if i_rows % 1000 == 0:
                    print("file: ", file_path, ", rows: ", i_rows)
                    conn.commit()