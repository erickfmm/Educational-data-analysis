import pandas as pd
import numpy as np
from os.path import join
import logging
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, LongType, DoubleType
from src.load_data.helper import to_int, to_float

BASE_FOLDER = "datosabiertos.mineduc.cl\establecimientos\directorio_establecimientos"

FILES_CSV = [
    "2004.csv",
    "2005.csv",
    "2006.csv",
    "2007.csv",
    "2008.csv",
    "2009.csv",
    "2010.csv",
    "2011.csv",
    "2012.csv",
    "Directorio_oficial_EE_2013.csv",
    "Directorio_oficial_EE_2014.csv",
    "Directorio_oficial_EE_2015.csv",
    "Directorio_oficial_EE_2016.csv",
    "Directorio_oficial_EE_2017.csv",
    "Directorio_oficial_EE_2018.csv",
    "Directorio_oficial_EE_2019.csv",
    "Directorio_oficial_EE_2020.csv",
    "Directorio_oficial_EE_2021.csv",
    "20220914_Directorio_Oficial_EE_2022_20220430_WEB.csv"
]

FILES_XLS = [
    "Directorio 1992.xls",
    "Directorio 1993.xls",
    "Directorio 1994.xls",
    "Directorio 1995.xls",
    "Directorio 1996.xls",
    "Directorio 1997.xls",
    "Directorio 1998.xls",
    "Directorio 1999.xls",
    "Directorio 2000.xls",
    "Directorio 2001.xls",
    "Directorio 2002.xls",
    "Directoriooficial2003/directorio 2003.xlsx"
]

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
    "PAGO_MENSUAL"
]

def insert_df(spark):
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
        "ORI_RELIGIOSA"
    ]
    _ = spark.sql("DROP TABLE IF EXISTS establecimientos_directorio_establecimientos")
    for file_path in FILES_CSV.extend(FILES_XLS):
        file_path : str = file_path
        print(file_path)
        full_path = join(BASE_FOLDER, file_path)
        #print(full_path)
        # Load the file into a DataFrame
        if file_path.endswith(".csv"):
            df = pd.read_csv(full_path, sep=";", on_bad_lines="warn", low_memory=False)
        if file_path.endswith(".xls") or file_path.endswith(".xlsx"):
            df = pd.read_excel(full_path, 0)
        print("to reindex")
        df = df.reindex(columns=COMMON_COLUMNS)
        df = df[COMMON_COLUMNS]

        for col in int_columns:
            df[col] = df[col].apply(to_int).astype('Int64')

        print("to create spark")
        sdf = spark.createDataFrame(data=df, schema=schema)
        sdf.printSchema()
        print("to write")
        #sdf.write.mode('append').saveAsTable('estudiantes_parvularia_matricula')
        sdf.write.mode('append').format('hive').saveAsTable('establecimientos_directorio_establecimientos')