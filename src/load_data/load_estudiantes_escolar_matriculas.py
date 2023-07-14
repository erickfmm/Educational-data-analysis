import pandas as pd
import numpy as np
from os.path import join
import logging
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, LongType, DoubleType
from src.load_data.helper import to_int, to_float, clean_row_forpgsql

BASE_FOLDER = "datosabiertos.mineduc.cl/estudiantes/escolar_matricula"

FILES_CSV = [
("20140805_matricula_unica_2004_20040430_PUBL.csv", 2004),
("20140805_matricula_unica_2005_20050430_PUBL.csv", 2005),
("20140805_matricula_unica_2006_20060430_PUBL.csv", 2006),
("20140805_matricula_unica_2007_20070430_PUBL.csv", 2007),
("20140805_matricula_unica_2008_20080430_PUBL.csv", 2008),
("20140805_matricula_unica_2009_20090430_PUBL.csv", 2009),
("20130904_matricula_unica_2010_20100430_PUBL.csv", 2010),
("20140812_matricula_unica_2011_20110430_PUBL.csv", 2011),
("20140812_matricula_unica_2012_20120430_PUBL.csv", 2012),
("20140808_matricula_unica_2013_20130430_PUBL.csv", 2013),
("20140924_matricula_unica_2014_20140430_PUBL.csv", 2014),
("20150923_matricula_unica_2015_20150430_PUBL.CSV", 2015),
("20160926_matricula_unica_2016_20160430_PUBL.csv", 2016),
("20170921_matricula_unica_2017_20170430_PUBL.csv", 2017),
("20181005_Matr", 2018),
("20191028_Matr", 2019),
("20200921_Matr", 2020),
("20210913_Matr", 2021),
("20220908_Matr", 2022)
]


COMMON_COLUMNS = [
    "AGNO",
"RBD",
"DGV_RBD",
"NOM_RBD",
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
"ESTADO_ESTAB",
"COD_ENSE",
"COD_ENSE2",
"COD_ENSE3",
"COD_GRADO",
"COD_GRADO2",
"LET_CUR",
"COD_JOR",
"COD_TIP_CUR",
"COD_DES_CUR",
"MRUN",
"GEN_ALU",
"FEC_NAC_ALU",
"EDAD_ALU",
"COD_REG_ALU",
"COD_COM_ALU",
"NOM_COM_ALU",
"COD_SEC",
"COD_ESPE",
"COD_RAMA",
"COD_MEN",
"ENS"
]

def insert_df(conn, bd: str):
    schema = StructType([
    ])
    int_columns = [
        "AGNO",
        "RBD",
        "DGV_RBD",
        #"NOM_RBD",
        "COD_REG_RBD",
        #"NOM_REG_RBD_A",
        "COD_PRO_RBD",
        "COD_COM_RBD",
        #"NOM_COM_RBD",
        "COD_DEPROV_RBD",
        #"NOM_DEPROV_RBD",
        "COD_DEPE",
        "COD_DEPE2",
        "RURAL_RBD",
        "ESTADO_ESTAB",
        "COD_ENSE",
        "COD_ENSE2",
        "COD_ENSE3",
        "COD_GRADO",
        "COD_GRADO2",
        "LET_CUR",
        "COD_JOR",
        "COD_TIP_CUR",
        "COD_DES_CUR",
        "MRUN",
        "GEN_ALU",
        "FEC_NAC_ALU",
        "EDAD_ALU",
        "COD_REG_ALU",
        "COD_COM_ALU",
        #"NOM_COM_ALU",
        "COD_SEC",
        "COD_ESPE",
        "COD_RAMA",
        "COD_MEN",
        "ENS"
    ]
    if bd == "spark":
        _ = conn.sql("DROP TABLE IF EXISTS estudiantes_escolar_matricula")
    if bd == "postgres":
        cur = conn.cursor()
        _ = cur.execute("DROP TABLE IF EXISTS estudiantes_escolar_matricula;")
        _ = cur.execute("""CREATE TABLE estudiantes_escolar_matricula(
                        FILE_YEAR int,
                AGNO int,
                RBD int,
                DGV_RBD int,
                NOM_RBD VARCHAR(100),
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
                ESTADO_ESTAB int,
                COD_ENSE int,
                COD_ENSE2 int,
                COD_ENSE3 int,
                COD_GRADO int,
                COD_GRADO2 int,
                LET_CUR int,
                COD_JOR int,
                COD_TIP_CUR int,
                COD_DES_CUR int,
                MRUN int,
                GEN_ALU int,
                FEC_NAC_ALU int,
                EDAD_ALU int,
                COD_REG_ALU int,
                COD_COM_ALU int,
                NOM_COM_ALU VARCHAR(100),
                COD_SEC int,
                COD_ESPE int,
                COD_RAMA int,
                COD_MEN int,
                ENS int
        );""")
        conn.commit()
    for file_path, fileyear in FILES_CSV:
        file_path : str = file_path
        print(file_path)
        full_path = join(BASE_FOLDER, file_path)
        #print(full_path)
        # Load the file into a DataFrame
        chunks = pd.read_csv(full_path, sep=";", on_bad_lines="warn", low_memory=False, chunksize=10**5, encoding="iso 8859-1")
        i_chunk = 0
        for df in chunks:
            print("chunk ", i_chunk)
            i_chunk += 1
            print("to reindex")
            mycols = []
            for name in df.columns:
                mycols.append(name.upper())
            df.columns = mycols
            df = df.reindex(columns=COMMON_COLUMNS)
            df = df[COMMON_COLUMNS]

            for col in int_columns:
                df[col] = df[col].apply(to_int).astype('Int64')

            if bd == "spark":
                print("to create spark")
                sdf = conn.createDataFrame(data=df, schema=schema)
                sdf.printSchema()
                print("to write")
                #sdf.write.mode('append').saveAsTable('estudiantes_parvularia_matricula')
                sdf.write.mode('append').format('hive').saveAsTable('estudiantes_escolar_matricula')
            if bd == "postgres":
                print(len(df))
                i_rows = 0
                for index, row in df.iterrows():
                    mirow = clean_row_forpgsql(row)
                    mirow["FILE_YEAR"] = fileyear
                    miinsert = f'INSERT INTO estudiantes_escolar_matricula(FILE_YEAR,AGNO,RBD,DGV_RBD,NOM_RBD,COD_REG_RBD,NOM_REG_RBD_A,COD_PRO_RBD,COD_COM_RBD,NOM_COM_RBD,COD_DEPROV_RBD,NOM_DEPROV_RBD,COD_DEPE,COD_DEPE2,RURAL_RBD,ESTADO_ESTAB,COD_ENSE,COD_ENSE2,COD_ENSE3,COD_GRADO,COD_GRADO2,LET_CUR,COD_JOR,COD_TIP_CUR,COD_DES_CUR,MRUN,GEN_ALU,FEC_NAC_ALU,EDAD_ALU,COD_REG_ALU,COD_COM_ALU,NOM_COM_ALU,COD_SEC,COD_ESPE,COD_RAMA,COD_MEN,ENS) VALUES(\
        {mirow["FILE_YEAR"]},\
{mirow["AGNO"]},\
    {mirow["RBD"]},\
    {mirow["DGV_RBD"]},\
    {mirow["NOM_RBD"]},\
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
    {mirow["ESTADO_ESTAB"]},\
    {mirow["COD_ENSE"]},\
    {mirow["COD_ENSE2"]},\
    {mirow["COD_ENSE3"]},\
    {mirow["COD_GRADO"]},\
    {mirow["COD_GRADO2"]},\
    {mirow["LET_CUR"]},\
    {mirow["COD_JOR"]},\
    {mirow["COD_TIP_CUR"]},\
    {mirow["COD_DES_CUR"]},\
    {mirow["MRUN"]},\
    {mirow["GEN_ALU"]},\
    {mirow["FEC_NAC_ALU"]},\
    {mirow["EDAD_ALU"]},\
    {mirow["COD_REG_ALU"]},\
    {mirow["COD_COM_ALU"]},\
    {mirow["NOM_COM_ALU"]},\
    {mirow["COD_SEC"]},\
    {mirow["COD_ESPE"]},\
    {mirow["COD_RAMA"]},\
    {mirow["COD_MEN"]},\
    {mirow["ENS"]}\
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
                        print("file: ",file_path,", chunk: ", i_chunk, ", rows: ", i_rows)
                        conn.commit()