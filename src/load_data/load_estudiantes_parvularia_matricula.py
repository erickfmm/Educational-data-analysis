import pandas as pd
import numpy as np
import glob
from os.path import join
import logging
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, LongType, DoubleType
from src.load_data.helper import to_int, to_float, clean_row_forpgsql
# Specify the files to be loaded (you can adjust the file paths and column names accordingly)
file_paths = [
    ("20201201_Educacion_parvularia_oficial_2020_20200831_WEB.csv", 2020),
    ("20211112_Educacion_parvularia_oficial_2021_20210831_WEB.csv", 2021),
    ("20221112_Educacion_parvularia_oficial_2022_20220831_WEB.csv", 2022),
    ("Educacion_Parvularia_oficial_2011_PUBL.csv", 2011),
    ("Educacion_Parvularia_oficial_2012_PUBL.csv", 2012),
    ("Educacion_Parvularia_oficial_2013_PUBL.csv", 2013),
    ("Educacion_Parvularia_oficial_2014_PUBL.csv", 2014),
    ("Educacion_Parvularia_oficial_2015_PUBL.csv", 2015),
    ("Educacion_Parvularia_oficial_2016_PUBL.csv", 2016),
    ("Educacion_Parvularia_oficial_2017_PUBL.csv", 2017),
    ("Educacion_Parvularia_oficial_2019_PUBL.csv", 2019),
    ("Educacion_parvularia_oficial_2018_PUBL.csv", 2018)
]

# Specify the common columns to be extracted from each file
common_columns = [
    "AGNO",
    "MES",
    "MRUN",
    "GEN_ALU",
    "FEC_NAC_ALU",
    "ID_ESTAB",
    "RBD",
    "ID_ESTAB_J",
    "ID_ESTAB_I",
    "NOM_ESTAB",
    "COD_REG_ESTAB",
    "COD_PRO_ESTAB",
    "COD_COM_ESTAB",
    "NOM_REG_ESTAB",
    "NOM_REG_A_ESTAB",
    "NOM_PRO_ESTAB",
    "NOM_COM_ESTAB",
    "COD_DEPROV_ESTAB",
    "NOM_DEPROV_ESTAB",
    "LATITUD",
    "LONGITUD",
    "RURAL_ESTAB",
    "ORIGEN",
    "DEPENDENCIA",
    "NIVEL1",
    "NIVEL2",
    "COD_ENSE1_M",
    "COD_GRADO_M",
    "LET_CUR_M",
    "COD_TIP_CUR_M",
    "COD_DEPE1_M",
    "COD_ENSE2_M",
    "ESTADO_ESTAB_M",
    "CORR_GRU_J",
    "LET_GRU_J",
    "COD_PROG_J",
    "DESC_PROG_J",
    "COD_NIVEL_J",
    "DESC_NIVEL_J",
    "COD_MODAL_J",
    "DESC_MODAL_J",
    "ASIS_REAL_J",
    "ASIS_POTEN_J",
    "POR_ASIS_J",
    "COD_JOR_J",
    "NOM_JOR_J",
    "DIAS_TRAB_GRUPO_J",
    "DESC_MOD_I",
    "DESC_NIV_I",
    "COD_NIVEL_I",
    "COD_GRUPO_I",
    "TIPO_SOSTENEDOR",
    "ASIS_REAL_I",
    "ASIS_POT_I",
    "POR_ASIS_I",
    "FORMAL"
]
base_folder = "datosabiertos.mineduc.cl/estudiantes/parvularia_matricula"

def get_df(conn, bd: str):
    if bd == "spark":
        schema = StructType([
            StructField("AGNO",IntegerType(), True),
            StructField("MES",IntegerType(), True),
            StructField("MRUN",IntegerType(), True),
            StructField("GEN_ALU",IntegerType(), True),
            StructField("FEC_NAC_ALU",IntegerType(), True),
            StructField("ID_ESTAB",IntegerType(), True),
            StructField("RBD",IntegerType(), True),
            StructField("ID_ESTAB_J",IntegerType(), nullable=True),
            StructField("ID_ESTAB_I",IntegerType(), nullable=True),
            StructField("NOM_ESTAB",StringType(), True),
            StructField("COD_REG_ESTAB",IntegerType(), True),
            StructField("COD_PRO_ESTAB",IntegerType(), True),
            StructField("COD_COM_ESTAB",IntegerType(), True),
            StructField("NOM_REG_ESTAB",StringType(), True),
            StructField("NOM_REG_A_ESTAB",StringType(), True),
            StructField("NOM_PRO_ESTAB",StringType(), True),
            StructField("NOM_COM_ESTAB",StringType(), True),
            StructField("COD_DEPROV_ESTAB",IntegerType(), True),
            StructField("NOM_DEPROV_ESTAB",StringType(), True),
            StructField("LATITUD",StringType(), True),
            StructField("LONGITUD",StringType(), True),
            StructField("RURAL_ESTAB",IntegerType(), True),
            StructField("ORIGEN",IntegerType(), True),
            StructField("DEPENDENCIA",IntegerType(), True),
            StructField("NIVEL1",IntegerType(), True),
            StructField("NIVEL2",IntegerType(), True),
            StructField("COD_ENSE1_M",IntegerType(), True),
            StructField("COD_GRADO_M",IntegerType(), True),
            StructField("LET_CUR_M",StringType(), True),
            StructField("COD_TIP_CUR_M",IntegerType(), True),
            StructField("COD_DEPE1_M",IntegerType(), True),
            StructField("COD_ENSE2_M",IntegerType(), True),
            StructField("ESTADO_ESTAB_M",IntegerType(), True),
            StructField("CORR_GRU_J",IntegerType(), True),
            StructField("LET_GRU_J",StringType(), True),
            StructField("COD_PROG_J",IntegerType(), True),
            StructField("DESC_PROG_J",StringType(), True),
            StructField("COD_NIVEL_J",IntegerType(), True),
            StructField("DESC_NIVEL_J",StringType(), True),
            StructField("COD_MODAL_J",IntegerType(), True),
            StructField("DESC_MODAL_J",StringType(), True),
            StructField("ASIS_REAL_J",IntegerType(), True),
            StructField("ASIS_POTEN_J",IntegerType(), True),
            StructField("POR_ASIS_J",FloatType(), True),
            StructField("COD_JOR_J",StringType(), True),
            StructField("NOM_JOR_J",StringType(), True),
            StructField("DIAS_TRAB_GRUPO_J",IntegerType(), True),
            StructField("DESC_MOD_I",StringType(), True),
            StructField("DESC_NIV_I",StringType(), True),
            StructField("COD_NIVEL_I",IntegerType(), True),
            StructField("COD_GRUPO_I",IntegerType(), True),
            StructField("TIPO_SOSTENEDOR",IntegerType(), True),
            StructField("ASIS_REAL_I",IntegerType(), True),
            StructField("ASIS_POT_I",IntegerType(), True),
            StructField("POR_ASIS_I",FloatType(), True), # FloatType
            StructField("FORMAL",IntegerType(), True)
        ])
    int_columns = [
        "AGNO",
        "MES",
        "MRUN",
        "GEN_ALU",
        "FEC_NAC_ALU",
        "ID_ESTAB",
        "RBD",
        "ID_ESTAB_J",
        "ID_ESTAB_I",
        "COD_REG_ESTAB",
        "COD_PRO_ESTAB",
        "COD_COM_ESTAB",
        "COD_DEPROV_ESTAB",
        "RURAL_ESTAB",
        "ORIGEN",
        "DEPENDENCIA",
        "NIVEL1",
        "NIVEL2",
        "COD_ENSE1_M",
        "COD_GRADO_M",
        "COD_TIP_CUR_M",
        "COD_DEPE1_M",
        "COD_ENSE2_M",
        "ESTADO_ESTAB_M",
        "CORR_GRU_J",
        "COD_PROG_J",
        "COD_NIVEL_J",
        "COD_MODAL_J",
        "ASIS_REAL_J",
        "ASIS_POTEN_J",
        "DIAS_TRAB_GRUPO_J",
        "COD_NIVEL_I",
        "COD_GRUPO_I",
        "TIPO_SOSTENEDOR",
        "ASIS_REAL_I",
        "ASIS_POT_I",
        "FORMAL"
    ]
    # Create an empty DataFrame to store the combined data
    #combined_data = pd.DataFrame()
    if bd == "spark":
        _ = conn.sql("DROP TABLE IF EXISTS estudiantes_parvularia_matricula")
    if bd == "postgres":
        cur = conn.cursor()
        _ = cur.execute("DROP TABLE IF EXISTS estudiantes_parvularia_matricula;")
        _ = cur.execute("""
        CREATE TABLE estudiantes_parvularia_matricula(
            FILE_YEAR int,
            AGNO int,
            MES int,
            MRUN int,
            GEN_ALU int,
            FEC_NAC_ALU int,
            ID_ESTAB int,
            RBD int,
            ID_ESTAB_J int,
            ID_ESTAB_I int,
            NOM_ESTAB VARCHAR(100),
            COD_REG_ESTAB int,
            COD_PRO_ESTAB int,
            COD_COM_ESTAB int,
            NOM_REG_ESTAB VARCHAR(100),
            NOM_REG_A_ESTAB VARCHAR(100),
            NOM_PRO_ESTAB VARCHAR(100),
            NOM_COM_ESTAB VARCHAR(100),
            COD_DEPROV_ESTAB int,
            NOM_DEPROV_ESTAB VARCHAR(100),
            LATITUD real,
            LONGITUD real,
            RURAL_ESTAB int,
            ORIGEN int,
            DEPENDENCIA int,
            NIVEL1 int,
            NIVEL2 int,
            COD_ENSE1_M int,
            COD_GRADO_M int,
            LET_CUR_M VARCHAR(100),
            COD_TIP_CUR_M int,
            COD_DEPE1_M int,
            COD_ENSE2_M int,
            ESTADO_ESTAB_M int,
            CORR_GRU_J int,
            LET_GRU_J VARCHAR(100),
            COD_PROG_J int,
            DESC_PROG_J VARCHAR(100),
            COD_NIVEL_J int,
            DESC_NIVEL_J VARCHAR(100),
            COD_MODAL_J int,
            DESC_MODAL_J VARCHAR(100),
            ASIS_REAL_J int,
            ASIS_POTEN_J int,
            POR_ASIS_J real,
            COD_JOR_J VARCHAR(100),
            NOM_JOR_J VARCHAR(100),
            DIAS_TRAB_GRUPO_J int,
            DESC_MOD_I VARCHAR(100),
            DESC_NIV_I VARCHAR(100),
            COD_NIVEL_I int,
            COD_GRUPO_I int,
            TIPO_SOSTENEDOR int,
            ASIS_REAL_I int,
            ASIS_POT_I int,
            POR_ASIS_I real,
            FORMAL int
        );
        """)
        conn.commit()
    # Iterate through each file
    for file_path, fileyear in file_paths:
        print(file_path)
        full_path = join(base_folder, file_path)
        #print(full_path)
        # Load the file into a DataFrame
        df = pd.read_csv(full_path, sep=";", on_bad_lines="warn", low_memory=False, encoding="iso 8859-1")
        mycols = list(df.columns)
        mycols[0] = "AGNO"
        mycols = [x.upper() for x in mycols]
        df.columns = mycols
        print("to reindex")
        df = df.reindex(columns=common_columns)
        # Select the common columns from the loaded DataFrame
        df = df[common_columns]
        print("to cast")
        
        for col in int_columns:
            df[col] = df[col].apply(to_int).astype('Int64')
        df["POR_ASIS_J"] = df["POR_ASIS_J"].apply(to_float).astype('Float64')
        df["POR_ASIS_I"] = df["POR_ASIS_I"].apply(to_float).astype('Float64')
        df["LATITUD"] = df["LATITUD"].apply(to_float).astype('Float64')
        df["LONGITUD"] = df["LONGITUD"].apply(to_float).astype('Float64')
        
        if bd == "spark":
            print("to create spark")
            sdf = conn.createDataFrame(data=df, schema=schema)
            sdf.printSchema()
            print("to write")
            sdf.write.mode('append').format('hive').saveAsTable('estudiantes_parvularia_matricula')
        if bd == "postgres":
            print("to insert postgres")
            print(len(df))
            i_rows = 0
            for index, row in df.iterrows():
                mirow = clean_row_forpgsql(row)
                mirow["FILE_YEAR"] = fileyear
                #print(mirow)
                miinsert = f'\
                INSERT INTO estudiantes_parvularia_matricula(FILE_YEAR,AGNO,MES,MRUN,GEN_ALU,FEC_NAC_ALU,ID_ESTAB,RBD,ID_ESTAB_J,ID_ESTAB_I,NOM_ESTAB,COD_REG_ESTAB,COD_PRO_ESTAB,COD_COM_ESTAB,NOM_REG_ESTAB,NOM_REG_A_ESTAB,NOM_PRO_ESTAB,NOM_COM_ESTAB,COD_DEPROV_ESTAB,NOM_DEPROV_ESTAB,LATITUD,LONGITUD,RURAL_ESTAB,ORIGEN,DEPENDENCIA,NIVEL1,NIVEL2,COD_ENSE1_M,COD_GRADO_M,LET_CUR_M,COD_TIP_CUR_M,COD_DEPE1_M,COD_ENSE2_M,ESTADO_ESTAB_M,CORR_GRU_J,LET_GRU_J,COD_PROG_J,DESC_PROG_J,COD_NIVEL_J,DESC_NIVEL_J,COD_MODAL_J,DESC_MODAL_J,ASIS_REAL_J,ASIS_POTEN_J,POR_ASIS_J,COD_JOR_J,NOM_JOR_J,DIAS_TRAB_GRUPO_J,DESC_MOD_I,DESC_NIV_I,COD_NIVEL_I,COD_GRUPO_I,TIPO_SOSTENEDOR,ASIS_REAL_I,ASIS_POT_I,POR_ASIS_I,FORMAL) VALUES(\
{mirow["FILE_YEAR"]},\
{mirow["AGNO"]},\
                    {mirow["MES"]},\
                    {mirow["MRUN"]},\
                    {mirow["GEN_ALU"]},\
                    {mirow["FEC_NAC_ALU"]},\
                    {mirow["ID_ESTAB"]},\
                    {mirow["RBD"]},\
                    {mirow["ID_ESTAB_J"]},\
                    {mirow["ID_ESTAB_I"]},\
                    {mirow["NOM_ESTAB"]},\
                    {mirow["COD_REG_ESTAB"]},\
                    {mirow["COD_PRO_ESTAB"]},\
                    {mirow["COD_COM_ESTAB"]},\
                    {mirow["NOM_REG_ESTAB"]},\
                    {mirow["NOM_REG_A_ESTAB"]},\
                    {mirow["NOM_PRO_ESTAB"]},\
                    {mirow["NOM_COM_ESTAB"]},\
                    {mirow["COD_DEPROV_ESTAB"]},\
                    {mirow["NOM_DEPROV_ESTAB"]},\
                    {mirow["LATITUD"]},\
                    {mirow["LONGITUD"]},\
                    {mirow["RURAL_ESTAB"]},\
                    {mirow["ORIGEN"]},\
                    {mirow["DEPENDENCIA"]},\
                    {mirow["NIVEL1"]},\
                    {mirow["NIVEL2"]},\
                    {mirow["COD_ENSE1_M"]},\
                    {mirow["COD_GRADO_M"]},\
                    {mirow["LET_CUR_M"]},\
                    {mirow["COD_TIP_CUR_M"]},\
                    {mirow["COD_DEPE1_M"]},\
                    {mirow["COD_ENSE2_M"]},\
                    {mirow["ESTADO_ESTAB_M"]},\
                    {mirow["CORR_GRU_J"]},\
                    {mirow["LET_GRU_J"]},\
                    {mirow["COD_PROG_J"]},\
                    {mirow["DESC_PROG_J"]},\
                    {mirow["COD_NIVEL_J"]},\
                    {mirow["DESC_NIVEL_J"]},\
                    {mirow["COD_MODAL_J"]},\
                    {mirow["DESC_MODAL_J"]},\
                    {mirow["ASIS_REAL_J"]},\
                    {mirow["ASIS_POTEN_J"]},\
                    {mirow["POR_ASIS_J"]},\
                    {mirow["COD_JOR_J"]},\
                    {mirow["NOM_JOR_J"]},\
                    {mirow["DIAS_TRAB_GRUPO_J"]},\
                    {mirow["DESC_MOD_I"]},\
                    {mirow["DESC_NIV_I"]},\
                    {mirow["COD_NIVEL_I"]},\
                    {mirow["COD_GRUPO_I"]},\
                    {mirow["TIPO_SOSTENEDOR"]},\
                    {mirow["ASIS_REAL_I"]},\
                    {mirow["ASIS_POT_I"]},\
                    {mirow["POR_ASIS_I"]},\
                    {mirow["FORMAL"]}\
                );\
                '
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

    #print("To save")
    # Save the combined data to a new CSV file
    #combined_data.to_csv("combined_data.csv", index=False)
    return #combined_data