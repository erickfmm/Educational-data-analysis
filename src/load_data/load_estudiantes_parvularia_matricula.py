import pandas as pd
import numpy as np
import glob
from os.path import join
import logging
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, LongType, DoubleType
# Specify the files to be loaded (you can adjust the file paths and column names accordingly)
file_paths = [
    "20201201_Educacion_parvularia_oficial_2020_20200831_WEB.csv",
    "20211112_Educacion_parvularia_oficial_2021_20210831_WEB.csv",
    "20221112_Educacion_parvularia_oficial_2022_20220831_WEB.csv",
    "Educacion_Parvularia_oficial_2011_PUBL.csv",
    "Educacion_Parvularia_oficial_2012_PUBL.csv",
    "Educacion_Parvularia_oficial_2013_PUBL.csv",
    "Educacion_Parvularia_oficial_2014_PUBL.csv",
    "Educacion_Parvularia_oficial_2015_PUBL.csv",
    "Educacion_Parvularia_oficial_2016_PUBL.csv",
    "Educacion_Parvularia_oficial_2017_PUBL.csv",
    "Educacion_Parvularia_oficial_2019_PUBL.csv",
    "Educacion_parvularia_oficial_2018_PUBL.csv",
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

def get_df(spark) -> pd.DataFrame:
    
    schema = StructType([
        StructField("AGNO",IntegerType(), True),
        StructField("MES",IntegerType(), True),
        StructField("MRUN",IntegerType(), True),
        StructField("GEN_ALU",IntegerType(), True),
        StructField("FEC_NAC_ALU",IntegerType(), True),
        StructField("ID_ESTAB",IntegerType(), True),
        StructField("RBD",IntegerType(), True),
        StructField("ID_ESTAB_J",LongType(), nullable=True),
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
    # Create an empty DataFrame to store the combined data
    #combined_data = pd.DataFrame()

    # Iterate through each file
    for file_path in file_paths:
        print(file_path)
        full_path = join(base_folder, file_path)
        #print(full_path)
        # Load the file into a DataFrame
        df = pd.read_csv(full_path, sep=";", on_bad_lines="warn", low_memory=False)
        print("to reindex")
        df = df.reindex(columns=common_columns)
        # Select the common columns from the loaded DataFrame
        selected_columns = df[common_columns]
        def to_int(n):
            try:
                return np.int64(n)
            except:
                return -1#np.nan
        def to_float(n):
            try:
                x = np.float64(n)
                if x is np.nan or not np.isfinite(x):
                    return -1.0
                return x
            except:
                return -1.0#np.nan
        print("to cast")
        selected_columns["AGNO"] = selected_columns["AGNO"].apply(to_int).astype('Int64')
        selected_columns["RBD"] = selected_columns["RBD"].apply(to_int).astype('Int64')
        selected_columns["ID_ESTAB_J"] = selected_columns["ID_ESTAB_J"].apply(to_int).astype('Int64')
        selected_columns["ID_ESTAB_I"] = selected_columns["ID_ESTAB_I"].apply(to_int).astype('Int64')
        selected_columns["COD_DEPROV_ESTAB"] = selected_columns["COD_DEPROV_ESTAB"].apply(to_int).astype('Int64')
        selected_columns["COD_ENSE1_M"] = selected_columns["COD_ENSE1_M"].apply(to_int).astype('Int64')
        selected_columns["COD_GRADO_M"] = selected_columns["COD_GRADO_M"].apply(to_int).astype('Int64')
        selected_columns["COD_TIP_CUR_M"] = selected_columns["COD_TIP_CUR_M"].apply(to_int).astype('Int64')
        selected_columns["COD_DEPE1_M"] = selected_columns["COD_DEPE1_M"].apply(to_int).astype('Int64')
        selected_columns["COD_ENSE2_M"] = selected_columns["COD_ENSE2_M"].apply(to_int).astype('Int64')
        selected_columns["ESTADO_ESTAB_M"] = selected_columns["ESTADO_ESTAB_M"].apply(to_int).astype('Int64')
        selected_columns["CORR_GRU_J"] = selected_columns["CORR_GRU_J"].apply(to_int).astype('Int64')
        selected_columns["COD_PROG_J"] = selected_columns["COD_PROG_J"].apply(to_int).astype('Int64')
        selected_columns["COD_NIVEL_J"] = selected_columns["COD_NIVEL_J"].apply(to_int).astype('Int64')
        selected_columns["COD_MODAL_J"] = selected_columns["COD_MODAL_J"].apply(to_int).astype('Int64')
        selected_columns["ASIS_REAL_J"] = selected_columns["ASIS_REAL_J"].apply(to_int).astype('Int64')
        selected_columns["ASIS_POTEN_J"] = selected_columns["ASIS_POTEN_J"].apply(to_int).astype('Int64')
        selected_columns["DIAS_TRAB_GRUPO_J"] = selected_columns["DIAS_TRAB_GRUPO_J"].apply(to_int).astype('Int64')
        selected_columns["COD_GRUPO_I"] = selected_columns["COD_GRUPO_I"].apply(to_int).astype('Int64')
        selected_columns["COD_NIVEL_I"] = selected_columns["COD_NIVEL_I"].apply(to_int).astype('Int64')
        selected_columns["TIPO_SOSTENEDOR"] = selected_columns["TIPO_SOSTENEDOR"].apply(to_int).astype('Int64')
        selected_columns["ASIS_REAL_I"] = selected_columns["ASIS_REAL_I"].apply(to_int).astype('Int64')
        selected_columns["ASIS_POT_I"] = selected_columns["ASIS_POT_I"].apply(to_int).astype('Int64')
        selected_columns["POR_ASIS_I"] = selected_columns["POR_ASIS_I"].apply(to_float).astype('Float64')
        

        print("to create spark")
        sdf = spark.createDataFrame(data=selected_columns, schema=schema)
        sdf.printSchema()
        print("to write")
        #sdf.write.mode('append').saveAsTable('estudiantes_parvularia_matricula')
        sdf.write.mode('append').format('hive').saveAsTable('estudiantes_parvularia_matricula')
        # Append the selected columns to the combined_data DataFrame
        #combined_data = pd.concat([combined_data, selected_columns], ignore_index=True)

    #print("To save")
    # Save the combined data to a new CSV file
    #combined_data.to_csv("combined_data.csv", index=False)
    return #combined_data