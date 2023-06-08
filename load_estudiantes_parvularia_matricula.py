import pandas as pd
import glob
from os.path import join

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
base_folder = "estudiantes/parvularia/matricula"
# Create an empty DataFrame to store the combined data
combined_data = pd.DataFrame()

# Iterate through each file
for file_path in file_paths:
    print(file_path)
    full_path = join(base_folder, file_path)
    #print(full_path)
    # Load the file into a DataFrame
    df = pd.read_csv(full_path, sep=";", on_bad_lines="warn", low_memory=False)
    
    df = df.reindex(columns=common_columns)
    # Select the common columns from the loaded DataFrame
    selected_columns = df[common_columns]
    
    # Append the selected columns to the combined_data DataFrame
    combined_data = pd.concat([combined_data, selected_columns], ignore_index=True)

print("To save")
# Save the combined data to a new CSV file
combined_data.to_csv("combined_data.csv", index=False)
