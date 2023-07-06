import numpy as np
import pandas as pd
DEFAULT_VALUE_INT = np.nan#-1
DEFAULT_VALUE_FLOAT = np.nan#-1.0

def to_int(n):
    try:
        return np.int64(n)
    except:
        return DEFAULT_VALUE_INT#np.nan
def to_float(n):
    try:
        x = np.float64(n)
        if x is np.nan or not np.isfinite(x):
            return DEFAULT_VALUE_FLOAT
        return x
    except:
        return DEFAULT_VALUE_FLOAT#np.nan

def clean_row_forpgsql(row):
    mirow = {}
    for key in row.keys():
        if pd.isna(row[key]) or row[key] == " ":
            mirow[key] = "NULL"
        elif type(row[key]) == str:
            mirow[key] = str("'")+str(row[key]).replace("'", "''")+str("'")
        else:
            mirow[key] = row[key]
    return mirow