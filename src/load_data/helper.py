import numpy as np
DEFAULT_VALUE_INT = -1
DEFAULT_VALUE_FLOAT = -1.0

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
