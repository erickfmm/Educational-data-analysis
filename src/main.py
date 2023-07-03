import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import argparse
import logging
from datetime import datetime
from pytz import timezone 
from src import connect_to_spark

import pandas as pd

from src.load_data import load_estudiantes_parvularia_matricula as parvularia_matricula_df

def setup_custom_logger(name: str, t_stamp: str) -> logging.Logger:
        _nameLogFile = f'./{t_stamp}_LOG.txt'
        stream_formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(module)s, %(lineno)d, %(funcName)s, %(message)s')
        file_formatter=logging.Formatter(
        '{"time": "%(asctime)s", "name": "%(name)s", "module": "%(module)s", "funcName": "%(funcName)s", \
        "lineno": "%(lineno)d", "levelname": "%(levelname)s", "message": "%(message)s", "pathname": "%(pathname)s"}'
        )
        handlerStream = logging.StreamHandler(); 
        handlerStream.setFormatter(stream_formatter)

        handlerFile = logging.FileHandler(_nameLogFile, mode='w', encoding='utf8')
        handlerFile.setFormatter(file_formatter)

        logger = logging.getLogger(name)
        _logginLevel = logging.DEBUG if "--debug" in sys.argv or "-d" in sys.argv else logging.DEBUG#INFO
        logger.setLevel(_logginLevel)
        logger.addHandler(handlerStream)
        logger.addHandler(handlerFile)
        return logger


def download_insert(args):        
        logger = logging.getLogger('root')
        logger.info(f"Iniciando create: args")
        spark = connect_to_spark.connect(args)
        #parvularia_df = 
        parvularia_matricula_df.get_df(spark)

        #_ = spark.sql("DROP TABLE IF EXISTS table_test_1")
        #df = spark.createDataFrame(parvularia_df)
        #df = spark.createDataFrame([
        #        (100, "Hyukjin Kwon"), (120, "Hyukjin Kwon"), (140, "Haejoon Lee")],
        #        schema=["age", "name"])
        #df.write.saveAsTable("estudiantes_parvularia_matricula")
        print("Tables:")
        print(spark.catalog.listTables())
        #_ = spark.sql("DROP TABLE table_test_1")
        #print("Tables:")
        #print(spark.catalog.listTables())


if __name__ == "__main__":
        print("holi")
        t_stamp = str(int(datetime.timestamp(datetime.now(timezone('Chile/Continental')))))
        logger = setup_custom_logger('root', t_stamp)

        parser = argparse.ArgumentParser(description='Analizador de datos educacionales')
        subparsers = parser.add_subparsers(
                help='Comandos permitdos en el  sistema ')
        parser_create = subparsers.add_parser(
                'insert', help='Inserta los datos a Spark')
        parser_create.set_defaults(func=download_insert)
        args = parser.parse_args()

        if('func' in args): 
                args.func(args); #Ejecuta la función por defecto
        else:
                logger.info(parser.format_help())

