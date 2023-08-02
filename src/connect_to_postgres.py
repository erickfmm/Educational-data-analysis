import psycopg2

def connect(args):
    return psycopg2.connect(dbname="postgres", user="postgres", password="mysecretpassword", host="localhost", port=5432)