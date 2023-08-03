import requests
import os
import sys
from os.path import dirname, join, abspath
import time

def download_PDFs_PEI(conn, bd, tipo="pei"):
    if bd == "postgres":
        cur = conn.cursor()
        _ = cur.execute("SELECT rbd FROM public.establecimientos_directorio_establecimientos WHERE file_year=2022;")
        rbds = cur.fetchall()
    elif bd == "spark":
        return
    else:
        return
    if tipo == "convivencia":
        base_url = lambda _rbd : f"https://wwwfs.mineduc.cl/Archivos/infoescuelas/documentos/{_rbd}/ReglamentodeConvivencia{_rbd}.pdf"
        folder = "files_convivencia"
        prefix = "reglamento_conv_"
    elif tipo == "pei":
        base_url = lambda _rbd : f"https://wwwfs.mineduc.cl/Archivos/infoescuelas/documentos/{_rbd}/ProyectoEducativo{_rbd}.pdf"
        folder = "files_pei"
        prefix = "proyecto_educativo_"
    elif tipo == "evaluacion":
        base_url = lambda _rbd : f"https://wwwfs.mineduc.cl/Archivos/infoescuelas/documentos/{_rbd}/ReglamentoDeEvaluacion{_rbd}.pdf"
        folder = "files_evaluacion"
        prefix = "reglamento_evaluacion_"
    else:
        print("Error no tipo encontrado")
        return
    if not os.path.exists(folder):
        os.mkdir(folder)
    for rbd in rbds:
        rbd = rbd[0]
        print(base_url(rbd))
        r = requests.get(base_url(rbd))
        if r.status_code == 200:
            fh = open(join(folder, f"{prefix}{rbd}.pdf"), "wb")
            fh.write(r.content)
            fh.flush()
            fh.close()
            time.sleep(5)
        else:
            print("Error downloading")
    print(len(rbds))
    

if __name__ == "__main__":
    sys.path.append(abspath(join(dirname(__file__), '..', '..')))
    from src import connect_to_postgres
    conn = connect_to_postgres.connect(None)
    download_PDFs_PEI(conn, "postgres")