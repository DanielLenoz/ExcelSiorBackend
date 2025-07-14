import pandas as pd
import os

base_path_original = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/NOTIFICACIONES_DIAN/ORIGINAL/")
base_path_limpio = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/NOTIFICACIONES_DIAN/CAMBIO_FORMATO_COLUMNAS/")
archivo_sav = os.path.join(base_path_original, 'Consolidado2017-2019_ent.sav')
archivo_csv = os.path.join(base_path_limpio, 'Consolidado2017-2019_ent.csv')

if os.path.exists(archivo_sav):
    print(f"El archivo .sav existe en: {archivo_sav}")
    try:
        df = pd.read_spss(archivo_sav)
        df.to_csv(archivo_csv, index=False, sep='|')
        print(f'Archivo .sav convertido a CSV y guardado en: {archivo_csv}')
    except Exception as e:
        print(f"Error al leer el archivo .sav: {e}")
else:
    print(f"El archivo .sav no existe en: {archivo_sav}")



