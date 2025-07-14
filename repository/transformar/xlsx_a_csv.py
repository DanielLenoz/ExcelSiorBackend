import pandas as pd
import os

base_path_original = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/NOTIFICACIONES_DIAN/ORIGINAL/CSV/INFORME_2025_ENERO_MARZO_2025/")
base_path_limpio = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/NOTIFICACIONES_DIAN/ORIGINAL/CSV/INFORME_2025_ENERO_MARZO_2025/")
archivo_xlsx = os.path.join(base_path_original, 'Mes de Agosto de 2021.xlsx')  # Reemplaza 'archivo.xlsx' con el nombre de tu archivo
archivo_csv = os.path.join(base_path_limpio, 'Mes_de_Agosto_de_2021.csv')      # Reemplaza 'archivo.csv' con el nombre deseado para el CSV

lista_archivos_xlsx = [
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_BAR.xlsx',
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_BUC.xlsx',
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_CAL.xlsx',
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_CAR.xlsx',
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_CUC.xlsx',
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_IBA.xlsx',
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_JUR.xlsx',
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_MAN.xlsx',
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_MED.xlsx',
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_PER.xlsx',
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_RES1.xlsx',
    'Informe_Notificaciones_ITRC_Libro_Radicador_20250101_20250331_RES2.xlsx',


]

for nombre_archivo_xlsx in lista_archivos_xlsx:
    archivo_xlsx = os.path.join(base_path_original, nombre_archivo_xlsx)
    nombre_archivo_base, _ = os.path.splitext(nombre_archivo_xlsx)
    nombre_archivo_csv = nombre_archivo_base.replace(" ", "_").replace("de_", "").replace("de", "").replace(".", "_") + ".csv"
    archivo_csv = os.path.join(base_path_limpio, nombre_archivo_csv)

    if os.path.exists(archivo_xlsx):
        print(f"Procesando archivo: {archivo_xlsx}")
        try:
            df = pd.read_excel(archivo_xlsx)
            df.to_csv(archivo_csv, index=False, sep='|', encoding='utf-8')
            print(f'Archivo convertido y guardado en: {archivo_csv}')
        except Exception as e:
            print(f"Error al leer el archivo {nombre_archivo_xlsx}: {e}")
    else:
        print(f"El archivo {nombre_archivo_xlsx} no existe en: {base_path_original}")

print("Proceso de conversión completado.")
