import pandas as pd
import os
import re

base_path_original = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/copia_COLJUEGOS_PQRS/2021")
base_path_limpio = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/copia_COLJUEGOS_PQRS/2021/CSV/")
archivo_xlsx = os.path.join(base_path_original, 'Mes de Agosto de 2021.xlsx')  # Reemplaza 'archivo.xlsx' con el nombre de tu archivo
archivo_csv = os.path.join(base_path_limpio, 'Mes_de_Agosto_de_2021.csv')      # Reemplaza 'archivo.csv' con el nombre deseado para el CSV

lista_archivos_xlsx = [
    "01_enero_2021.xlsx",
    "02_febrero_2021.xlsx",
    "03_marzo_2021.xlsx",
    "04_abril_2021.xlsx",
    "05_mayo_2021.xlsx",
    "06_junio_2021.xlsx",
    "07_julio_2021.xlsx",
    "08_agosto_2021.xlsx",
    "09_septiembre_2021.xlsx",
    "10_octubre_2021.xlsx",
    "11_noviembre_2021.xlsx",
    "12_diciembre_2021.xlsx",

]
MESES = {
    "Enero": "01", "Febrero": "02", "Marzo": "03", "Abril": "04",
    "Mayo": "05", "Junio": "06", "Julio": "07", "Agosto": "08",
    "Septiembre": "09", "Octubre": "10", "Noviembre": "11", "Diciembre": "12"
}
MESES_LOWER = {
    "enero": "1", "febrero": "2", "marzo": "3", "abril": "4", "mayo": "5", "junio": "6",
    "julio": "7", "agosto": "8", "septiembre": "9", "octubre": "10", "noviembre": "11", "diciembre": "12"
}

for nombre_archivo_xlsx in lista_archivos_xlsx:
    archivo_xlsx = os.path.join(base_path_original, nombre_archivo_xlsx)
    nombre_archivo_base, _ = os.path.splitext(nombre_archivo_xlsx)
    nombre_archivo_csv = nombre_archivo_base.replace(" ", "_").replace("de_", "").replace("de", "").replace(".", "_") + ".csv"
    archivo_csv = os.path.join(base_path_limpio, nombre_archivo_csv)

    coincidencia = re.search(r"Mes_([A-Za-z]+)_(\d{4})", nombre_archivo_csv)
    coincidencia_2 = re.search(r"Mes_de_([A-Za-z]+)_de_(\d{4})", nombre_archivo_csv)
    coincidencia_fecha = re.search(r"I(\d{8})", nombre_archivo_csv)
    coincidencia_pqr = re.search( r"(\d{4})-(\d{2})", nombre_archivo_csv)
    coincidencia_dynamics = re.search(  r"_(\w+)_(\d{4})_", nombre_archivo_csv)
    mes_reporte = "Desconocido"
    if coincidencia :
        mes_nombre = coincidencia.group(1).capitalize()
        anio = coincidencia.group(2)
        mes_numero = MESES.get(mes_nombre)
        if mes_numero:
            mes_reporte = f"{mes_numero}_{anio}"
    elif coincidencia_2:
        mes_nombre = coincidencia_2.group(1).capitalize()
        anio = coincidencia_2.group(2)
        mes_numero = MESES.get(mes_nombre)
        if mes_numero:
            mes_reporte = f"{mes_numero}_{anio}"
    if coincidencia_fecha:
        fecha_str = coincidencia_fecha.group(1)
        anio = fecha_str[:4]
        mes = fecha_str[4:6]
        mes_reporte = f"{mes}_{anio}"
    if coincidencia_dynamics:
        mes_nombre = coincidencia_dynamics.group(1).lower()  
        anio = coincidencia_dynamics.group(2)
        mes_numero = MESES_LOWER.get(mes_nombre)
        if mes_numero:
            mes_reporte = f"{mes_numero}_{anio}"
    if coincidencia_pqr:
        anio = coincidencia_pqr.group(1)
        mes_numero = coincidencia_pqr.group(2)
        mes_numero_sin_cero = str(int(mes_numero))
        mes_reporte = f"{mes_numero_sin_cero}_{anio}"
    

    if os.path.exists(archivo_xlsx):
        print(f"Procesando archivo: {archivo_xlsx}")
        try:
            df = pd.read_excel(archivo_xlsx)
            if not df.empty and len(df) > 0:
                df = df.iloc[1:]
            df.insert(0, 'nombre_archivo', nombre_archivo_base)
            df.insert(1, 'mes_reporte', mes_reporte)

            df.to_csv(archivo_csv, index=False, sep='|', encoding='utf-8')
            print(f'Archivo convertido y guardado en: {archivo_csv}')
        except Exception as e:
            print(f"Error al leer el archivo {nombre_archivo_xlsx}: {e}")
    else:
        print(f"El archivo {nombre_archivo_xlsx} no existe en: {base_path_original}")


print("Proceso de conversión completado.")
