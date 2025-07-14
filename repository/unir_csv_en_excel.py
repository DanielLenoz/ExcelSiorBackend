import pandas as pd
import os
import csv

def unir_csv_en_excel(lista_archivos_csv, archivo_salida):
    try:
        max_filas = 1048576  # Límite de filas en Excel
        
        # Leer el archivo Excel existente (si existe)
        if os.path.exists(archivo_salida):
            try:
                excel_data = pd.read_excel(archivo_salida, sheet_name=None)  # Leer todas las hojas
            except Exception:
                excel_data = {}  # Si falla, inicia un diccionario vacío
        else:
            excel_data = {}  # Iniciar con un diccionario vacío si el archivo no existe
        
        # Procesar los archivos CSV y agregar a excel_data
        for ruta in lista_archivos_csv:
            try:
                nombre_base = ruta.split('/')[-1].replace('.csv', '')[:25]  # Nombre base limitado a 25 caracteres
                data = []
                with open(ruta, 'r', encoding='utf-8') as archivo_csv:
                    lector_csv = csv.reader(archivo_csv)
                    for fila in lector_csv:
                        data.append(fila)
                
                df = pd.DataFrame(data)
                num_partes = (len(df) // max_filas) + 1  # Calcular cuántas partes necesitamos
                
                for i in range(num_partes):
                    inicio = i * max_filas
                    fin = (i + 1) * max_filas
                    nombre_hoja = f"{nombre_base}_part{i+1}"[:31]  # Limitar nombre a 31 caracteres
                    excel_data[nombre_hoja] = df.iloc[inicio:fin]  # Guardar parte en el diccionario
                    print(f"Parte {i+1} del archivo {ruta} agregada como hoja '{nombre_hoja}'")
            except Exception as e:
                print(f"Error procesando {ruta}: {e}")
        
        # Escribir todo a un nuevo archivo Excel
        with pd.ExcelWriter(archivo_salida) as writer:
            for sheet_name, df in excel_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
    
    except Exception as e:
        print(f"Error general: {e}")

# Lista de archivos CSV
rutas_csv_final = [
    
    # "/home/anacleto/Descargas/consolidados/CSV/final/2024abr-sep.csv",
    # "/home/anacleto/Descargas/consolidados/CSV/final/2024ene-mar.csv",
    # "/home/anacleto/Descargas/consolidados/CSV/final/Consolidadodic2022-dic2023.csv",
    # "/home/anacleto/Descargas/consolidados/CSV/final/Consolidadojun-nov2022.csv",
    # "/home/anacleto/Descargas/consolidados/CSV/final/Consolidado2021-mayo2022.csv",
    # "/home/anacleto/Descargas/consolidados/CSV/final/Consolidado2019-2020_ent.csv",
    "/home/anacleto/Descargas/consolidados/CSV/final/Consolidado2017-2019_ent.csv",
    # "/home/anacleto/Descargas/consolidados/CSV/final/Consolidado2017-2022.csv",
    
    
    
    
]


archivo_excel_salida = "/home/anacleto/Descargas/consolidados/CSV/final/Consolidado_Final.xlsx"

unir_csv_en_excel(rutas_csv_final, archivo_excel_salida)
