import os
import csv
import re

base_path_original = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/copia_COLJUEGOS_PQRS/2025/CSV")
base_path_limpio = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/copia_COLJUEGOS_PQRS/2025/CSV/A")

lista_archivos_csv_at = [
"ARCHIVO_COLJ_I20250101_F20250131.csv",
"ARCHIVO_COLJ_I20250201_F20250227.csv",
"ARCHIVO_COLJ_I20250301_F20250331.csv",


]

# Asegurarse de que el directorio de salida exista
os.makedirs(base_path_limpio, exist_ok=True)

for nombre_archivo_csv_at in lista_archivos_csv_at:
    archivo_entrada = os.path.join(base_path_original, nombre_archivo_csv_at)
    nombre_archivo_base, _ = os.path.splitext(nombre_archivo_csv_at)
    nombre_archivo_csv_pipe = nombre_archivo_base + ".csv"
    archivo_salida = os.path.join(base_path_limpio, nombre_archivo_csv_pipe)


    if os.path.exists(archivo_entrada):
        print(f"Procesando archivo: {archivo_entrada}")
        try:
            with open(archivo_entrada, 'r', newline='', encoding='utf-8') as infile, \
                 open(archivo_salida, 'w', newline='', encoding='utf-8') as outfile:
                # Escribir la cabecera con las nuevas columnas al principio
                coincidencia_fecha = re.search(r"I(\d{8})", nombre_archivo_csv_at)

                mes_reporte = "Desconocido"
                if coincidencia_fecha:
                    fecha_str = coincidencia_fecha.group(1)
                    anio = fecha_str[:4]
                    mes = fecha_str[4:6]
                    mes_reporte = f"{mes}_{anio}"
                primera_linea = infile.readline().strip()
                cabecera = primera_linea.split('|@')
                nueva_cabecera = ['nombre_archivo', 'mes_reporte'] + cabecera
                outfile.write('|'.join(nueva_cabecera) + '\n')

                # Escribir los datos con las columnas adicionales vacías al principio
                for line in infile:
                    campos = line.strip().split('|@')
                    nueva_linea = [nombre_archivo_csv_at, mes_reporte] + campos
                    outfile.write('|'.join(nueva_linea) + '\n')
            print(f'Archivo convertido y guardado en: {archivo_salida}')
        except UnicodeDecodeError:
            print(f"Error de codificación UTF-8. Intentando con latin-1 para: {nombre_archivo_csv_at}")
            try:
                with open(archivo_entrada, 'r', newline='', encoding='latin-1') as infile_latin, \
                     open(archivo_salida, 'w', newline='', encoding='utf-8') as outfile:
                    # Escribir la cabecera con las nuevas columnas al principio
                    coincidencia_fecha = re.search(r"I(\d{8})", nombre_archivo_csv_at)

                    mes_reporte = "Desconocido"
                    if coincidencia_fecha:
                        fecha_str = coincidencia_fecha.group(1)
                        anio = fecha_str[:4]
                        mes = fecha_str[4:6]
                        mes_reporte = f"{mes}_{anio}"
                    primera_linea = infile_latin.readline().strip()
                    cabecera = primera_linea.split('|@')
                    nueva_cabecera = ['nombre_archivo', 'mes_reporte'] + cabecera
                    outfile.write('|'.join(nueva_cabecera) + '\n')

                    # Escribir los datos con las columnas adicionales vacías al principio
                    for line in infile_latin:
                        campos = line.strip().split('|@')
                        nueva_linea = [os.path.splitext(nombre_archivo_csv_at)[0], mes_reporte] + campos
                        outfile.write('|'.join(nueva_linea) + '\n')
                print(f'Archivo convertido y guardado (con latin-1) en: {archivo_salida}')
            except Exception as e_latin:
                print(f"Error al leer o convertir el archivo {nombre_archivo_csv_at} con latin-1: {e_latin}")
        except FileNotFoundError:
            print(f"Error: El archivo {nombre_archivo_csv_at} no se encontró en: {base_path_original}")
        except Exception as e:
            print(f"Error al leer o convertir el archivo {nombre_archivo_csv_at}: {e}")
    else:
        print(f"El archivo {nombre_archivo_csv_at} no existe en: {base_path_original}")

print("Proceso de conversión de archivos CSV completado.")