import re
import os
import csv

def procesar_csv(ruta_entrada, ruta_salida):
    """
    Lee un archivo CSV delimitado por "|", elimina todas las "@" que no forman parte de una dirección de correo electrónico,
    y guarda el resultado en un nuevo archivo CSV utilizando la librería csv.

    Args:
        ruta_entrada (str): La ruta al archivo CSV de entrada.
        ruta_salida (str): La ruta al archivo CSV de salida.
    """
    try:
        with open(ruta_entrada, 'r', encoding='utf-8', newline='') as archivo_entrada, \
                open(ruta_salida, 'w', encoding='utf-8', newline='') as archivo_salida:

            lector_csv = csv.reader(archivo_entrada, delimiter='|')
            escritor_csv = csv.writer(archivo_salida, delimiter='|', quoting=csv.QUOTE_MINIMAL)

            for fila_leida in lector_csv:
                if '|' in fila_leida[2]:
                    # print(fila_leida)
                    # breakpoint()
                    # La línea no se dividió correctamente, la dividimos manualmente
                    fila = fila_leida[2].split('|')
                    fila = fila_leida[0:2]+ fila
                else:
                    # La línea ya fue dividida correctamente
                    fila = fila_leida

                nueva_fila = []
                for celda in fila:
                    if isinstance(celda, str):
                        # Elimina "@" excepto en correos electrónicos
                        nueva_celda = re.sub(r'@(?!(?:[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}))', '', celda)
                    else:
                        nueva_celda = celda
                    nueva_fila.append(nueva_celda)
                escritor_csv.writerow(nueva_fila)


        print(f"Archivo procesado y guardado en: {ruta_salida}")

    except FileNotFoundError:
        print(f"Error: No se pudo encontrar el archivo de entrada: {ruta_entrada}")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

def main():
    """
    Función principal para ejecutar el procesamiento del CSV.
    """
    # Rutas de los archivos
    archivo_entrada_nombre = "ARCHIVO_DIAN_DISC_I20250401_20250430_CONVERTIDO.csv"
    base_path = os.path.expanduser("~/Descargas/A")
    ruta_entrada = os.path.join(base_path, archivo_entrada_nombre)
    ruta_salida = os.path.join(base_path, "ARCHIVO_DIAN_DISC_I20250401_20250430_CONVERTIDO_sin_arrobas.csv")

    # Llamar a la función para procesar el CSV
    procesar_csv(ruta_entrada, ruta_salida)

if __name__ == "__main__":
    main()
  
