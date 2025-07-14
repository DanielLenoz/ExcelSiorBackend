import csv
import os
import re

def limpiar_nit(valor):
    """
    Limpia un NIT eliminando caracteres no numéricos y ajustando la longitud.

    Args:
        valor: El NIT a limpiar.

    Returns:
        El NIT limpiado, o None si no es válido.
    """
    if valor is None:
        return None
    valor = str(valor).strip()
    valor = valor.replace(".000000", "")
    valor = valor.split("-")[0]  # quitar parte después del guion
    valor = re.sub(r"[^\w]", "", valor)    # Elimina no numéricos
    return valor

def validar_entero(valor):
    """Valida si un valor es un entero."""
    if valor is None:
        return False
    valor = str(valor).strip()
    return re.fullmatch(r"^-?\d+$", valor) is not None

def validar_flotante(valor):
    """Valida si un valor es un flotante."""
    if valor is None:
        return False
    valor = str(valor).strip()
    return re.fullmatch(r"^-?\d+(\.\d+)?$", valor) is not None

def validar_fecha(valor):
    """Valida si un valor es una fecha en formato YYYY-MM-DD, ignorando la hora si está presente."""
    if valor is None:
        return False
    valor = str(valor).strip().split(" ")[0]  # Solo tomar la parte de la fecha
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", valor)) is not None

def validar_fecha_hora(valor):
    """Valida si un valor es una fecha y hora en formato YYYY-MM-DD HH:MM."""
    if valor is None:
        return False
    valor = str(valor).strip()
    return re.fullmatch(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$", valor) is not None

def validar_cadena(valor):
    """Valida si un valor es una cadena (siempre es verdadero, pero limpia espacios)."""
    if valor is None:
        return True #consideramos nulo como cadena vacia
    return True

def procesar_csv(archivo_entrada, archivo_salida, archivo_errores, tipos_por_posicion):
    """
    Procesa un archivo CSV, valida los tipos de datos y guarda los datos limpios y los errores.

    Args:
        archivo_entrada: Ruta del archivo CSV de entrada.
        archivo_salida: Ruta del archivo CSV de salida para los datos limpios.
        archivo_errores: Ruta del archivo CSV para guardar los errores.
        tipos_por_posicion: Diccionario que mapea las posiciones de las columnas a los tipos de datos.
    """
    errores = []
    filas_procesadas = []

    with open(archivo_entrada, 'r', encoding='utf-8') as archivo_csv:
        lector_csv = csv.reader(archivo_csv, delimiter='|')
        encabezado = next(lector_csv)  # Leer el encabezado
        print(f"Archivo leído exitosamente. Número de columnas: {len(encabezado)}")
        print(f"Primeras columnas: {encabezado[:5]} ...")

        for num_fila, fila in enumerate(lector_csv, start=1):  # Iterar sobre las filas
            fila_procesada = []
            if len(fila) != len(encabezado):
                errores.append({
                    "fila": num_fila,
                    "error": "Número de columnas en la fila no coincide con el encabezado",
                    "valor": fila
                })
                continue  # Saltar la fila si el número de columnas no coincide

            for num_col, valor in enumerate(fila, start=1):
                col_name = encabezado[num_col-1]
                tipo_esperado = None
                for tipo, posiciones in tipos_por_posicion.items():
                    if num_col in posiciones:
                        tipo_esperado = tipo
                        break
                if tipo_esperado is None:
                    tipo_esperado = "str" #si no esta en la lista se trata como str
                valor_original = valor
                valor = valor.replace("$null$", "").replace("nan", "")

                if tipo_esperado == "int" and valor != "":
                    valor = str(valor).replace(".0", "")
                    if validar_entero(valor):
                        valor_procesado = int(valor)
                    else:
                        errores.append({
                            "columna": col_name,
                            "numero_columna":num_col,
                            "tipo": "int",
                            "valor": valor_original,
                            "fila": num_fila,
                            "error": "No es un entero válido"
                        })
                        valor_procesado = 0
                        break
                elif tipo_esperado == "float" and valor != "":
                    if "." not in valor and validar_entero(valor):
                        valor = str(valor) + ".0"
                    if validar_flotante(valor):
                        valor_procesado = float(valor)
                    else:
                        errores.append({
                            "columna": col_name,
                            "numero_columna":num_col,
                            "tipo": "float",
                            "valor": valor_original,
                            "fila": num_fila,
                            "error": "No es un flotante válido"
                        })
                        valor_procesado = 0.0
                        break
                elif tipo_esperado == "date" and valor != "":                    # breakpoint()
                    if validar_fecha(valor):
                        valor_procesado = valor  # Ya está en formato YYYY-MM-DD
                    else:
                        errores.append({
                            "columna": col_name,
                            "numero_columna":num_col,
                            "tipo": "date",
                            "valor": valor_original,
                            "fila": num_fila,
                            "error": "No es una fecha válida (YYYY-MM-DD)"
                        })
                        valor_procesado = True
                        break
                elif tipo_esperado == "datetime" and valor != "":
                    if validar_fecha_hora(valor):
                        valor_procesado = valor #ya esta en el formato
                    else:
                        errores.append({
                            "columna": col_name,
                            "numero_columna":num_col,
                            "tipo": "datetime",
                            "valor": valor_original,
                            "fila": num_fila,
                            "error": "No es una fecha y hora válida (YYYY-MM-DD HH:MM)"
                        })
                        valor_procesado = True
                        break
                elif tipo_esperado == "nit":
                    valor_procesado = limpiar_nit(valor)
                    if valor_procesado is None:
                            errores.append({
                            "columna": col_name,
                            "numero_columna":num_col,
                            "tipo": "nit",
                            "valor": valor_original,
                            "fila": num_fila,
                            "error": "No es un NIT válido"
                        })
                            valor_procesado = True
                            break
                elif tipo_esperado == "str":
                    valor_procesado = str(valor).strip()
                else:
                    valor_procesado = str(valor).strip()

                fila_procesada.append(valor_procesado)
            filas_procesadas.append(fila_procesada)

        # Guardar archivo limpio
        if not errores: # Solo guardar si no hay errores
            print(f"Guardando archivo limpio como '{archivo_salida}'...")
            try:
                with open(archivo_salida, 'w', newline='', encoding='utf-8') as archivo_csv_limpio:
                    escritor_csv = csv.writer(archivo_csv_limpio, delimiter='|')
                    escritor_csv.writerow(encabezado)
                    escritor_csv.writerows(filas_procesadas)
                print("✅ Archivo limpio guardado.")
            except Exception as e:
                print(f"❌ Error al guardar el archivo limpio: {e}")
                errores.append({"archivo": archivo_salida, "error": str(e), "tipo": "escritura"})
        else:
            print("No se guardó el archivo limpio debido a errores.")
            
        # Guardar errores
        if errores:
            print(f"⚠️ Se encontraron {len(errores)} errores. Guardando en '{archivo_errores}'...")
            try:
                with open(archivo_errores, 'w', newline='', encoding='utf-8') as archivo_csv_errores:
                    escritor_csv_errores = csv.DictWriter(archivo_csv_errores, fieldnames=errores[0].keys())
                    escritor_csv_errores.writeheader()
                    escritor_csv_errores.writerows(errores)
                print("📝 Archivo de errores guardado.")
            except Exception as e:
                print(f"❌ Error al guardar el archivo de errores: {e}")
                print("  Errores encontrados:")
                for error in errores:
                    print(f"    - {error}")
        else:
            print("🎉 No se encontraron errores durante el procesamiento.")

if __name__ == "__main__":
    # Rutas de los archivos
    archivo_entrada = "consolidado_final_INFORME_2025_ENERO_MARZO_2025.csv"  # Reemplaza con tu archivo de entrada
    base_path = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/NOTIFICACIONES_DIAN/ORIGINAL/CSV/INFORME_2025_ENERO_MARZO_2025/")
    archivo_entrada = os.path.join(base_path, archivo_entrada)
    archivo_salida = os.path.join(base_path, "consolidado_final_INFORME_2025_ENERO_MARZO_2025_procesado.csv")
    archivo_errores = os.path.join(base_path, "errores_procesamiento.csv")

    # Mapeo de tipos de datos por posición de columna
    tipos_por_posicion = tipos_por_posicion = {
    "int": [
        1, 2, 4, 6, 7, 9, 16, 20, 34, 36, 38, 45
    ],
    "str": [
        3, 5, 8,10, 14, 15, 18, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 35, 37, 39, 40, 42, 43, 44
    ],
    "float": [12,],
    "date": [
        11, 17, 19, 21, 22, 23, 41, 46
    ],
    "nit": [
        13
    ]
}
    procesar_csv(archivo_entrada, archivo_salida, archivo_errores, tipos_por_posicion)



