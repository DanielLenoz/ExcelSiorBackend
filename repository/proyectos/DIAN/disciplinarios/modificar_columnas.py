import pandas as pd
import os
import re
from datetime import datetime

def es_fecha(valor):
    """Verifica si un valor puede interpretarse como una fecha."""
    if pd.isna(valor):  # Si es NaN, no es fecha
        return False

    valor = str(valor).strip()
    formatos_fecha = ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d']
    for fmt in formatos_fecha:
        try:
            datetime.strptime(valor, fmt)
            return True
        except ValueError:
            continue
    return False

def reordenar_columnas(df):
    """
    Reordena columnas si hay más de 34 y la columna 35 es fecha.
    Mueve columnas 35-67 al final, después de 'fecha_radicacion'.
    """
    # breakpoint()
    if df.shape[1] <= 34:
        return df

    columnas = list(df.columns)
    col_34 = columnas[:35]
    col_extra = columnas[35:67]  # columna 35 a 67

    # Verifica si la primera columna extra tiene formato de fecha
    primera_fecha_col = col_extra[0]
    if not df[primera_fecha_col].apply(es_fecha).any():
        return df  # No es fecha, se deja igual

    # Separar columnas
    columnas_fijas = col_34
    columnas_mover = col_extra
    # breakpoint()

    # Identificar posición de 'fecha_radicacion' para insertar después
    if 'FECHA RADICACION' in columnas_fijas:
        pos = columnas_fijas.index('FECHA RADICACION')
    else:
        pos = len(columnas_fijas)

    # Insertar columnas extra después de fecha_radicacion
    columnas_nuevas = columnas_fijas[:35]
    columnas_finales = columnas_nuevas

    return df[columnas_fijas[:35]]

# Configuración del archivo
archivo_entrada_nombre = "consolidado_final_disciplinario_Ene-Dic_2021_sin_arrobas.csv"
base_path = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/copia_DIAN_DISC/2021/CSV_LIMPIO/")
ruta_del_archivo = os.path.join(base_path, archivo_entrada_nombre)
archivo_salida = os.path.join(base_path, "consolidado_final_disciplinario_Ene-Dic_2021_reordenado.csv")

# Leer archivo original delimitado por '|'
df = pd.read_csv(ruta_del_archivo, sep='|', encoding='utf-8', dtype=str)

# Reordenar si es necesario
df_reordenado = reordenar_columnas(df)

# Guardar resultado
df_reordenado.to_csv(archivo_salida, index=False, sep='|', encoding='utf-8')
print(f"✅ Archivo procesado y guardado en: {archivo_salida}")
