import csv
import os

# Encabezados de referencia en el orden correcto
REFERENCE_HEADERS = [
    "PLAN_IDENTIF_ACTO",
    "CODIGO_ADMINISTRACION",
    "SECCIONAL",
    "CODIGO_DEPENDENCIA",
    "DEPENDENCIA",
    "ANO_CALENDARIO",
    "CODIGO_ACTO",
    "DESCRIPCION_ACTO",
    "ANO_ACTO",
    "CONSECUTIVO_ACTO",
    "FECHA_ACTO",
    "CUANTIA_ACTO",
    "NIT",
    "RAZON_SOCIAL",
    "DIRECCION",
    "PLANILLA_REMISION",
    "FECHA_PLANILLA_REMISION",
    "FUNCIONARIO_ENVIA",
    "PLANILLA_CORRECCION",
    "FECHA_PLANILLA_CORRECCION",
    "FECHA_NOTIFICACION",
    "FECHA_EJECUTORIA",
    "GUIA",
    "COD_ESTADO",
    "ESTADO_NOTIFICACION",
    "COD_NOTIFICACION",
    "MEDIO_NOTIFICACION",
    "NUMERO_EXPEDIENTE",
    "TIPO_DOCUMENTO",
    "PERI_IMPUESTO",
    "PERI_PERIODO",
    "NOMBRE_APLICACION",
    "PAIS_COD_NUM_PAIS",
    "PAIS",
    "MUNI_CODIGO_DEPART",
    "NOMBRE_DEPARTAMENTO",
    "MUNI_CODIGO_MUNICI",
    "NOMBRE_MUNICIPIO",
    "REGI_CODIGO_REGIMEN",
    "REGIMEN",
    "FECHA_LEVANTE_EJECUTORIA",
    "MOTIVO_LEVANTE_EJECUTORIA",
    "NORMATIVIDAD",
    "FUNCIONARIO_RECIBE",
    "PLANILLA_REMI",
    "FECHA_PLANILLA_REMI_ARCHIVO",
    "SEC",
    "FECHA_CITACION",
    "ENTREGA"
]

# Diccionario para reemplazar nombres de columnas
replacement_map = {
    'NOMBRE_DEPTO': 'NOMBRE_DEPARTAMENTO',
    'DEPARTAMENTO': 'NOMBRE_DEPARTAMENTO',
    'MUNICIPIO': 'NOMBRE_MUNICIPIO',
    'FECHA_PLANILLA_REMI': 'FECHA_PLANILLA_REMISION',
    'COPL_PLANILLA_CORR': 'PLANILLA_CORR',
    'COPL_PLANILLA_REMI': 'PLANILLA_REMI',
    'COD_ACTO': 'CODIGO_ACTO',
    'PLANILLA_REMI': 'PLANILLA_REMISION',
    'PLANILLA_CORR': 'PLANILLA_CORRECCION',
    'FECHA_PLANILLA_CORR': 'FECHA_PLANILLA_CORRECCION',
    'COD_NOTI': 'COD_NOTIFICACION',
    'PERIODO': 'PERI_PERIODO',
    'IMPUESTO': 'PERI_IMPUESTO',
    'DEPTO': 'NOMBRE_DEPARTAMENTO',
    'MUNICIPIO': 'NOMBRE_MUNICIPIO',
    'PIA': 'PLAN_IDENTIF_ACTO',
    'SECC': 'SECCIONAL',
    'DEP': 'CODIGO_DEPENDENCIA',
    'ANO': 'ANO_CALENDARIO',
    'AÑO': 'ANO_CALENDARIO',
    'DESCRIPCION': 'DESCRIPCION_ACTO',
    'CONSECUTIVO': 'CONSECUTIVO_ACTO',
    'CUANTIA': 'CUANTIA_ACTO',
    'FECHA_PLANILLA': 'FECHA_PLANILLA_REMISION',
    'TIPO_DOC': 'TIPO_DOCUMENTO',
    'TINO_CODIGO_NOTIFICACION':'COD_NOTIFICACION',
    'ÁREA': 'AREA',
    'FECHA_LEVANTE':'FECHA_LEVANTE_EJECUTORIA',
    'MOTIVO_LEVANTE': 'MOTIVO_LEVANTE_EJECUTORIA'
}
replacement_muni_depto = {
    'MUNICIPIO': 'MUNI_CODIGO_MUNICI',
    'DEPTO': 'MUNI_CODIGO_DEPART',
    'FECHA_PLANILLA': 'FECHA_PLANILLA_REMISION_1'
}

def organize_headers(actual_headers:list):
    """
    Organiza los headers en el orden de reference_headers y maneja los renombramientos.
    """
    # Normalizar nombres (todo a mayúsculas)
    actual_headers_list = [header.strip().upper() for header in actual_headers ]
    if actual_headers in ('MUNICIPIO', 'NOMBRE_MUNICIPIO') and ('FECHA_PLANILLA', 'FECHA_PLANILLA_CORR', 'FECHA_PLANILLA_REMI'):
        for key, new_name in replacement_muni_depto.items():
            if key in actual_headers_list:
                index_to_replace = actual_headers_list.index(key)
                actual_headers_list[index_to_replace] = new_name



    # Aplicar reemplazos
    for key, new_name in replacement_map.items():
        if key in actual_headers_list:
            index_to_replace = actual_headers_list.index(key)
            actual_headers_list[index_to_replace] = new_name


    # Agregar columnas adicionales que no están en reference_headers
    extra_headers = [header for header in actual_headers_list if header not in REFERENCE_HEADERS]
    return REFERENCE_HEADERS + extra_headers  # Ordenadas y luego las adicionales al final

def extract_headers(csv_filename):
    """
    Extrae y organiza los headers de un CSV.
    """
    with open(csv_filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter='|')
        headers = next(reader, None)
        return organize_headers(headers if headers else [])

def reorganize_row(row, original_headers, final_headers):
    """
    Reorganiza los datos de una fila según la nueva disposición de columnas.
    """
    header_index = {col: idx for idx, col in enumerate(original_headers)}
    new_row = [row[header_index[col]] if col in header_index else '' for col in final_headers]
    return new_row

def unir_csvs_en_csv(input_filepath, output_filepath):
    """
    Une múltiples CSV asegurando que las columnas estén organizadas y completas.
    """


    if not os.path.exists(input_filepath):
        print(f"Error: El archivo {input_filepath} no existe.")
        return

        # Obtener los encabezados organizados
    final_headers = extract_headers(input_filepath)

    with open(input_filepath, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter='|')
        original_headers = reader.fieldnames if reader.fieldnames else []

        # Estandarizar encabezados originales (strip y upper)
        actual_headers_list = [header.strip().upper() for header in original_headers]

        with open(output_filepath, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=final_headers, delimiter='|')
            writer.writeheader()

            for row in reader:
                cleaned_row = {key.strip().upper(): (value if value != "nan" else "")  for key, value in row.items()}
                full_row = {header: cleaned_row.get(header, '') for header in final_headers}
                writer.writerow(full_row)
    print(f"CSV procesado y guardado en {output_filepath}")

# Lista de archivos CSV de entrada
base_path = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/NOTIFICACIONES_DIAN/ORIGINAL/CSV/INFORME_2025_ENERO_MARZO_2025/")
filenames = [
    # "2024abr-sep.csv",
    # "2024ene-mar.csv",
    "consolidado_final_INFORME_2025_ENERO_MARZO_2025_procesado.csv",
    # "Consolidado2017-2022_procesado.csv",
    # "Consolidado2019-2020_ent_procesado.csv",
    # "Consolidado2021-mayo2022_procesado.csv",
    # "Consolidadodic2022-dic2023_procesado.csv",
    # "Consolidadojun-nov2022_procesado.csv",
]

# Definir la ruta base para los archivos de salida
output_base_path = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/NOTIFICACIONES_DIAN/ORIGINAL/CSV/INFORME_2025_ENERO_MARZO_2025/FINAL/")

# Asegurarse de que el directorio de salida exista
os.makedirs(output_base_path, exist_ok=True)

# Procesar cada archivo
for filename in filenames:
    input_filepath = os.path.join(base_path, filename)
    output_filename = f"final_{filename}"
    output_filepath = os.path.join(output_base_path, output_filename)
    unir_csvs_en_csv(input_filepath, output_filepath)
