import csv
import os
from typing import List, Dict

# Encabezados de referencia en el orden correcto
REFERENCE_HEADERS = [
    'ARCHIVO_FUENTE',
    'MES_REPORTE',
    "ID_CASO",
    "TIPO_SOLICITUD",
    "FECHA_RADICADO_EN_DEFENSORIA",
    "NOMBRE_O_RAZON_SOCIAL",
    "REPRESENTANTE_O_APODERADO",
    "NIT/CC",
    "DIRECCION",
    "TELEFONO",
    "E_MAIL",
    "DEPENDENCIA_DIAN",
    "MACROPROCESO",
    "PROCESO",
    "SUBPROCESO",
    "PROCEDIMIENTO",
    "RIESGO",
    "MOTIVO_RIESGO",
    "DESCRIPCION_DE_LA_SOLICITUD",
    "FECHA_RADICADO_EN_DEFENSORIA",
    "ASIGNACION",
    "CARGO",
    "DEPENDENCIA_DIAN",
    "MOTIVO_RIESGO",
    "DESCRIPCION_DE_LA_SOLICITUD",
    "FECHA_ASIGNACION",
    "ACTUACIONES",
    "SOLUCIONADO_A_FAVOR"
]

# Diccionario para reemplazar nombres de columnas
REPLACEMENT_MAP = {
    'NOMBRE_ARCHIVO': 'ARCHIVO_FUENTE',
    'NIT_CC': 'NIT/CC'
}

def normalize_column_name(column_name: str) -> str:
    """Normaliza los nombres de columna reemplazando espacios y caracteres especiales."""
    # Reemplazar espacios, guiones y caracteres especiales
    column_name = column_name.strip().upper()
    column_name = column_name.replace(' ', '_')
    column_name = column_name.replace('-', '_')
    column_name = column_name.replace('Á', 'A')
    column_name = column_name.replace('É', 'E')
    column_name = column_name.replace('Í', 'I')
    column_name = column_name.replace('Ó', 'O')
    column_name = column_name.replace('Ú', 'U')
    column_name = column_name.replace('Ñ', 'N')
    column_name = column_name.replace('.', '')
    return column_name

def organize_headers(actual_headers: List[str]) -> List[str]:
    """Organiza los headers manteniendo el orden de REFERENCE_HEADERS primero y luego los demás en su orden original."""
    # Normalizar nombres de columnas
    normalized_headers = [normalize_column_name(header) for header in actual_headers]
    
    # Aplicar reemplazos
    for i, header in enumerate(normalized_headers):
        normalized_headers[i] = REPLACEMENT_MAP.get(header, header)
    
    # Eliminar duplicados manteniendo el orden original
    seen = set()
    unique_headers = []
    for header in normalized_headers:
        if header not in seen:
            seen.add(header)
            unique_headers.append(header)

    reference_headers_normalized = [normalize_column_name(h) for h in REFERENCE_HEADERS]

    ordered_headers = []
    for ref_header in reference_headers_normalized:
        if ref_header in unique_headers:
            ordered_headers.append(ref_header)
    
    # Headers adicionales (en el orden original en que aparecen)
    remaining_headers = [h for h in unique_headers if h not in reference_headers_normalized]
    return ordered_headers + remaining_headers

def extract_headers(csv_filename: str) -> List[str]:
    """Extrae y organiza los headers de un CSV."""
    with open(csv_filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter='|')
        headers = next(reader, None)
        return organize_headers(headers if headers else [])

def clean_value(value: str) -> str:
    """Limpia los valores de las celdas."""
    if value is None:
        return ''
    value = str(value).strip()
    if value.upper() == 'NAN':
        return ''
    return value

def process_csv(input_filepath: str, output_filepath: str) -> None:
    """Procesa un archivo CSV reorganizando las columnas y normalizando los datos."""
    if not os.path.exists(input_filepath):
        print(f"Error: El archivo {input_filepath} no existe.")
        return

    # Obtener los encabezados organizados
    final_headers = extract_headers(input_filepath)

    with open(input_filepath, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile,delimiter='|', quotechar='"', quoting=csv.QUOTE_ALL)
        
        # Crear mapeo de nombres originales a nombres normalizados con reemplazos
        header_mapping = {}
        for orig_header in reader.fieldnames:
            # Normalizar el nombre original
            norm_header = normalize_column_name(orig_header)
            # Aplicar reemplazos del REPLACEMENT_MAP
            mapped_header = REPLACEMENT_MAP.get(norm_header, norm_header)
            header_mapping[orig_header] = mapped_header

        with open(output_filepath, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=final_headers, delimiter='|')
            writer.writeheader()

            for row in reader:
                # Limpiar y normalizar los valores con el mapeo correcto
                cleaned_row = {}
                for orig_header, value in row.items():
                    # Obtener el nombre normalizado y mapeado
                    mapped_header = header_mapping[orig_header]
                    cleaned_row[mapped_header] = clean_value(value)
                
                # Verificar si hay headers en final_headers que no están en cleaned_row
                # pero podrían estar en REPLACEMENT_MAP con valores alternativos
                for header in final_headers:
                    if header not in cleaned_row:
                        # Buscar si hay algún nombre alternativo en REPLACEMENT_MAP
                        for original, replacement in REPLACEMENT_MAP.items():
                            if replacement == header and original in cleaned_row:
                                cleaned_row[header] = cleaned_row[original]
                                break
                
                # Crear fila con todos los encabezados finales
                full_row = {header: cleaned_row.get(header, '') for header in final_headers}
                writer.writerow(full_row)
    
    print(f"CSV procesado y guardado en {output_filepath}")

def main():
    # Configuración de rutas
    base_path = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/copia_DIAN_DEFENSORIA/2025/CSV/")
    output_base_path = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/copia_DIAN_DEFENSORIA/2025/CSV/")
    
    # Lista de archivos a procesar
    filenames = [
        "consolidado_final_defensoria_Ene_Feb_2025_procesado.csv",
    ]

    # Asegurar que el directorio de salida exista
    os.makedirs(output_base_path, exist_ok=True)

    # Procesar cada archivo
    for filename in filenames:
        input_filepath = os.path.join(base_path, filename)
        output_filename = f"final_{filename}"
        output_filepath = os.path.join(output_base_path, output_filename)
        process_csv(input_filepath, output_filepath)

if __name__ == "__main__":
    main()