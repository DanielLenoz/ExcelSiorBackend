import csv
import os
from typing import Dict, List, Union, Optional
from dataclasses import dataclass
from validadores.validadores_defensoria import ValidadoresDefensoria


NULL_VALUES = {"$null$", "nan", "NULL", "N.A", "null"}
DELIMITER = '|'
ENCODING = 'utf-8'
TEMP_COMMA_REPLACEMENT = '\uE000'

@dataclass
class ErrorInfo:
    columna: str
    numero_columna: int
    tipo: str
    valor: str
    fila: int
    error: str

class CSVProcessor:
    def __init__(self):
        self.validator = ValidadoresDefensoria()
        self.error_messages = {
            'invalid_integer': "No es un entero válido",
            'invalid_float': "No es un flotante válido",
            'invalid_date': "No es una fecha válida",
            'invalid_datetime': "No es una fecha y hora válida",
            'invalid_nit': "No es un NIT válido",
            'invalid_dependencia': "No se encuentra en dependencia DIAN",
            'invalid_procedimiento': "No se encuentra en Procedimientos",
            'invalid_macroproceso': "No se encuentra en macroproceso",
            'invalid_proceso': "No se encuentra en proceso",
            'invalid_columns': "Número de columnas no coincide con el encabezado"
        }
        self.type_defaults = {
            "int": 0,
            "float": 0.0,
            "date": "",
            "date-dd-mm-yyyy": "",
            "datetime": "",
            "nit": "",
            "choice_dependencia_dian": None,
            "choice_procedimiento": None,
            "choice_macroproceso": None,
            "choice_proceso": None,
            "str": None,
            "str-sin-caracteres-especiales": None
        }
    def get_default_value(self, expected_type: str, processed_value: str):
        """Devuelve un valor por defecto según el tipo cuando falla la validación."""
        type_defaults = {
            "int": "",
            "float": "",
            "date": "",
            "date-dd-mm-yyyy": "",
            "datetime": "",
            "nit": processed_value,
            "choice_dependencia_dian": processed_value,
            "choice_procedimiento": processed_value,
            "choice_macroproceso": processed_value,
            "choice_proceso": processed_value,
            "str": processed_value,
            "str-sin-caracteres-especiales": processed_value
        }
        return type_defaults.get(expected_type, processed_value)

    def clean_value(self, value: str) -> str:
        """Limpia valores nulos y espacios en blanco."""
        if value.strip() in NULL_VALUES:
            return ""
        return value.strip()

    def validate_column_count(self, row: List[str], header: List[str], row_num: int) -> Optional[ErrorInfo]:
        """Valida que el número de columnas coincida con el encabezado."""
        if len(row) != len(header):
            return ErrorInfo(
                columna="",
                numero_columna=0,
                tipo="structure",
                valor=str(row),
                fila=row_num,
                error=self.error_messages['invalid_columns']
            )
        return None


    def process_value(self, value: str, expected_type: str, col_name: str, col_num: int, row_num: int) -> tuple:
        """Procesa un valor según su tipo esperado y devuelve el valor procesado y posible error."""
        original_value = value
        value = self.clean_value(value)
        processed_value = value
        error = None

        try:
            if expected_type == "int":
                processed_value, is_valid = self.validator.validar_entero(value)
                if not is_valid:
                    raise ValueError(self.error_messages['invalid_integer'])
                processed_value = int(processed_value)
                
            elif expected_type == "float":
                processed_value, is_valid = self.validator.validar_flotante(value)
                if not is_valid:
                    raise ValueError(self.error_messages['invalid_float'])
                processed_value = float(processed_value)
                
            elif expected_type == "date":
                processed_value, is_valid = self.validator.validar_fecha(value)
                if not is_valid:
                    raise ValueError(self.error_messages['invalid_date'])
                    
            elif expected_type == "date-dd-mm-yyyy":
                processed_value, is_valid = self.validator.validar_fecha(value, 'date_dd_mm_yyyy')
                if not is_valid:
                    raise ValueError(self.error_messages['invalid_date'])
                    
            elif expected_type == "datetime":
                processed_value, is_valid = self.validator.validar_fecha_hora(value)
                if not is_valid:
                    raise ValueError(self.error_messages['invalid_datetime'])
                    
            elif expected_type == "nit":
                processed_value, is_valid = self.validator.limpiar_nit(value)
                if not is_valid:
                    raise ValueError(self.error_messages['invalid_nit'])
                    
            elif expected_type == "choice_dependencia_dian":
                processed_value, is_valid = self.validator.validar_dependencia_dian(value)
                if not is_valid:
                    raise ValueError(self.error_messages['invalid_dependencia'])
                    
            elif expected_type == "choice_procedimiento":
                processed_value, is_valid = self.validator.validar_procedimientos(value)
                if not is_valid:
                    raise ValueError(self.error_messages['invalid_procedimiento'])
                    
            elif expected_type == "choice_macroproceso":
                processed_value, is_valid = self.validator.validar_macroproceso(value)
                if not is_valid:
                    raise ValueError(self.error_messages['invalid_macroproceso'])
                    
            elif expected_type == "choice_proceso":
                processed_value, is_valid = self.validator.validar_proceso(value)
                if not is_valid:
                    raise ValueError(self.error_messages['invalid_proceso'])
                    
            elif expected_type == "str-sin-caracteres-especiales":
                processed_value, _ = self.validator.validar_cadena_caracteres_especiales(value)
                
            elif expected_type == "str":
                processed_value = str(value).strip()
                
        except ValueError as e:
            error = ErrorInfo(
                columna=col_name,
                numero_columna=col_num,
                tipo=expected_type,
                valor=original_value,
                fila=row_num,
                error=str(e)
            )
            processed_value = self.get_default_value(expected_type, processed_value)
        return processed_value, error

    def preprocess_line(self, line: str) -> str:
        """Reemplaza comas internas por un carácter temporal"""
        # Dividir conservando campos entre comillas
        parts = []
        current_part = []
        in_quotes = False
        
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
                current_part.append(char)
            elif char == ',' and not in_quotes:
                current_part.append(TEMP_COMMA_REPLACEMENT)
            else:
                current_part.append(char)
        
        return ''.join(current_part)

    def postprocess_field(self, field: str) -> str:
        """Restaura las comas originales"""
        return field.replace(TEMP_COMMA_REPLACEMENT, ',')

    def process_csv(self, input_file: str, output_file: str, error_file: str, type_mapping: Dict[str, List[int]]) -> None:
        """Procesa el archivo CSV con manejo seguro de comas"""
        errors = []
        processed_rows = []
        
        with open(input_file, 'r', encoding=ENCODING) as csv_file:
            # Leer como texto normal para preprocesamiento
            lines = csv_file.readlines()
            
            # Procesar encabezado
            header_line = self.preprocess_line(lines[0])
            header = next(csv.reader([header_line], delimiter=DELIMITER))
            print(f"Columnas: {header}")

            # Procesar cada línea
            for row_num, line in enumerate(lines[1:], start=1):
                try:
                    # Preprocesar la línea
                    processed_line = self.preprocess_line(line)
                    
                    # Parsear con csv.reader
                    row = next(csv.reader([processed_line], delimiter=DELIMITER))
                    
                    # Postprocesar cada campo
                    row = [self.postprocess_field(field) for field in row]
                    
                    # Validar número de columnas
                    if len(row) != len(header):
                        errors.append(ErrorInfo(
                            columna="",
                            numero_columna=0,
                            tipo="structure",
                            valor=line,
                            fila=row_num,
                            error="Número de columnas incorrecto"
                        ))
                        continue
                    
                    # Procesar cada valor
                    processed_row = []
                    for col_num, (value, col_name) in enumerate(zip(row, header), start=1):
                        expected_type = type_mapping.get(col_num, "str")
                        processed_value, error = self.process_value(
                            value, expected_type, col_name, col_num, row_num
                        )
                        
                        if error:
                            errors.append(error)
                        processed_row.append(processed_value)
                    
                    processed_rows.append(processed_row)
                
                except Exception as e:
                    errors.append(ErrorInfo(
                        columna="",
                        numero_columna=0,
                        tipo="processing",
                        valor=line,
                        fila=row_num,
                        error=f"Error de procesamiento: {str(e)}"
                    ))

        # Guardar resultados (igual que antes)
        self.save_output(output_file, header, processed_rows, "finalizado")
        if errors:
            self.save_errors(error_file, errors)
        else:
            print("🎉 No se encontraron errores durante el procesamiento.")


    def save_output(self, file_path: str, header: List[str], data: List[List[str]], file_type: str) -> None:
        """Guarda el archivo de salida."""
        try:
            with open(file_path, 'w', newline='', encoding=ENCODING) as csv_file:
                writer = csv.writer(csv_file, delimiter=DELIMITER)
                writer.writerow(header)
                writer.writerows(data)
            print(f"✅ Archivo {file_type} guardado como '{file_path}'")
        except Exception as e:
            print(f"❌ Error al guardar el archivo {file_type}: {e}")
            raise

    def save_errors(self, file_path: str, errors: List[ErrorInfo]) -> None:
        """Guarda los errores en un archivo CSV."""
        print(f"⚠️ Se encontraron {len(errors)} errores. Guardando en '{file_path}'...")
        try:
            with open(file_path, 'w', newline='', encoding=ENCODING) as csv_file:
                fieldnames = errors[0].__dict__.keys() if errors else []
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                for error in errors:
                    writer.writerow(error.__dict__)
            print("📝 Archivo de errores guardado.")
        except Exception as e:
            print(f"❌ Error al guardar el archivo de errores: {e}")
            print("Errores encontrados:")
            for error in errors[:10]:  # Mostrar solo los primeros 10 errores para evitar saturación
                print(f"  - {error}")

if __name__ == "__main__":
    # Configuración de rutas
    base_path = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/copia_DIAN_DEFENSORIA/2025/CSV/")
    input_file = os.path.join(base_path, "consolidado_final_defensoria_Ene-Feb_2025.csv")
    output_file = os.path.join(base_path, "consolidado_final_defensoria_Ene-Feb_2025_procesado.csv")
    error_file = os.path.join(base_path, "errores_procesamiento.csv")

    # Mapeo de tipos de datos
    type_mapping = {
        "int": [],
        "float": [],
        "date": [],
        "datetime": [5, 20, 25],
        "str": [1, 2, 3, 4, 6, 7, 9, 10, 11, 17, 18, 19, 21, 22, 23, 26],
        "str-sin-caracteres-especiales": [15],
        "nit": [8],
        "choice_macroproceso": [13],
        "choice_procedimiento": [16],
        "choice_dependencia_dian": [12],
        "choice_proceso": [14]
    }

    # Procesar archivo
    processor = CSVProcessor()
    processor.process_csv(input_file, output_file, error_file, type_mapping)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    import csv
import os

# Encabezados de referencia en el orden correcto
REFERENCE_HEADERS = [
    "ARCHIVO_FUENTE",
    "Mes de reporte",
    "ID_CASO",
    "Tipo Solicitud",
    "FECHA_RADICADO EN DEFENSORIA",
    "NOMBRE O RAZON SOCIAL",
    "REPRESENTANTE O  APODERADO",
    "NIT/CC",
    "DIRECCIÓN",
    "TELÉFONO",
    "E-MAIL",
    "DEPENDENCIA DIAN",
    "MACROPROCESO",
    "PROCESO",
    "SUBPROCESO",
    "PROCEDIMIENTO",
    "RIESGO",
    "MOTIVO DE LA SOLICITUD ",
    "ProblemFin (DESCRIPCIÓN DE LA  SOLICITUD)",
    "FECHA_RADICADO EN DEFENSORIA",
    "ASIGNACIÓN",
    "CARGO",
    "DEPENDENCIA DIAN",
    "MOTIVO DE LA SOLICITUD ",
    "DESCRIPCIÓN DE LA  SOLICITUD",
    "FECHA ASIGNACIÓN",
    "ACTUACIONES",
    "SOLUCIONADO A FAVOR",

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
