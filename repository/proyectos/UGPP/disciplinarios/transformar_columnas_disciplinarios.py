import csv
import os
from typing import Dict, List, Union, Optional, Tuple
from dataclasses import dataclass
import io

NULL_VALUES = {"$null$", "nan", "NULL", "N.A", "null", "N.A."}
DELIMITER = '|'
ENCODING = 'utf-8'
TEMP_COMMA_REPLACEMENT = '\uE000'
TEMP_NEWLINE = '⏎'
TEMP_COMMA = '\uE000'

# Encabezados de referencia
REFERENCE_HEADERS = [
    'NOMBRE_ARCHIVO',
    'MES_REPORTE',
    'SOLICITUD',
    'NUMERO_DE_IDENTIFICACION_(CLIENTE)_(PERSONA_NATURAL)',
    'CLIENTE_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'PERSONA_JURIDICA_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'CIUDAD_MUNICIPIO_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'FECHA_DE_CREACION_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'FECHA_DE_RADICACION_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'NUMERO_DE_RADICADO_POR_CORRESPONDENCIA_(ENTRANTE)_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'TIPO_DE_RADICACION_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'CANAL_DE_ENTRADA_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'TIPO_DE_SOLICITUD_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'TEMA_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'SUB_TEMA_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'OTRA_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'TIPO_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'ESTADO_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'EVENTO',
    'FECHA_DE_EVENTO',
    'RAZON_PARA_EL_ESTADO_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'PROPIETARIO_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'EJECUTADO_POR',
    'AREA_DESDE_DONDE_SE_EJECUTO_LA_ACCION',
    'FECHA_DE_CIERRE_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'FECHA_DE_VENCIMIENTO_DEL_ANS_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'ESTADO_DEL_CASO_ANS_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'DIAS_HABILES_DE_LA_SOLICITUD_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'DIAS_HABILES_PARA_VENCIMIENTO_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'OPORTUNIDAD_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)',
    'DESCRIPCION_DEL_CASO_(SOLICITUD)_(REGISTRO_DE_SOLICITUD)'
]

# Mapeo de reemplazo de columnas
REPLACEMENT_MAP = {
}


@dataclass
class ErrorInfo:
    columna: str
    numero_columna: int
    tipo: str
    valor: str
    fila: int
    error: str

class CSVProcessor:
    def __init__(self, validator=None):
        self.validator = validator
        self.error_messages = {
            'invalid_integer': "No es un entero válido",
            'invalid_float': "No es un flotante válido",
            'invalid_date': "No es una fecha válida",
            'invalid_datetime': "No es una fecha y hora válida",
            'invalid_nit': "No es un NIT válido",
            'invalid_direccion_seccional': "No se encuentra en direccion seccional",
            'invalid_columns': "Número de columnas no coincide con el encabezado",
            "invalid_dependencia": "No se encuentra en dependencia"
        }

    def normalize_column_name(self, column_name: str) -> str:
        """Normaliza nombres de columnas reemplazando espacios y caracteres especiales."""
        column_name = column_name.strip().upper()
        replacements = [
            (' ', '_'), ('-', '_'), ('Á', 'A'), ('É', 'E'), ('Í', 'I'),
            ('Ó', 'O'), ('Ú', 'U'), ('Ñ', 'N'), ('.', ''), ('/', '_'),
        ]
        for old, new in replacements:
            column_name = column_name.replace(old, new)
        return column_name

    def organize_headers(self, actual_headers: List[str]) -> List[str]:
        """Organiza headers segue REFERENCE_HEADERS y aplica reemplazos."""
        normalized = [self.normalize_column_name(h) for h in actual_headers]

        # Aplicar reemplazos
        for i, header in enumerate(normalized):
            normalized[i] = REPLACEMENT_MAP.get(header, header)

        # Eliminar duplicados manteniendo orden
        seen = set()
        unique_headers = []
        for h in normalized:
            if h not in seen:
                seen.add(h)
                unique_headers.append(h)

        # Ordenar según REFERENCE_HEADERS
        ref_headers_normalized = [self.normalize_column_name(h) for h in REFERENCE_HEADERS]
        ordered = []
        remaining = []
        
        for ref_h in ref_headers_normalized:
            if ref_h in unique_headers:
                ordered.append(ref_h)
        remaining = [h for h in unique_headers if h not in ordered]
        return ordered + remaining

    def clean_value(self, value: str) -> str:
        """Limpia valores nulos y espacios."""
        if value is None:
            return ""
        value = str(value).strip()
        return "" if value.upper() in NULL_VALUES else value

    def preprocess_line(self, line: str) -> str:
        """Preprocesa línea para manejar comas y saltos internos."""
        if not line.strip():
            return line
            
        line = line.replace('\\n', TEMP_NEWLINE)
        in_quotes = False
        processed = []
        
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
                processed.append(char)
            elif char == ',' and not in_quotes:
                processed.append(TEMP_COMMA)
            else:
                processed.append(char)
        
        return ''.join(processed)

    def postprocess_field(self, field: str) -> str:
        """Restaura caracteres especiales a su forma original."""
        return field.replace(TEMP_COMMA, ',').replace(TEMP_NEWLINE, '\n')

    def read_csv(self, input_file: str) -> Tuple[List[str], List[List[str]]]:
        """Lee CSV con manejo de comas y saltos internos."""
        with open(input_file, 'r', encoding=ENCODING) as f:
            processed_content = [self.preprocess_line(line) for line in f.read().splitlines()]
            reader = csv.reader(
                processed_content,
                delimiter=DELIMITER,
                quotechar='"',
                escapechar='\\'
            )
            
            data = [row for row in reader]
            return data[0], data[1:] if len(data) > 1 else []

    def process_csv(self, input_file: str, output_file: str, error_file: str = None, 
                   type_mapping: Dict[str, List[int]] = None) -> None:
        """
        Procesa CSV completo con:
        - Normalización de headers
        - Validación de datos
        - Manejo de errores
        """
        try:
            header, rows = self.read_csv(input_file)
            normalized_header = self.organize_headers(header)
            
            errors = []
            processed_rows = []
            
            for row_num, row in enumerate(rows, start=1):
                try:
                    if len(row) != len(header):
                        raise ValueError(f"Columnas esperadas: {len(header)}, obtenidas: {len(row)}")
                    
                    processed_row = []
                    for col_num, (raw_val, col_name) in enumerate(zip(row, header), start=1):
                        value = self.postprocess_field(raw_val)
                        clean_val = self.clean_value(value)
                        
                        # Validación de tipos si hay type_mapping y validator
                        if type_mapping and self.validator:
                            expected_type = self._get_expected_type(col_num, type_mapping)
                            clean_val, error = self._validate_value(clean_val, expected_type, col_name, col_num, row_num)
                            if error:
                                errors.append(error)
                        
                        processed_row.append(clean_val)
                    
                    # Reorganizar según headers normalizados
                    final_row = self._reorganize_row(processed_row, header, normalized_header)
                    processed_rows.append(final_row)
                
                except Exception as e:
                    errors.append(ErrorInfo(
                        columna="", numero_columna=0, tipo="processing",
                        valor=str(row), fila=row_num, error=str(e)
                    ))
            
            self._save_output(output_file, normalized_header, processed_rows)
            if errors and error_file:
                self._save_errors(error_file, errors)
            
        except Exception as e:
            raise Exception(f"Error procesando archivo: {str(e)}")

    def _get_expected_type(self, col_num: int, type_mapping: Dict[str, List[int]]) -> str:
        """Obtiene el tipo esperado para una columna."""
        for type_name, columns in type_mapping.items():
            if col_num in columns:
                return type_name
        return "str"

    def _validate_value(self, value: str, expected_type: str, col_name: str, 
                       col_num: int, row_num: int) -> Tuple[str, Optional[ErrorInfo]]:
        """Valida un valor según su tipo esperado."""
        if not value or not self.validator:
            return value, None
            
        try:
            validation_methods = {
                "int": ("validar_entero", "invalid_integer"),
                "float": ("validar_flotante", "invalid_float"),
                "date": ("validar_date", "invalid_date"),
                "datetime": ("validar_date", "invalid_datetime"),
                "nit": ("limpiar_nit", "invalid_nit"),
                "choice_direccion_seccional": ("validar_direccion_seccional", "invalid_direccion_seccional"),
                "choice_dependencia": ("validar_dependencia", "invalid_dependencia")
            }
            
            if expected_type in validation_methods:
                method, error_key = validation_methods[expected_type]
                validator = getattr(self.validator, method)
                validated, is_valid = validator(value)
                if not is_valid:
                    raise ValueError(self.error_messages[error_key])
                
                return validated, None
            
            return value, None
            
        except ValueError as e:
            error = ErrorInfo(
                columna=col_name, numero_columna=col_num,
                tipo=expected_type, valor=value, fila=row_num,
                error=str(e)
            )
            return validated, error

    def _reorganize_row(self, row: List[str], original_headers: List[str], 
                       final_headers: List[str]) -> List[str]:
        """Reorganiza una fila según los headers finales."""
        header_map = {self.normalize_column_name(h): i for i, h in enumerate(original_headers)}
        final_row = []
        for header in final_headers:
            norm_header = self.normalize_column_name(header)
            if norm_header in header_map:
                final_row.append(row[header_map[norm_header]])
            else:
                # Buscar posibles mapeos alternativos
                for orig, replacement in REPLACEMENT_MAP.items():
                    if replacement == header and orig in header_map:
                        final_row.append(row[header_map[orig]])
                        break
                    else:
                        final_row.append("")

        return final_row

    def _save_output(self, file_path: str, header: List[str], data: List[List[str]]) -> None:
        """Guarda datos procesados en CSV."""
        with open(file_path, 'w', newline='', encoding=ENCODING) as f:
            writer = csv.writer(f, delimiter=DELIMITER)
            writer.writerow(header)
            writer.writerows(data)

    def _save_errors(self, file_path: str, errors: List[ErrorInfo]) -> None:
        """Guarda errores en CSV."""
        with open(file_path, 'w', newline='', encoding=ENCODING) as f:
            writer = csv.DictWriter(f, fieldnames=errors[0].__dict__.keys())
            writer.writeheader()
            writer.writerows([e.__dict__ for e in errors])

# Ejemplo de uso
if __name__ == "__main__":
    from validadores.validadores_disciplianrios import ValidadoresDisciplinarios
    
    processor = CSVProcessor(validator=ValidadoresDisciplinarios())
    
    type_mapping = {
    "int": [],
    "float": [],
    "date": [],
    "datetime": [4,5,6,7,33,34,35],
    "str": [
    ],
    "str-sin-caracteres-especiales": [
        1, 2, 3, 8, 10, 11, 13, 15, 16, 17, 18, 19, 20, 21,
        22, 23, 24, 25, 26, 27, 28, 29, 30,31, 32
    ],
    "nit": [9],
    "choice_direccion_seccional": [12],
    "choice_dependencia": [14]
}

    base_path = os.path.expanduser("~/Documentos/ITRC/DOCUMENTOS_LIMPIAR/copia_UGPP_DISC/2025/A")
    input_file = os.path.join(base_path, "consolidado_ugpp_2025_Ene_a_Abr_disciplinarios.csv")
    output_file = os.path.join(base_path, "consolidado_ugpp_2025_Ene_a_Abr_disciplinarios_procesado.csv")
    error_file = os.path.join(base_path, "consolidado_ugpp_2025_Ene_a_Abr_disciplinarios_errores_procesamiento.csv")


    processor.process_csv(input_file, output_file, error_file, type_mapping)


