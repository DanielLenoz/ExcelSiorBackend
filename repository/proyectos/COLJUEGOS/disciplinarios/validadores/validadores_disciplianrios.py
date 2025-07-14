import re
import unicodedata
from datetime import datetime, time, timedelta
from typing import Tuple, Dict, Union

from valores_choice.direccion_seccional import VALORES_DIRECCION_SECCIONAL, VALORES_REEMPLAZO_DIRECCION_SECCIONAL
from valores_choice.proceso import VALORES_REEMPLAZO_PROCESO, VALORES_PROCESO


DATE_FORMATS = {
    'date': "%Y-%m-%d",
    'date_YY': "%Y/%m/%d",
    'date_dd_mm_yyyy': "%d/%m/%Y",
    'datetime': "%Y-%m-%d %H:%M:%S"
}

class ValidadoresDisciplinarios:
    """Clase para validar y normalizer diferentes tipos de datos según requerimientos de la Defensoría."""

    @staticmethod
    def _normalize_string(valor: str) -> str:
        """Normaliza una cadena eliminando acentos y caracteres especiales."""
        if not valor:
            return ""
        return unicodedata.normalize('NFKD', valor.strip()).encode('ASCII', 'ignore').decode('ASCII')

    @staticmethod
    def _clean_numeric(value: str) -> str:
        """Limpia valores numéricos eliminando caracteres no deseados."""
        return re.sub(r"[^\d.-]", "", str(value))

    def _normalizar_para_validacion(self, valor: str, reemplazos: Dict[str, str]) -> Tuple[str, bool]:
        valor_normalizado, _ = self.validar_cadena_caracteres_especiales(valor)

        valor_normalizado = reemplazos.get(valor_normalizado, valor_normalizado)

        return valor_normalizado

    def validar_entero(self, valor: str) -> Tuple[str, bool]:
        valor_limpio = self._clean_numeric(valor)
        es_valido = re.fullmatch(r"^-?\d+$", valor_limpio) is not None
        return valor_limpio, es_valido

    def validar_flotante(self, valor: str) -> Tuple[str, bool]:
        valor_limpio = self._clean_numeric(valor)
        es_valido = re.fullmatch(r"^-?\d+(\.\d+)?$", valor_limpio) is not None
        return valor_limpio, es_valido

    def validar_date(self, valor: str, formato_salida: str = 'date') -> Tuple[str, bool]:

        valor_original = valor
        if valor is None:
            return "", False

        valor = str(valor).strip()
        if not valor:
            return "", False
        


        formatos_intento = [
        ('datetime', DATE_FORMATS['datetime'], False),
        ('date', DATE_FORMATS['date'], False),
        ('date_YY', DATE_FORMATS['date_YY'], False),
        ('date_dd_mm_yyyy', DATE_FORMATS['date_dd_mm_yyyy'], False),
        ('date_range_iso', r'^(\d{4}-\d{2}-\d{2})(?: - \d{4}-\d{2}-\d{2})?$', True),  # YYYY-MM-DD range
        ('date_range_dmy', r'^(\d{1,2}/\d{1,2}/\d{4})(?: - \d{1,2}/\d{1,2}/\d{4})?$', True)  # DD/MM/YYYY range
    ]

        dt = None
        formato_detectado = None

        for formato, date_format, es_rango in formatos_intento:
            try:
                if es_rango:
                    match = re.match(date_format, valor)
                    if not match:
                        continue
                    fecha_str = match.group(1)

                    if formato == 'date_range_dmy':
                        dt = datetime.strptime(fecha_str, DATE_FORMATS['date_dd_mm_yyyy'])
                    else:
                        dt = datetime.strptime(fecha_str, DATE_FORMATS['date'])

                    formato_detectado = formato.split('_')[1]  # 'dmy' o 'iso'
                    break
                else:
                    dt = datetime.strptime(valor, date_format)
                    formato_detectado = formato
                    break
            except (ValueError, TypeError):
                continue
        
        if dt is None:
            try:
                excel_num = float(valor)

                if 0 <= excel_num <= 100000:
                    base_date = datetime(1899, 12, 30)
                    fecha = (base_date + timedelta(days=excel_num))
                    if formato_salida == 'date_dd_mm_yyyy':
                        return fecha.strftime('%d/%m/%Y'), True
                    elif formato_salida == 'date_YY':
                        return fecha.strftime('%y-%m-%d'), True
                    elif formato_salida == 'datetime':
                        return fecha.strftime('%Y-%m-%d %H:%M:%S'), True
                    else:  # formato 'date' por defecto
                        return fecha.strftime('%Y-%m-%d'), True
                return "", False
            except (ValueError, TypeError):
                return "", False

        formato_output = DATE_FORMATS.get(formato_salida, DATE_FORMATS['date'])

        try:
            if formato_detectado == 'datetime' and formato_salida in ['date', 'date_dd_mm_yyyy']:
                if dt.time() == time.min:
                    return dt.strftime(DATE_FORMATS[formato_salida]), True
                return valor, True

            elif formato_detectado in ['date', 'date_dd_mm_yyyy'] and formato_salida == 'datetime':
                return f"{dt.strftime(DATE_FORMATS['date'])} 00:00:00", True

            return dt.strftime(formato_output), True

        except ValueError:
            return valor, False

    def limpiar_nit(self, valor: str) -> Tuple[str, bool]:
        if not valor or str(valor).strip().lower() in {'nan', 'null', '', 'sin registro',
        'desconocido', 'no aplica', 'ninguna', 'no registra', 'sin', 'sin id'
        ''}:
            return "", False
        valor_limpio = str(valor).strip()
        if re.fullmatch(r'[a-zA-Z\s]+', valor_limpio):
            return valor_limpio, True
        valor_limpio = valor_limpio.replace(".000000", "")
        if re.fullmatch(r'\d+(?:-\d+)*', valor_limpio):
            valor_limpio = valor_limpio.split("-")[0]
        es_valido = not re.fullmatch(r"^[a-zA-Z]+$", valor_limpio) is not None
        if es_valido:
            return valor_limpio, es_valido
        else:
            return valor_limpio, es_valido

    def validar_cadena_caracteres_especiales(self, valor: str) -> Tuple[str, bool]:
        valor_normalizado = self._normalize_string(valor)
        valor_normalizado = valor_normalizado.replace('.', '').replace(',', '')
        return valor_normalizado, True

    def validar_direccion_seccional(self, valor):
        """
        Valida si un valor corresponde a una dirección seccional válida.
        Retorna True si es válido, False en caso contrario.
        """
        if '-' in valor:
            valor = valor.split('-', 1)[1]
        normalizado = self._normalizar_para_validacion(valor.lower(), VALORES_REEMPLAZO_DIRECCION_SECCIONAL)
        es_valido = normalizado in VALORES_DIRECCION_SECCIONAL
        return normalizado, True

    def validar_proceso(self, valor):
        normalizado = self._normalizar_para_validacion(valor.upper(), VALORES_REEMPLAZO_PROCESO)
        es_valido = normalizado in VALORES_PROCESO
        return normalizado, True
 