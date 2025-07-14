import re
import unicodedata
from datetime import datetime, time
from typing import Tuple, Dict, Union
from valores_choice.dependencia_dian import VALORES_DEPENDENCIA_DIAN, VALORES_REEMPLAZO_DEPENDENCIA_DIAN
from valores_choice.procedimientos import VALORES_PROCEDIMIENTOS, VALORES_REEMPLAZAR_PROCEDIMIENTOS
from valores_choice.proceso import VALORES_PROCESO

# Constantes
VALORES_MACROPROCESO = [
    "TRIBUTARIO",
    "ADUANERO",
    "CAMBIARIO",
    "TRIBUTARIO, ADUANERO Y CAMBIARIO"
]

DATE_FORMATS = {
    'date': "%Y-%m-%d",
    'date_dd_mm_yyyy': "%d/%m/%Y",
    'datetime': "%Y-%m-%d %H:%M:%S"
}

class ValidadoresDefensoria:
    """Clase para validar y normalizar diferentes tipos de datos según requerimientos de la Defensoría."""

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

        if '-' in valor_normalizado and not reemplazos in VALORES_PROCEDIMIENTOS:
            valor_normalizado = valor_normalizado.split('-', 1)[1].strip()

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

    def validar_fecha(self, valor: str, formato_salida: str = 'date') -> Tuple[str, bool]:
        valor = str(valor).strip()
        
        
        formatos_intento = [
            'datetime',
            'date',
            'date_dd_mm_yyyy',
        ]
        
        dt = None
        formato_detectado = None
        
        for formato in formatos_intento:
            try:
                dt = datetime.strptime(valor, DATE_FORMATS[formato])
                formato_detectado = formato
                break
            except ValueError:
                continue

        if dt is None:
            return "", False
        try:
            formato_output = DATE_FORMATS[formato_salida]
        except KeyError:
            # Si el formato de salida no existe, usamos 'date' como predeterminado
            formato_output = DATE_FORMATS['date']

        if formato_detectado == 'datetime' and formato_salida == 'datetime':
            if dt.time() == time.min:
                return f"{dt.strftime(DATE_FORMATS['date'])} 00:00:00", True
            return dt.strftime(formato_output), True

        if formato_detectado in ['date', 'date_dd_mm_yyyy'] and formato_salida == 'datetime':
            return f"{dt.strftime(DATE_FORMATS['date'])} 00:00:00", True

        return dt.strftime(formato_output), True

    def limpiar_nit(self, valor: str) -> Tuple[str, bool]:
        if not valor or str(valor).strip().lower() in {'nan', 'null', ''}:
            return "", False
        
        valor_limpio = str(valor).strip()
        if re.fullmatch(r'[a-zA-Z\s]+', valor_limpio):
            return "", False
        valor_limpio = valor_limpio.replace(".000000", "")
        if re.fullmatch(r'\d+(?:-\d+)*', valor_limpio):
            valor_limpio = valor_limpio.split("-")[0]
        es_valido = not re.fullmatch(r"^[a-zA-Z]+$", valor_limpio) is not None
        if es_valido:
            return valor_limpio, es_valido
        else:
            return "", es_valido

    def validar_cadena_caracteres_especiales(self, valor: str) -> Tuple[str, bool]:
        valor_normalizado = self._normalize_string(valor)
        valor_normalizado = valor_normalizado.replace('.', '').replace(',', '')
        return valor_normalizado, True

    def validar_dependencia_dian(self, valor: str) -> Tuple[str, bool]:
        normalizado = self._normalizar_para_validacion(valor.lower(), VALORES_REEMPLAZO_DEPENDENCIA_DIAN)
        es_valido = normalizado in VALORES_DEPENDENCIA_DIAN
        return normalizado, True

    def validar_procedimientos(self, valor: str) -> Tuple[str, bool]:
        normalizado = self._normalizar_para_validacion(valor.upper(), VALORES_REEMPLAZAR_PROCEDIMIENTOS)
        es_valido = normalizado in VALORES_PROCEDIMIENTOS
        return normalizado, True

    def validar_macroproceso(self, valor: str) -> Tuple[str, bool]:
        normalizado, _ = self.validar_cadena_caracteres_especiales(valor.upper())
        es_valido = normalizado in VALORES_MACROPROCESO
        return normalizado, True

    def validar_proceso(self, valor: str) -> Tuple[str, bool]:
        normalizado, _ = self.validar_cadena_caracteres_especiales(valor.lower())
        es_valido = normalizado in VALORES_PROCESO
        return normalizado, True