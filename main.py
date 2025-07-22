from fastapi import FastAPI, File, UploadFile, Body, Form
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import pandas as pd
import tempfile
from typing import List
import zipfile
import csv

app = FastAPI()

# Permitir CORS para el frontend (ajusta el origen si es necesario)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/unir-csv")
def unir_csv(files: List[UploadFile] = File(...)):
    def clean_value(val):
        if isinstance(val, str) and val.endswith(".0"):
            return val[:-2]
        return val

    dataframes = []
    for file in files:
        try:
            # Guardar archivo temporalmente
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                tmp.write(file.file.read())
                tmp_path = tmp.name
            df = pd.read_csv(tmp_path, delimiter="|", dtype=str)
            df = df.applymap(clean_value)
            dataframes.append(df)
            os.unlink(tmp_path)
        except Exception as e:
            return JSONResponse(
                status_code=500,
                content={"error": f"Error al leer {file.filename}: {str(e)}"},
            )

    if not dataframes:
        return JSONResponse(
            status_code=400, content={"error": "No se cargó ningún archivo válido."}
        )
    else:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as out_tmp:
            combined_df = pd.concat(dataframes, ignore_index=True)
            combined_df.to_csv(out_tmp.name, sep="|", index=False)
            out_path = out_tmp.name
        return FileResponse(
            out_path,
            filename="consolidado_coljuegos_pqr_2021.csv",
            media_type="text/csv",
        )


@app.post("/api/v1/csv-a-otro-separador/")
def csv_a_otro_separador(body: dict = Body(...)):
    import io
    import re

    lista_archivos_csv_at = body.get("lista_archivos_csv_at", [])
    antiguo_separador = body.get("antiguo_separador", "|@")
    nuevo_separador = body.get("nuevo_separador", "|")

    archivos_convertidos = []
    temp_dir = tempfile.mkdtemp()
    for nombre_archivo_csv_at in lista_archivos_csv_at:
        if not os.path.exists(nombre_archivo_csv_at):
            continue
        try:
            nombre_archivo_base, _ = os.path.splitext(
                os.path.basename(nombre_archivo_csv_at)
            )
            nombre_archivo_csv_pipe = nombre_archivo_base + ".csv"
            archivo_salida = os.path.join(temp_dir, nombre_archivo_csv_pipe)
            with open(
                nombre_archivo_csv_at, "r", newline="", encoding="utf-8"
            ) as infile, open(
                archivo_salida, "w", newline="", encoding="utf-8"
            ) as outfile:
                coincidencia_fecha = re.search(r"I(\d{8})", nombre_archivo_csv_at)
                mes_reporte = "Desconocido"
                if coincidencia_fecha:
                    fecha_str = coincidencia_fecha.group(1)
                    anio = fecha_str[:4]
                    mes = fecha_str[4:6]
                    mes_reporte = f"{mes}_{anio}"
                primera_linea = infile.readline().strip()
                cabecera = primera_linea.split(antiguo_separador)
                nueva_cabecera = ["nombre_archivo", "mes_reporte"] + cabecera
                outfile.write(nuevo_separador.join(nueva_cabecera) + "\n")
                for line in infile:
                    campos = line.strip().split(antiguo_separador)
                    nueva_linea = [
                        os.path.basename(nombre_archivo_csv_at),
                        mes_reporte,
                    ] + campos
                    outfile.write(nuevo_separador.join(nueva_linea) + "\n")
            archivos_convertidos.append(archivo_salida)
        except UnicodeDecodeError:
            try:
                with open(
                    nombre_archivo_csv_at, "r", newline="", encoding="latin-1"
                ) as infile_latin, open(
                    archivo_salida, "w", newline="", encoding="utf-8"
                ) as outfile:
                    coincidencia_fecha = re.search(r"I(\d{8})", nombre_archivo_csv_at)
                    mes_reporte = "Desconocido"
                    if coincidencia_fecha:
                        fecha_str = coincidencia_fecha.group(1)
                        anio = fecha_str[:4]
                        mes = fecha_str[4:6]
                        mes_reporte = f"{mes}_{anio}"
                    primera_linea = infile_latin.readline().strip()
                    cabecera = primera_linea.split(antiguo_separador)
                    nueva_cabecera = ["nombre_archivo", "mes_reporte"] + cabecera
                    outfile.write(nuevo_separador.join(nueva_cabecera) + "\n")
                    for line in infile_latin:
                        campos = line.strip().split(antiguo_separador)
                        nueva_linea = [
                            os.path.basename(nombre_archivo_csv_at),
                            mes_reporte,
                        ] + campos
                        outfile.write(nuevo_separador.join(nueva_linea) + "\n")
                archivos_convertidos.append(archivo_salida)
            except Exception as e_latin:
                continue
        except Exception as e:
            continue
    if not archivos_convertidos:
        return JSONResponse(
            status_code=400, content={"error": "No se pudo convertir ningún archivo."}
        )
    # Crear ZIP
    zip_path = os.path.join(temp_dir, "csv_convertidos.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file in archivos_convertidos:
            zipf.write(file, os.path.basename(file))
    return FileResponse(
        zip_path, filename="csv_convertidos.zip", media_type="application/zip"
    )


@app.post("/api/v1/csv-a-otro-separador-upload/")
def csv_a_otro_separador_upload(
    files: list[UploadFile] = File(...),
    antiguo_separador: str = Form("|@"),
    nuevo_separador: str = Form("|"),
):
    import re
    import shutil

    temp_dir = tempfile.mkdtemp()
    archivos_convertidos = []
    for file in files:
        # Guardar archivo temporalmente
        temp_input_path = os.path.join(temp_dir, file.filename)
        with open(temp_input_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        try:
            nombre_archivo_base, _ = os.path.splitext(os.path.basename(file.filename))
            nombre_archivo_csv_pipe = nombre_archivo_base + ".csv"
            archivo_salida = os.path.join(temp_dir, nombre_archivo_csv_pipe)
            with open(
                temp_input_path, "r", newline="", encoding="utf-8"
            ) as infile, open(
                archivo_salida, "w", newline="", encoding="utf-8"
            ) as outfile:
                coincidencia_fecha = re.search(r"I(\d{8})", file.filename)
                mes_reporte = "Desconocido"
                if coincidencia_fecha:
                    fecha_str = coincidencia_fecha.group(1)
                    anio = fecha_str[:4]
                    mes = fecha_str[4:6]
                    mes_reporte = f"{mes}_{anio}"
                primera_linea = infile.readline().strip()
                cabecera = primera_linea.split(antiguo_separador)
                nueva_cabecera = ["nombre_archivo", "mes_reporte"] + cabecera
                outfile.write(nuevo_separador.join(nueva_cabecera) + "\n")
                for line in infile:
                    campos = line.strip().split(antiguo_separador)
                    nueva_linea = [file.filename, mes_reporte] + campos
                    outfile.write(nuevo_separador.join(nueva_linea) + "\n")
            archivos_convertidos.append(archivo_salida)
        except UnicodeDecodeError:
            try:
                with open(
                    temp_input_path, "r", newline="", encoding="latin-1"
                ) as infile_latin, open(
                    archivo_salida, "w", newline="", encoding="utf-8"
                ) as outfile:
                    coincidencia_fecha = re.search(r"I(\d{8})", file.filename)
                    mes_reporte = "Desconocido"
                    if coincidencia_fecha:
                        fecha_str = coincidencia_fecha.group(1)
                        anio = fecha_str[:4]
                        mes = fecha_str[4:6]
                        mes_reporte = f"{mes}_{anio}"
                    primera_linea = infile_latin.readline().strip()
                    cabecera = primera_linea.split(antiguo_separador)
                    nueva_cabecera = ["nombre_archivo", "mes_reporte"] + cabecera
                    outfile.write(nuevo_separador.join(nueva_cabecera) + "\n")
                    for line in infile_latin:
                        campos = line.strip().split(antiguo_separador)
                        nueva_linea = [file.filename, mes_reporte] + campos
                        outfile.write(nuevo_separador.join(nueva_linea) + "\n")
                archivos_convertidos.append(archivo_salida)
            except Exception as e_latin:
                continue
        except Exception as e:
            continue
    if not archivos_convertidos:
        return JSONResponse(
            status_code=400, content={"error": "No se pudo convertir ningún archivo."}
        )
    # Crear ZIP
    zip_path = os.path.join(temp_dir, "csv_convertidos.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file in archivos_convertidos:
            zipf.write(file, os.path.basename(file))
    return FileResponse(
        zip_path, filename="csv_convertidos.zip", media_type="application/zip"
    )


@app.post("/api/v1/sav-a-csv-upload/")
def sav_a_csv_upload(files: list[UploadFile] = File(...)):
    import shutil

    temp_dir = tempfile.mkdtemp()
    archivos_convertidos = []
    for file in files:
        # Guardar archivo temporalmente
        temp_input_path = os.path.join(temp_dir, file.filename)
        with open(temp_input_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        try:
            nombre_archivo_base, _ = os.path.splitext(os.path.basename(file.filename))
            nombre_archivo_csv = nombre_archivo_base + ".csv"
            archivo_salida = os.path.join(temp_dir, nombre_archivo_csv)
            df = pd.read_spss(temp_input_path)
            df.to_csv(archivo_salida, index=False, sep="|")
            archivos_convertidos.append(archivo_salida)
        except Exception as e:
            continue
    if not archivos_convertidos:
        return JSONResponse(
            status_code=400,
            content={"error": "No se pudo convertir ningún archivo .sav."},
        )
    # Crear ZIP
    zip_path = os.path.join(temp_dir, "csv_convertidos.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file in archivos_convertidos:
            zipf.write(file, os.path.basename(file))
    return FileResponse(
        zip_path, filename="csv_convertidos.zip", media_type="application/zip"
    )


@app.post("/api/v1/txt-a-csv-upload/")
def txt_a_csv_upload(
    files: list[UploadFile] = File(...),
    separador_entrada: str = Form("|"),
    separador_salida: str = Form("|"),
):
    import shutil

    temp_dir = tempfile.mkdtemp()
    archivos_convertidos = []
    for file in files:
        # Guardar archivo temporalmente
        temp_input_path = os.path.join(temp_dir, file.filename)
        with open(temp_input_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        try:
            nombre_archivo_base, _ = os.path.splitext(os.path.basename(file.filename))
            nombre_archivo_csv = nombre_archivo_base + "_CONVERTIDO.csv"
            archivo_salida = os.path.join(temp_dir, nombre_archivo_csv)
            with open(temp_input_path, "r", encoding="utf-8") as txt_file:
                lines = txt_file.readlines()
                if not lines:
                    continue
                with open(
                    archivo_salida, "w", newline="", encoding="utf-8"
                ) as csv_file:
                    writer = csv.writer(csv_file, delimiter=separador_salida)
                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue
                        row = [field.strip() for field in line.split(separador_entrada)]
                        writer.writerow(row)
            archivos_convertidos.append(archivo_salida)
        except Exception as e:
            continue
    if not archivos_convertidos:
        return JSONResponse(
            status_code=400,
            content={"error": "No se pudo convertir ningún archivo .txt."},
        )
    # Crear ZIP
    zip_path = os.path.join(temp_dir, "csv_convertidos.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file in archivos_convertidos:
            zipf.write(file, os.path.basename(file))
    return FileResponse(
        zip_path, filename="csv_convertidos.zip", media_type="application/zip"
    )


@app.post("/api/v1/xlsx-a-csv-con-columna-mes-de-reporte-upload/")
def xlsx_a_csv_upload(
    files: list[UploadFile] = File(...), separador_salida: str = Form("|")
):
    import shutil
    import re

    temp_dir = tempfile.mkdtemp()
    archivos_convertidos = []
    MESES = {
        "Enero": "01",
        "Febrero": "02",
        "Marzo": "03",
        "Abril": "04",
        "Mayo": "05",
        "Junio": "06",
        "Julio": "07",
        "Agosto": "08",
        "Septiembre": "09",
        "Octubre": "10",
        "Noviembre": "11",
        "Diciembre": "12",
    }
    MESES_LOWER = {
        "enero": "1",
        "febrero": "2",
        "marzo": "3",
        "abril": "4",
        "mayo": "5",
        "junio": "6",
        "julio": "7",
        "agosto": "8",
        "septiembre": "9",
        "octubre": "10",
        "noviembre": "11",
        "diciembre": "12",
    }
    for file in files:
        temp_input_path = os.path.join(temp_dir, file.filename)
        with open(temp_input_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        try:
            nombre_archivo_base, _ = os.path.splitext(os.path.basename(file.filename))
            nombre_archivo_csv = (
                nombre_archivo_base.replace(" ", "_")
                .replace("de_", "")
                .replace("de", "")
                .replace(".", "_")
                + ".csv"
            )
            archivo_salida = os.path.join(temp_dir, nombre_archivo_csv)
            coincidencia = re.search(r"Mes_([A-Za-z]+)_(\d{4})", nombre_archivo_csv)
            coincidencia_2 = re.search(
                r"Mes_de_([A-Za-z]+)_de_(\d{4})", nombre_archivo_csv
            )
            coincidencia_fecha = re.search(r"I(\d{8})", nombre_archivo_csv)
            coincidencia_pqr = re.search(r"(\d{4})-(\d{2})", nombre_archivo_csv)
            coincidencia_dynamics = re.search(r"_(\w+)_(\d{4})_", nombre_archivo_csv)
            mes_reporte = "Desconocido"
            if coincidencia:
                mes_nombre = coincidencia.group(1).capitalize()
                anio = coincidencia.group(2)
                mes_numero = MESES.get(mes_nombre)
                if mes_numero:
                    mes_reporte = f"{mes_numero}_{anio}"
            elif coincidencia_2:
                mes_nombre = coincidencia_2.group(1).capitalize()
                anio = coincidencia_2.group(2)
                mes_numero = MESES.get(mes_nombre)
                if mes_numero:
                    mes_reporte = f"{mes_numero}_{anio}"
            if coincidencia_fecha:
                fecha_str = coincidencia_fecha.group(1)
                anio = fecha_str[:4]
                mes = fecha_str[4:6]
                mes_reporte = f"{mes}_{anio}"
            if coincidencia_dynamics:
                mes_nombre = coincidencia_dynamics.group(1).lower()
                anio = coincidencia_dynamics.group(2)
                mes_numero = MESES_LOWER.get(mes_nombre)
                if mes_numero:
                    mes_reporte = f"{mes_numero}_{anio}"
            if coincidencia_pqr:
                anio = coincidencia_pqr.group(1)
                mes_numero = coincidencia_pqr.group(2)
                mes_numero_sin_cero = str(int(mes_numero))
                mes_reporte = f"{mes_numero_sin_cero}_{anio}"
            df = pd.read_excel(temp_input_path)
            if not df.empty and len(df) > 0:
                df = df.iloc[1:]
            df.insert(0, "nombre_archivo", nombre_archivo_base)
            df.insert(1, "mes_reporte", mes_reporte)
            df.to_csv(
                archivo_salida, index=False, sep=separador_salida, encoding="utf-8"
            )
            archivos_convertidos.append(archivo_salida)
        except Exception as e:
            continue
    if not archivos_convertidos:
        return JSONResponse(
            status_code=400,
            content={"error": "No se pudo convertir ningún archivo .xlsx."},
        )
    # Crear ZIP
    zip_path = os.path.join(temp_dir, "csv_convertidos.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file in archivos_convertidos:
            zipf.write(file, os.path.basename(file))
    return FileResponse(
        zip_path, filename="csv_convertidos.zip", media_type="application/zip"
    )


@app.post("/api/v1/xlsx-a-csv-upload/")
def xlsx_a_csv_con_columna_mes_de_reporte_upload(
    files: list[UploadFile] = File(...), separador_salida: str = Form("|")
):
    import shutil

    temp_dir = tempfile.mkdtemp()
    archivos_convertidos = []
    for file in files:
        temp_input_path = os.path.join(temp_dir, file.filename)
        with open(temp_input_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        try:
            nombre_archivo_base, _ = os.path.splitext(os.path.basename(file.filename))
            nombre_archivo_csv = (
                nombre_archivo_base.replace(" ", "_")
                .replace("de_", "")
                .replace("de", "")
                .replace(".", "_")
                + ".csv"
            )
            archivo_salida = os.path.join(temp_dir, nombre_archivo_csv)
            df = pd.read_excel(temp_input_path)
            df.to_csv(
                archivo_salida, index=False, sep=separador_salida, encoding="utf-8"
            )
            archivos_convertidos.append(archivo_salida)
        except Exception as e:
            continue
    if not archivos_convertidos:
        return JSONResponse(
            status_code=400,
            content={"error": "No se pudo convertir ningún archivo .xlsx."},
        )
    # Crear ZIP
    zip_path = os.path.join(temp_dir, "csv_convertidos.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file in archivos_convertidos:
            zipf.write(file, os.path.basename(file))
    return FileResponse(
        zip_path, filename="csv_convertidos.zip", media_type="application/zip"
    )


@app.post("/api/v1/unir-archivos-csv-en-xlsx-upload/")
def unir_archivos_csv_en_xlsx_upload(
    files: list[UploadFile] = File(...), separador_salida: str = Form("|")
):
    import shutil

    temp_dir = tempfile.mkdtemp()
    rutas_csv = []
    for file in files:
        temp_input_path = os.path.join(temp_dir, file.filename)
        with open(temp_input_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        rutas_csv.append(temp_input_path)
    archivo_excel_salida = os.path.join(temp_dir, "Consolidado_Final.xlsx")
    try:
        max_filas = 1048576  # Límite de filas en Excel
        excel_data = {}
        for ruta in rutas_csv:
            try:
                nombre_base = os.path.basename(ruta).replace(".csv", "")[:25]
                data = []
                with open(ruta, "r", encoding="utf-8") as archivo_csv:
                    lector_csv = csv.reader(archivo_csv, delimiter=separador_salida)
                    for fila in lector_csv:
                        data.append(fila)
                df = pd.DataFrame(data)
                num_partes = (len(df) // max_filas) + 1
                for i in range(num_partes):
                    inicio = i * max_filas
                    fin = (i + 1) * max_filas
                    nombre_hoja = f"{nombre_base}_part{i+1}"[:31]
                    excel_data[nombre_hoja] = df.iloc[inicio:fin]
            except Exception as e:
                continue
        with pd.ExcelWriter(archivo_excel_salida) as writer:
            for sheet_name, df in excel_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Error general: {e}"})
    # Crear ZIP
    zip_path = os.path.join(temp_dir, "consolidado_xlsx.zip")
    import zipfile

    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.write(archivo_excel_salida, os.path.basename(archivo_excel_salida))
    return FileResponse(
        zip_path, filename="consolidado_xlsx.zip", media_type="application/zip"
    )


@app.post("/api/v1/normalizar-columnas/coljuegos/disciplinarios/upload/")
def normalizar_columnas_coljuegos_disciplinarios_upload(
    file: UploadFile = File(...),
    nombre_archivo_salida: str = Form(...),
    nombre_archivo_errores: str = Form(...),
):
    import shutil
    import zipfile
    from repository.proyectos.COLJUEGOS.disciplinarios.transformar_columnas_disciplinarios_col import (
        CSVProcessor,
    )
    from repository.proyectos.COLJUEGOS.disciplinarios.validadores.validadores_disciplianrios import (
        ValidadoresDisciplinarios,
    )

    temp_dir = tempfile.mkdtemp()
    temp_input_path = os.path.join(temp_dir, file.filename)
    with open(temp_input_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    output_file = os.path.join(temp_dir, nombre_archivo_salida)
    error_file = os.path.join(temp_dir, nombre_archivo_errores)
    # type_mapping como en el ejemplo del script
    type_mapping = {
        "int": [],
        "float": [],
        "date": [],
        "datetime": [4, 5, 6, 7, 21, 33, 34, 35],
        "str": [
            1,
            2,
            3,
            8,
            10,
            11,
            13,
            15,
            16,
            17,
            18,
            19,
            20,
            22,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            32,
        ],
        "str-sin-caracteres-especiales": [],
        "nit": [9],
        "choice_direccion_seccional": [12],
        "choice_proceso": [14],
    }
    processor = CSVProcessor(validator=ValidadoresDisciplinarios())
    try:
        processor.process_csv(temp_input_path, output_file, error_file, type_mapping)
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"error": f"Error procesando archivo: {e}"}
        )
    # Crear ZIP con ambos archivos
    zip_path = os.path.join(temp_dir, "archivos_procesados.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.write(output_file, nombre_archivo_salida)
        zipf.write(error_file, nombre_archivo_errores)
    return FileResponse(
        zip_path, filename="archivos_procesados.zip", media_type="application/zip"
    )


@app.post("/api/v1/normalizar-columnas/coljuegos/pqr/upload/")
def normalizar_columnas_coljuegos_pqr_upload(
    file: UploadFile = File(...),
    nombre_archivo_salida: str = Form(...),
    nombre_archivo_errores: str = Form(...),
):
    import shutil
    import zipfile
    from repository.proyectos.COLJUEGOS.pqr.transformar_columnas_pqr_coljuegos import (
        CSVProcessor,
    )
    from repository.proyectos.COLJUEGOS.pqr.validadores.validadores_pqr_coljuegos import (
        ValidadoresPQRColjuegos,
    )

    temp_dir = tempfile.mkdtemp()
    temp_input_path = os.path.join(temp_dir, file.filename)
    with open(temp_input_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    output_file = os.path.join(temp_dir, nombre_archivo_salida)
    error_file = os.path.join(temp_dir, nombre_archivo_errores)
    # type_mapping como en el ejemplo del script
    type_mapping = {
        "int": [],
        "float": [],
        "date": [4, 13],
        "datetime": [],
        "str": [1, 2, 3, 5, 6, 9, 10, 11, 14, 15, 16, 17, 18, 19, 20],
        "str-sin-caracteres-especiales": [],
        "nit": [],
        "choice_clasificacion": [8],
        "choice_dependencia_asignada": [12],
        "choice_linea_negocio": [7],
    }
    processor = CSVProcessor(validator=ValidadoresPQRColjuegos())
    try:
        processor.process_csv(temp_input_path, output_file, error_file, type_mapping)
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"error": f"Error procesando archivo: {e}"}
        )
    # Crear ZIP con ambos archivos
    zip_path = os.path.join(temp_dir, "archivos_procesados.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.write(output_file, nombre_archivo_salida)
        zipf.write(error_file, nombre_archivo_errores)
    return FileResponse(
        zip_path, filename="archivos_procesados.zip", media_type="application/zip"
    )


@app.post("/api/v1/normalizar-columnas/Dian/disciplinarios/upload/")
def normalizar_columnas_dian_disciplinarios_upload(
    file: UploadFile = File(...),
    nombre_archivo_salida: str = Form(...),
    nombre_archivo_errores: str = Form(...),
):
    import shutil
    import zipfile
    from repository.proyectos.DIAN.disciplinarios.transformar_columnas_disciplinarios import (
        CSVProcessor,
    )
    from repository.proyectos.DIAN.disciplinarios.validadores.validadores_disciplinarios import (
        ValidadoresDisciplinarios,
    )

    temp_dir = tempfile.mkdtemp()
    temp_input_path = os.path.join(temp_dir, file.filename)
    with open(temp_input_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    output_file = os.path.join(temp_dir, nombre_archivo_salida)
    error_file = os.path.join(temp_dir, nombre_archivo_errores)
    # type_mapping como en el ejemplo del script
    type_mapping = {
        "int": [],
        "float": [],
        "date": [4, 5, 7],
        "datetime": [],
        "str": [
            1,
            2,
            8,
            6,
            13,
            14,
            16,
            17,
            18,
            19,
            22,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            33,
            32,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
        ],
        "str-sin-caracteres-especiales": [15],
        "nit": [9],
        "choice_departamento": [10],
        "choice_ciudad": [11],
        "choice_direccion_seccional": [12],
        "expediente": [3],
    }
    processor = CSVProcessor(validator=ValidadoresDisciplinarios())
    try:
        processor.process_csv(temp_input_path, output_file, error_file, type_mapping)
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"error": f"Error procesando archivo: {e}"}
        )
    # Crear ZIP con ambos archivos
    zip_path = os.path.join(temp_dir, "archivos_procesados.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.write(output_file, nombre_archivo_salida)
        zipf.write(error_file, nombre_archivo_errores)
    return FileResponse(
        zip_path, filename="archivos_procesados.zip", media_type="application/zip"
    )


@app.post("/api/v1/normalizar-columnas/Dian/pqr/upload/")
def normalizar_columnas_dian_pqr_upload(
    file: UploadFile = File(...),
    nombre_archivo_salida: str = Form(...),
    nombre_archivo_errores: str = Form(...),
):
    import shutil
    import zipfile
    from repository.proyectos.DIAN.PQR.transformar_columnas_pqr_muisca import (
        CSVProcessor,
    )
    from repository.proyectos.DIAN.PQR.validadores.validadores_pqr_muisca import (
        ValidadoresPQRMuisca,
    )

    temp_dir = tempfile.mkdtemp()
    temp_input_path = os.path.join(temp_dir, file.filename)
    with open(temp_input_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    output_file = os.path.join(temp_dir, nombre_archivo_salida)
    error_file = os.path.join(temp_dir, nombre_archivo_errores)
    # type_mapping como en el ejemplo del script
    type_mapping = {
        "int": [],
        "float": [],
        "date": [9],
        "datetime": [],
        "str": [
            3,
            5,
            6,
            7,
            8,
            10,
            12,
            13,
            14,
            15,
            19,
            20,
            21,
            22,
            24,
            25,
            26,
            27,
            28,
            30,
            31,
        ],
        "str-sin-caracteres-especiales": [],
        "nit": [4],
        "choice_clasificacion_muisca": [11],
        "choice_calidad_quien_solicito": [16],
        "choice_estado_solicitud": [17],
        "choice_direccion_seccional": [18, 23, 29],
    }
    processor = CSVProcessor(validator=ValidadoresPQRMuisca())
    try:
        processor.process_csv(temp_input_path, output_file, error_file, type_mapping)
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"error": f"Error procesando archivo: {e}"}
        )
    # Crear ZIP con ambos archivos
    zip_path = os.path.join(temp_dir, "archivos_procesados.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.write(output_file, nombre_archivo_salida)
        zipf.write(error_file, nombre_archivo_errores)
    return FileResponse(
        zip_path, filename="archivos_procesados.zip", media_type="application/zip"
    )
