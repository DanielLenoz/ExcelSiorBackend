from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import pandas as pd
import tempfile
from typing import List

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
