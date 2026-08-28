import azure.functions as func
import datetime
import json
import io
import logging
import os

import pandas as pd
import numpy as np

# Ensure Spark can find the JVM before pyspark is imported. VS Code's integrated
# terminals inherit the environment captured when VS Code launched, so a
# persistently-set JAVA_HOME is not visible until VS Code is fully restarted.
# Resolve it here so the app works regardless of terminal environment state.
if not os.environ.get("JAVA_HOME"):
    for _candidate in (
        os.path.join(
            os.environ.get("LOCALAPPDATA", ""), "Java", "jdk-17.0.20.1+1"
        ),
    ):
        if _candidate and os.path.isfile(os.path.join(_candidate, "bin", "java.exe")):
            os.environ["JAVA_HOME"] = _candidate
            os.environ["PATH"] = (
                os.path.join(_candidate, "bin") + os.pathsep + os.environ.get("PATH", "")
            )
            break

import pyspark.pandas as ks
from pyspark.sql import SparkSession

# Build a single, lean SparkSession for the whole worker process BEFORE any
# pandas-on-Spark operation runs. This POC handles small, single-request files,
# so the Spark defaults (one task per core, 200 shuffle partitions) add large
# overhead and can stall the Functions worker. Creating the session eagerly here
# guarantees this config is honored -- otherwise pandas-on-Spark starts a default
# session first and "only runtime SQL configurations take effect".
_spark_session = (
    SparkSession.builder
    .appName("KoalasXlsxPoc")
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.default.parallelism", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

# Allow assigning a Series derived from one frame back onto another (e.g.
# kdf["new"] = kdf[cols].sum(axis=1)); pyspark.pandas blocks this by default.
ks.set_option("compute.ops_on_diff_frames", True)

app = func.FunctionApp()


@app.route(route="Koalas_poc", auth_level=func.AuthLevel.ANONYMOUS)
def Koalas_poc(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
        )


# A single SparkSession is created once at import (see top of file) and reused
# for every invocation, so it isn't rebuilt (expensive) on each request.


def _get_spark():
    return _spark_session


@app.route(route="Koalas_xlsx_poc", auth_level=func.AuthLevel.ANONYMOUS, methods=["POST"])
def Koalas_xlsx_poc(req: func.HttpRequest) -> func.HttpResponse:
    """
    Accepts a tabular file (.xlsx or .csv) in the raw request body, loads it
    into a pyspark.pandas DataFrame, performs a sample transformation, and
    returns the result as JSON. The format is chosen from the Content-Type
    header, falling back to sniffing the file's magic bytes.
    """
    logging.info('Koalas xlsx POC function processed a request.')

    # 1. Get raw bytes from the request body
    file_bytes = req.get_body()
    if not file_bytes:
        return func.HttpResponse(
            json.dumps({"error": "No file content found in request body."}),
            status_code=400,
            mimetype="application/json"
        )

    # 2. Decide the format from Content-Type, falling back to magic bytes.
    #    .xlsx files are zip archives and start with the "PK\x03\x04" signature;
    #    anything else is treated as CSV/plain text.
    content_type = (req.headers.get("Content-Type") or "").lower()
    is_xlsx = (
        "spreadsheetml" in content_type
        or content_type.endswith("/vnd.ms-excel")
        or file_bytes[:4] == b"PK\x03\x04"
    )
    is_csv = "csv" in content_type or "text/plain" in content_type

    try:
        if is_xlsx and not is_csv:
            pdf = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
        else:
            pdf = pd.read_csv(io.BytesIO(file_bytes))
    except Exception as e:
        logging.error(f"Failed to parse file: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Could not parse uploaded file: {str(e)}"}),
            status_code=400,
            mimetype="application/json"
        )

    if pdf.empty:
        return func.HttpResponse(
            json.dumps({"error": "Uploaded file contains no data."}),
            status_code=400,
            mimetype="application/json"
        )

    try:
        # Ensure Spark session is initialized (pyspark.pandas needs an active SparkSession)
        _get_spark()

        # 3. Convert to a pyspark.pandas DataFrame
        kdf = ks.from_pandas(pdf)

        # 4. Example operation(s) — adjust to whatever transformation you need.
        #    Here: fill numeric NaNs with column mean, then add a computed column
        #    for every numeric column found.
        #    NOTE: pyspark.pandas' select_dtypes(include=[np.number]) does not
        #    reliably match numeric columns, so detect them from the dtypes.
        numeric_cols = [
            col for col, dtype in kdf.dtypes.items()
            if np.issubdtype(dtype, np.number)
        ]

        for col in numeric_cols:
            col_mean = kdf[col].mean()
            kdf[col] = kdf[col].fillna(col_mean)

        if numeric_cols:
            kdf["row_numeric_sum"] = kdf[numeric_cols].sum(axis=1)

        # 5. Convert back to pandas to serialize as JSON
        #    (to_pandas() triggers the Spark computation / collects results)
        result_pdf = kdf.to_pandas()

    except Exception as e:
        logging.error(f"Koalas processing failed: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Processing failed: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

    # 6. Return dataframe as JSON (records orient, ISO dates)
    result_json = result_pdf.to_json(orient="records", date_format="iso")

    return func.HttpResponse(
        body=result_json,
        status_code=200,
        mimetype="application/json"
    )