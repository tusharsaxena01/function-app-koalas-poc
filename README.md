# Koalas (pyspark.pandas) Azure Function — POC

An Azure Functions (Python v2 programming model) app that accepts a tabular file
(`.xlsx` or `.csv`) over HTTP, processes it with **`pyspark.pandas`** (the former
Koalas API, now part of PySpark), and returns the transformed data as JSON.

> **Key fact:** This app runs an embedded **Apache Spark** engine, which requires a
> **Java (JVM)** runtime. That single fact drives most of the prerequisites and the
> Azure deployment approach below.

---

## 1. What it does

| Function | Method | Route | Purpose |
| --- | --- | --- | --- |
| `Koalas_poc` | GET | `/api/Koalas_poc?name=<name>` | Simple health/hello check. |
| `Koalas_xlsx_poc` | POST | `/api/Koalas_xlsx_poc` | Parse an uploaded `.xlsx`/`.csv`, transform with `pyspark.pandas`, return JSON. |

`Koalas_xlsx_poc` transformation:
1. Reads raw request body bytes.
2. Detects format from the `Content-Type` header, falling back to the file's magic
   bytes (`.xlsx` files are ZIP archives starting with `PK\x03\x04`).
3. Loads into a `pyspark.pandas` DataFrame.
4. Fills numeric `NaN`s with the column mean.
5. Adds a `row_numeric_sum` column (sum of all numeric columns per row).
6. Returns the result as JSON (`orient="records"`).

---

## 2. Prerequisites

### Common (local and Azure)
- **Python 3.11** (64-bit). **Important:** Python 3.12+ is not compatible with
  `pyspark 3.5.1`, and Python 3.14 additionally breaks Azure Functions Core Tools.
  Use 3.11.
- **Java (JDK) 17** — required by PySpark. JDK 8/11/17 all work with Spark 3.5;
  this POC is verified on **Temurin JDK 17**.

### Local development (Windows)
- [Azure Functions Core Tools v4](https://learn.microsoft.com/azure/azure-functions/functions-run-local)
  (`func`), installed e.g. via `npm i -g azure-functions-core-tools@4`.
- [Node.js](https://nodejs.org/) (only needed to install Core Tools via npm).
- (Optional) [Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)
  storage emulator. Not required for HTTP-only triggers — you can ignore the
  `AzureWebJobsStorage` "Unhealthy" log locally.
- (Optional) [Azure CLI](https://learn.microsoft.com/cli/azure/) for deployment.

### Azure deployment
- An **Azure subscription**.
- Because PySpark needs Java, the app must be deployed as a **custom Docker
  container** (Java cannot be added to the stock Python Functions image). This
  requires an **Elastic Premium (EP)** or **Dedicated (App Service)** plan — the
  Linux **Consumption** plan does **not** support custom containers.
- **Azure Container Registry (ACR)** (or Docker Hub) to host the image.
- Docker Desktop (to build the image locally) — or build in CI / ACR Tasks.

---

## 3. Project structure

```
functionapp_koalas/
├── function_app.py       # The functions (Python v2 model) + Spark setup
├── host.json             # Functions host config (extension bundle v4)
├── local.settings.json   # Local-only settings (NOT deployed)
├── requirements.txt      # Python dependencies (pinned)
├── Dockerfile            # For Azure custom-container deployment (bundles Java)
├── .dockerignore
└── README.md             # This file
```

---

## 4. requirements.txt

```text
azure-functions
pandas==2.2.3
numpy==1.26.4
pyarrow==15.0.2
pyspark==3.5.1
openpyxl
```

**Why these are pinned:**
- `pyspark==3.5.1` — the Spark engine providing `pyspark.pandas`.
- `numpy==1.26.4` — `pyspark.pandas` 3.5.x uses `np.NaN`, which was **removed** in
  NumPy 2.0, so NumPy must stay `<2.0`.
- `pandas==2.2.3` — kept `<3.0` for compatibility with `pyspark.pandas` 3.5.x.
- `pyarrow==15.0.2` — required for efficient pandas ⇄ Spark conversion.
- `openpyxl` — engine used by `pandas.read_excel` for `.xlsx`.

---

## 5. Run locally (Windows)

### 5.1 Install Java 17 (one-time)
Install any JDK 17 and note its path. This POC uses a portable Temurin JDK at:

```
%LOCALAPPDATA%\Java\jdk-17.0.20.1+1
```

`function_app.py` automatically sets `JAVA_HOME` to that path **if `JAVA_HOME` is
not already set**. If your JDK is elsewhere, either:
- set `JAVA_HOME` yourself (recommended), or
- update the candidate path near the top of `function_app.py`.

Set `JAVA_HOME` persistently (recommended):
```powershell
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Path\To\jdk-17", "User")
[Environment]::SetEnvironmentVariable(
  "PATH",
  "$([Environment]::GetEnvironmentVariable('PATH','User'));%JAVA_HOME%\bin",
  "User"
)
```
> After changing user environment variables, **fully restart VS Code** (not just the
> terminal) — integrated terminals inherit the environment captured when VS Code
> started. The in-code `JAVA_HOME` fallback exists precisely so the app still works
> before that restart.

Verify Java:
```powershell
java -version   # should report 17.x
```

### 5.2 Create the virtual environment & install deps
```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 5.3 Start the host
```powershell
.\.venv\Scripts\Activate.ps1
func start
```
You should see both functions listed:
```
Koalas_poc:      http://localhost:7071/api/Koalas_poc
Koalas_xlsx_poc: [POST] http://localhost:7071/api/Koalas_xlsx_poc
```

> **Note:** The first request after each start incurs a one-time JVM/Spark warm-up
> (~15–20s). Subsequent requests reuse the same Spark session and are fast.

---

## 6. Test it

**Health check (GET):**
```bash
curl "http://localhost:7071/api/Koalas_poc?name=Koalas"
```

**Upload a CSV (POST):**
```bash
curl --location "http://localhost:7071/api/Koalas_xlsx_poc" ^
  --header "Content-Type: text/csv" ^
  --data-binary "@C:/path/to/data.csv"
```

**Upload an XLSX (POST):**
```bash
curl --location "http://localhost:7071/api/Koalas_xlsx_poc" ^
  --header "Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ^
  --data-binary "@C:/path/to/data.xlsx"
```

Example response for `col1,col2` = `(1,2),(3,),(5,6)`:
```json
[
  {"col1":1,"col2":2.0,"row_numeric_sum":3.0},
  {"col1":3,"col2":4.0,"row_numeric_sum":7.0},
  {"col1":5,"col2":6.0,"row_numeric_sum":11.0}
]
```
(`col2`'s missing value was filled with the column mean `4.0`.)

---

## 7. Deploy to Azure (custom container — required for Java)

The standard `func azure functionapp publish` flow uses the stock Python image,
which has **no Java**, so PySpark would fail with `JAVA_GATEWAY_EXITED`. Deploy a
custom container that bundles a JDK instead.

### 7.1 Build & push the image
```powershell
# Variables
$ACR = "myregistry"          # ACR name (without .azurecr.io)
$IMAGE = "koalas-func:v1"

az acr login --name $ACR
docker build -t "$ACR.azurecr.io/$IMAGE" .
docker push "$ACR.azurecr.io/$IMAGE"
```

### 7.2 Create the Function App (Premium plan) on the container
```powershell
$RG   = "rg-koalas"
$LOC  = "eastus"
$PLAN = "plan-koalas"
$APP  = "koalas-func-app"      # must be globally unique
$STG  = "koalasfuncstg$((Get-Random))"

az group create -n $RG -l $LOC

az storage account create -n $STG -g $RG -l $LOC --sku Standard_LRS

# Elastic Premium plan (Linux) — supports custom containers
az functionapp plan create -n $PLAN -g $RG -l $LOC `
  --is-linux --sku EP1

az functionapp create -n $APP -g $RG `
  --plan $PLAN `
  --storage-account $STG `
  --functions-version 4 `
  --runtime python `
  --deployment-container-image-name "$ACR.azurecr.io/$IMAGE"

# Let the Function App pull from ACR
az functionapp config container set -n $APP -g $RG `
  --docker-custom-image-name "$ACR.azurecr.io/$IMAGE" `
  --docker-registry-server-url "https://$ACR.azurecr.io"
```

### 7.3 App settings
`JAVA_HOME` is baked into the image via the Dockerfile, so no extra setting is
needed for Java. Set any app config you need:
```powershell
az functionapp config appsettings set -n $APP -g $RG `
  --settings "FUNCTIONS_WORKER_RUNTIME=python"
```

### 7.4 Invoke
```
https://<APP>.azurewebsites.net/api/Koalas_poc?name=Koalas
https://<APP>.azurewebsites.net/api/Koalas_xlsx_poc   (POST)
```

> **Sizing:** Spark inside a Function is memory-hungry. Use at least an **EP1**
> (3.5 GB) instance; scale up if you process larger files.

---

## 8. Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `JAVA_GATEWAY_EXITED` / `Java not found and JAVA_HOME ... not set` | JVM not visible to the process | Install JDK 17, set `JAVA_HOME`, restart VS Code. In a container, ensure the Dockerfile `ENV JAVA_HOME` points at the bundled JDK. |
| `System.ArgumentException: Destination is too short` on `func start` | Core Tools running an unsupported global Python (3.12+/3.14) | Activate the 3.11 `.venv` before `func start`. |
| `ModuleNotFoundError: No module named 'databricks'` | Legacy `databricks.koalas` import | Use `import pyspark.pandas as ks` (already done). |
| `File is not a zip file` on CSV upload | CSV parsed as `.xlsx` | Send `Content-Type: text/csv` (format detection already handles this). |
| `Cannot combine ... enable 'compute.ops_on_diff_frames'` | pandas-on-Spark cross-frame assignment | Already enabled via `ks.set_option("compute.ops_on_diff_frames", True)`. |
| Very slow / worker restarts | Spark default 200 partitions | Already mitigated with a lean `local[1]` session. First call is warm-up only. |
| `WARN Shell: Did not find winutils.exe` / `HADOOP_HOME ... unset` | Windows Hadoop native libs missing | Harmless for this local POC; ignore. |
| `AzureWebJobsStorage` "Unhealthy" locally | Azurite emulator not running | Harmless for HTTP triggers; start Azurite if you need storage bindings. |

---

## 9. Notes & caveats
- This is a **POC**. Running Spark inside an Azure Function is unusual and resource
  heavy; for production tabular processing at scale, prefer **Azure Synapse**,
  **Azure Databricks**, or **Fabric**. Use plain `pandas` if you don't need Spark.
- `local.settings.json` is for local use only and is **not** deployed. Never commit
  secrets to it.
- The Spark session is created **once** at module import and reused for all
  requests in that worker process.

## Important callout in the manual
The single most important fact for Azure: PySpark requires Java, and the stock Azure Functions Python image has none. So the standard `func azure functionapp publish` won't work — you must deploy the **custom container** (Dockerfile provided) on an Elastic Premium (EP1+) or Dedicated plan, not the Linux Consumption plan. The README walks through building/pushing to ACR and creating the app.

One honest note I included: running Spark inside a Function is unusual and heavy — for real workloads, Databricks/Synapse/Fabric (or plain pandas if Spark isn't needed) are better fits. This POC works, but keep that in mind.

