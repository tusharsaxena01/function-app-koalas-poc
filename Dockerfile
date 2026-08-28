# Azure Functions Python base image (Functions v4, Python 3.11).
# We extend it to bundle a JDK because pyspark requires a JVM.
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

# Install a JDK (Temurin 17) for PySpark.
# The default-jdk on Debian bookworm is OpenJDK 17.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Point Spark at the JVM. This satisfies the JAVA_HOME requirement so the
# in-code fallback in function_app.py is not needed inside the container.
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Azure Functions expects the app code under this path.
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY . /home/site/wwwroot
