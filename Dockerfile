# DataRetail 360 · imagen del pipeline (Python 3.14 + ODBC 18 + dbt aislado en /opt/dbt).
FROM python:3.14-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 MPLBACKEND=Agg
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates unixodbc \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb -o /tmp/ms.deb \
    && dpkg -i /tmp/ms.deb && rm /tmp/ms.deb \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# Dependencias primero: capas cacheables mientras no cambien los locks.
COPY requirements/ requirements/
RUN python -m pip install --no-cache-dir -r requirements/pipeline.lock.txt -r requirements/dev.txt \
    && python -m venv /opt/dbt \
    && /opt/dbt/bin/python -m pip install --no-cache-dir -r requirements/dbt.lock.txt \
    && python -m pip check && /opt/dbt/bin/python -m pip check

COPY pyproject.toml README.md ./
COPY src/ src/
RUN python -m pip install --no-cache-dir --no-deps . && rm -rf build
COPY dbt/ dbt/
COPY tests/unit/ tests/unit/

# Código inmutable en /app; estado de corridas (data/, artifacts/, logs/) en /workspace.
RUN useradd --create-home --uid 1000 app && mkdir /workspace && chown -R app:app /workspace /app/dbt
USER app
ENV DATARETAIL_HOME=/workspace DBT_PROJECT_DIR=/app/dbt DBT_EXECUTABLE=/opt/dbt/bin/dbt
WORKDIR /workspace
ENTRYPOINT ["dataretail360"]
CMD ["run"]
