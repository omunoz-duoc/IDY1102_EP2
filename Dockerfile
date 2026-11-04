FROM python:3.14-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates unixodbc git \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb -o /tmp/ms.deb \
    && dpkg -i /tmp/ms.deb && rm /tmp/ms.deb \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt requirements-notebook.txt requirements-dbt-lock.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt -r requirements-notebook.txt \
    && python -m venv /opt/dbt \
    && /opt/dbt/bin/python -m pip install --no-cache-dir -r requirements-dbt-lock.txt \
    && python -m pip check && /opt/dbt/bin/python -m pip check
ENV DBT_EXECUTABLE=/opt/dbt/bin/dbt
COPY . .
RUN useradd --create-home --uid 1000 notebook && chown -R notebook:notebook /app
USER notebook
EXPOSE 8888
CMD ["python", "-m", "dataretail.start"]
