FROM asia-southeast1-docker.pkg.dev/infra-thelivingos/thelivingos/devops/docker-images/prefect:3.x-python3.11-Y2025
WORKDIR /app

# CHANGE POINT: added ODBC Driver 17 for SQL Server installation.
# Required by pyodbc to connect to MSSQL — was not needed when using pymysql.
# Uses Microsoft's official Debian/Ubuntu package repo.
# If the base image already ships ODBC Driver 17, this block is a safe no-op.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg2 apt-transport-https unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
       -o /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash"]
