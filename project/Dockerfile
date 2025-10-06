FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /usr/src/app

# System deps (no software-properties-common needed)
# - build-essential for wheels that need compiling
# - GEOS/PROJ/GDAL for GeoPandas/Fiona/Rasterio stack if you use them
# - libpq5 if you talk to Postgres
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      gcc \
      g++ \
      curl \
      ca-certificates \
      libgeos-dev \
      proj-bin \
      libproj-dev \
      gdal-bin \
      libgdal-dev \
      unixodbc-dev \
      libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Create venv
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Python deps
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# App
COPY . .
CMD ["python", "./main.py"]
