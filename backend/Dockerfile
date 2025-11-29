FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .

# Instalar TODAS las dependencias de una vez
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .

# Crear directorio de datos
RUN mkdir -p data

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]