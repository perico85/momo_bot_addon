ARG BUILD_FROM=ghcr.io/home-assistant/aarch64-base:latest
FROM $BUILD_FROM

# 1. Instalar dependencias del sistema críticas
RUN apk add --no-cache --virtual .build-deps \
    build-base \
    python3-dev \
    libffi-dev \
    openssl-dev \
    && apk add --no-cache \
    python3 \
    py3-pip \
    sqlite \
    sqlite-dev

# 2. Configurar entorno Python optimizado
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

# 3. Copiar solo requirements.txt primero (para mejor caché de Docker)
COPY requirements.txt /tmp/
RUN pip3 install --upgrade pip && \
    pip3 install wheel && \
    pip3 install -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

# 4. Copiar el resto de archivos
COPY run.sh /
COPY momo_bot.py /

# 5. Limpieza final
RUN apk del .build-deps && \
    rm -rf /var/cache/apk/* && \
    chmod +x /run.sh

WORKDIR /
CMD ["/run.sh"]
