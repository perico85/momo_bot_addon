ARG BUILD_FROM
FROM $BUILD_FROM

# Instalar dependencias del sistema
RUN apk add --no-cache \
    python3 \
    py3-pip \
    sqlite \
    build-base \
    python3-dev \
    libffi-dev \
    openssl-dev

# Copiar archivos
COPY requirements.txt /
COPY momo_bot.py /
COPY run.sh /

# Instalar dependencias
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir wheel && \
    pip3 install --no-cache-dir -r /requirements.txt

# Limpiar para reducir tamaño
RUN apk del build-base python3-dev libffi-dev openssl-dev && \
    rm -rf /var/cache/apk/*

# Permisos
RUN chmod a+x /run.sh

WORKDIR /
CMD ["/run.sh"]
