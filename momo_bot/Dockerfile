ARG BUILD_FROM
FROM $BUILD_FROM

# Instalar dependencias del sistema primero
RUN apk add --no-cache python3 py3-pip sqlite gcc python3-dev musl-dev libffi-dev openssl-dev

# Copiar archivos
COPY requirements.txt /
COPY momo_bot.py /
COPY run.sh /

# Instalar dependencias de Python con pip
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /requirements.txt

# Limpiar dependencias de compilación
RUN apk del gcc python3-dev musl-dev libffi-dev openssl-dev

# Permisos
RUN chmod a+x /run.sh

WORKDIR /
CMD [ "/run.sh" ]
