ARG BUILD_FROM
FROM $BUILD_FROM

# Instalar dependencias del sistema
RUN apk add --no-cache python3 py3-pip sqlite

# Copiar archivos necesarios
COPY requirements.txt /
COPY momo_bot.py /
COPY run.sh /

# Instalar dependencias de Python
RUN pip3 install --no-cache-dir -r /requirements.txt

# Dar permisos de ejecución
RUN chmod a+x /run.sh

# Directorio de trabajo
WORKDIR /

CMD [ "/run.sh" ]
