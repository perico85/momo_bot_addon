#!/bin/sh

# Establecer nivel de log desde la configuración
LOG_LEVEL=${LOG_LEVEL:-info}

# Exportar variables de entorno
export BOT_TOKEN=${BOT_TOKEN}

# Verificar que el token está configurado
if [ -z "$BOT_TOKEN" ]; then
  echo "ERROR: BOT_TOKEN no está configurado. Por favor configúralo en las opciones del addon."
  exit 1
fi

# Iniciar el bot
echo "Iniciando MoMo Bot con nivel de log $LOG_LEVEL..."
exec python3 /momo_bot.py
