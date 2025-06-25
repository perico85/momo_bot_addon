# -*- coding: utf-8 -*-
import os
import sys
import time
import logging
import traceback
import asyncio
from datetime import datetime

# --- Third-party Libraries ---
import pandas as pd
import requests
from pytz import timezone

import telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, Defaults

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

# =============================================================================
# --- CONFIGURATION & CONSTANTS ---
# =============================================================================

# --- Security ---
# El token ahora viene directamente del Supervisor del Add-on como variable de entorno
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    sys.stderr.write("Error: La variable de entorno BOT_TOKEN no está definida. ¿La configuraste en el Add-on?\n")
    sys.exit(1) # Salimos del script si no hay token

# --- Scheduling ---
MADRID_TZ = timezone('Europe/Madrid')
DEFAULT_NOTIFICATION_HOUR = 12
DEFAULT_NOTIFICATION_MINUTE = 0
CSV_DOWNLOAD_HOUR = 4
CSV_DOWNLOAD_MINUTE = 0
CSV_DELETE_HOUR = 3
CSV_DELETE_MINUTE = 55

# --- File & DB ---
# Usamos la carpeta /data, que es la carpeta persistente de los Add-ons
DATA_DIR = "/data"
CSV_URL = "https://momo.isciii.es/public/momo/data"
CSV_FILE = os.path.join(DATA_DIR, "momo.csv")
DB_FILE = os.path.join(DATA_DIR, 'momo_bot.db')
DB_ENGINE_URL = f'sqlite:///{DB_FILE}'

# --- Callback Data Payloads ---
CALLBACK_NACIONAL = 'nacional'
CALLBACK_COMUNIDADES = 'comunidades'
CALLBACK_PROVINCIAS = 'provincias'
CALLBACK_ACEPTAR = 'aceptar'
CALLBACK_BORRAR = 'borrar'
CALLBACK_VOLVER = 'volver'
PREFIX_COMUNIDAD = 'com_'
PREFIX_PROVINCIA = 'prov_'

# --- Customizable Messages ---
MENU_MESSAGE = "📍 *Opciones seleccionadas:*{}\n\nElija más ámbitos geográficos o pulse \"Aceptar\" para ver los datos."
PROCESSING_MESSAGE = "⏳ Realizando consulta, por favor espere..."
AUTO_SEND_MESSAGE = "✅ ¡Hecho! Envío automático programado para las *{:02d}:{:02d}* horas (CET) diariamente.\nUsa `/settime HH:MM` para cambiar la hora."
NO_DATA_MESSAGE = "🚫 No hay datos disponibles para las selecciones actuales."
HELP_MESSAGE = """
*AYUDA - BOT DE DATOS MOMO*

Este bot te permite consultar el exceso de mortalidad diario del sistema MoMo.

*¿Cómo funciona?*
1️⃣ *Selecciona*: Pulsa los botones para elegir los ámbitos que te interesan (Nacional, CCAA, Provincias). Puedes elegir varios.
2️⃣ *Consulta*: Pulsa *"Aceptar"* para ver los últimos datos disponibles para tus selecciones.
3️⃣ *Automatiza*: Tras tu primera consulta, el bot programará un envío diario automático.

*Comandos disponibles:*
- `/start` - Inicia la conversación y muestra el menú.
- `/help` - Muestra este mensaje de ayuda.
- `/settime HH:MM` - Cambia la hora de la notificación diaria. (Ej: `/settime 08:30`)
- `/borrar` - Elimina todas tus selecciones y cancela el envío automático.
"""

# =============================================================================
# --- INITIALIZATION ---
# =============================================================================

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- In-memory DataFrame ---
df_momo = None

# --- Database (SQLAlchemy) ---
Base = declarative_base()
engine = create_engine(DB_ENGINE_URL)
Session = sessionmaker(bind=engine)

class User(Base):
    __tablename__ = 'users'
    user_id = Column(Integer, primary_key=True)
    selections = Column(String, default="")
    auto_send = Column(Boolean, default=False)
    notification_hour = Column(Integer, default=DEFAULT_NOTIFICATION_HOUR)
    notification_minute = Column(Integer, default=DEFAULT_NOTIFICATION_MINUTE)

class JobsTable(Base):
    __tablename__ = 'apscheduler_jobs'
    id = Column(String(191), primary_key=True)
    next_run_time = Column(DateTime, index=True)
    job_state = Column(String)

# Aseguramos que el directorio de datos existe
os.makedirs(DATA_DIR, exist_ok=True)
Base.metadata.create_all(engine)

# --- Scheduler ---
jobstores = {'default': SQLAlchemyJobStore(engine=engine)}
scheduler = BackgroundScheduler(jobstores=jobstores, timezone=MADRID_TZ)

# =============================================================================
# --- DATABASE HELPER FUNCTIONS (SQLAlchemy) ---
# =============================================================================

def get_or_create_user(session, user_id):
    user = session.query(User).filter_by(user_id=user_id).first()
    if not user:
        user = User(user_id=user_id)
        session.add(user)
        session.commit()
    return user

# =============================================================================
# --- CORE BOT LOGIC ---
# =============================================================================

def download_and_load_csv():
    global df_momo
    logger.info("Iniciando descarga de datos...")
    try:
        response = requests.get(CSV_URL, stream=True, timeout=60)
        response.raise_for_status()
        
        with open(CSV_FILE, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info("Descarga completa. Cargando datos en memoria...")
        df_momo = pd.read_csv(CSV_FILE)
        df_momo['fecha_defuncion'] = pd.to_datetime(df_momo['fecha_defuncion'])
        logger.info("DataFrame de MoMo cargado y procesado en memoria.")
        return True
    except requests.RequestException as e:
        logger.error(f"Error durante la descarga: {e}")
    except Exception as e:
        logger.error(f"Error al procesar el CSV: {e}")
    
    df_momo = None
    return False

def delete_csv():
    if os.path.exists(CSV_FILE):
        os.remove(CSV_FILE)
        logger.info(f"Archivo {CSV_FILE} eliminado.")

async def process_selection(user_id: int):
    """Procesa las selecciones de un usuario y devuelve una lista de mensajes."""
    if df_momo is None:
        return ["🚫 Los datos no están disponibles temporalmente. Por favor, inténtelo de nuevo más tarde."]

    session = Session()
    user = get_or_create_user(session, user_id)
    selections = set(user.selections.split(',')) if user.selections else set()
    session.close()

    if not selections:
        return ["Por favor, seleccione al menos una opción antes de aceptar."]

    messages = []
    today = datetime.now(MADRID_TZ).date()

    for selection in selections:
        if not selection: continue
        
        df_ambito = None
        ambito_name = ""
        
        if selection == CALLBACK_NACIONAL:
            ambito_name = 'Nacional'
            df_ambito = df_momo[df_momo['ambito'] == 'nacional']
        elif selection.startswith(PREFIX_COMUNIDAD):
            ambito_name = selection[len(PREFIX_COMUNIDAD):]
            df_ambito = df_momo[(df_momo['ambito'] == 'ccaa') & (df_momo['nombre_ambito'] == ambito_name)]
        elif selection.startswith(PREFIX_PROVINCIA):
            ambito_name = selection[len(PREFIX_PROVINCIA):]
            df_ambito = df_momo[(df_momo['ambito'] == 'provincia') & (df_momo['nombre_ambito'] == ambito_name)]

        if df_ambito is None or df_ambito.empty:
            continue

        df_filtered = df_ambito[
            (df_ambito['cod_sexo'] == 'all') & 
            (df_ambito['cod_gedad'] == 'all') & 
            (df_ambito['defunciones_observadas'].notna())
        ].copy()

        if df_filtered.empty:
            continue
        
        df_filtered['diff_days'] = (df_filtered['fecha_defuncion'] - pd.Timestamp(today)).abs().dt.days
        closest_row = df_filtered.loc[df_filtered['diff_days'].idxmin()]
        
        obs = int(closest_row['defunciones_observadas'])
        esp = int(closest_row['defunciones_esperadas'])
        exceso = obs - esp
        signo = "+" if exceso >= 0 else ""
        
        message = (f"📊 *{ambito_name}* ({closest_row['fecha_defuncion'].strftime('%d/%m/%Y')})\n"
                   f"  - Observadas: *{obs}*\n"
                   f"  - Esperadas: *{esp}*\n"
                   f"  - Exceso: *{signo}{exceso}*")
        messages.append(message)

    return messages if messages else [NO_DATA_MESSAGE]

# =============================================================================
# --- SCHEDULING FUNCTIONS ---
# =============================================================================
def schedule_daily_update(user_id: int, hour: int, minute: int):
    job_id = f'daily_update_{user_id}'
    scheduler.add_job(
        send_daily_update,
        'cron',
        hour=hour,
        minute=minute,
        args=[user_id],
        id=job_id,
        replace_existing=True
    )
    logger.info(f"Trabajo '{job_id}' programado para las {hour:02d}:{minute:02d}.")

def remove_scheduled_job(user_id: int):
    job_id = f'daily_update_{user_id}'
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        logger.info(f"Trabajo programado '{job_id}' eliminado.")

async def send_daily_update(user_id: int):
    """Función ejecutada por el scheduler para enviar la actualización diaria."""
    logger.info(f"Ejecutando envío diario para el usuario {user_id}")
    messages = await process_selection(user_id)
    
    bot = telegram.Bot(BOT_TOKEN)
    try:
        await bot.send_message(chat_id=user_id, text="🔔 *Tu actualización diaria de MoMo:*\n\n" + "\n\n".join(messages), parse_mode='Markdown')
        logger.info(f"Mensaje diario enviado al usuario {user_id}")
    except telegram.error.Forbidden:
        logger.warning(f"El usuario {user_id} ha bloqueado el bot. Desactivando envíos automáticos.")
        session = Session()
        user = get_or_create_user(session, user_id)
        user.auto_send = False
        session.commit()
        session.close()
        remove_scheduled_job(user_id)
    except Exception as e:
        logger.error(f"Error al enviar mensaje diario al usuario {user_id}: {e}")

# =============================================================================
# --- TELEGRAM HANDLERS ---
# =============================================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    session = Session()
    user = get_or_create_user(session, user_id)
    
    if user.auto_send and not scheduler.get_job(f'daily_update_{user_id}'):
        schedule_daily_update(user.user_id, user.notification_hour, user.notification_minute)
    
    session.close()
    await show_main_menu(update, context)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_MESSAGE, parse_mode='Markdown')

async def set_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        time_str = context.args[0]
        new_hour, new_minute = map(int, time_str.split(':'))
        if not (0 <= new_hour < 24 and 0 <= new_minute < 60):
            raise ValueError("Hora o minutos fuera de rango.")

        user_id = update.effective_user.id
        session = Session()
        user = get_or_create_user(session, user_id)
        user.notification_hour = new_hour
        user.notification_minute = new_minute
        session.commit()
        
        if user.auto_send:
            schedule_daily_update(user_id, new_hour, new_minute)
            await update.message.reply_text(f"✅ Hora de notificación actualizada a las *{new_hour:02d}:{new_minute:02d}*.", parse_mode='Markdown')
        else:
            await update.message.reply_text(f"✅ Hora guardada. Se usará cuando actives los envíos automáticos.", parse_mode='Markdown')
            
        session.close()

    except (IndexError, ValueError):
        await update.message.reply_text("❌ Formato incorrecto. Por favor, usa `/settime HH:MM` (ej: `/settime 08:30`).")

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    session = Session()
    user = get_or_create_user(session, user_id)
    selections = set(user.selections.split(',')) if user.selections else set()
    session.close()
    
    selected_options = []
    for sel in selections:
        if sel == CALLBACK_NACIONAL: selected_options.append('Nacional')
        elif sel.startswith(PREFIX_COMUNIDAD): selected_options.append(sel[len(PREFIX_COMUNIDAD):])
        elif sel.startswith(PREFIX_PROVINCIA): selected_options.append(sel[len(PREFIX_PROVINCIA):])
    
    selected_text = "\n - " + "\n - ".join(sorted(selected_options)) if selected_options else " Ninguna"
    text = MENU_MESSAGE.format(selected_text)

    keyboard = [
        [InlineKeyboardButton("🇪🇸 Nacional", callback_data=CALLBACK_NACIONAL)],
        [InlineKeyboardButton("📍 Comunidades", callback_data=CALLBACK_COMUNIDADES)],
        [InlineKeyboardButton("🏙️ Provincias", callback_data=CALLBACK_PROVINCIAS)],
        [InlineKeyboardButton("✅ Aceptar", callback_data=CALLBACK_ACEPTAR), InlineKeyboardButton("🗑️ Borrar Todo", callback_data=CALLBACK_BORRAR)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    
    session = Session()
    user = get_or_create_user(session, user_id)
    
    data = query.data
    
    if data == CALLBACK_ACEPTAR:
        if not user.selections:
            await context.bot.answer_callback_query(query.id, "Por favor, seleccione al menos una opción.", show_alert=True)
            session.close()
            return
            
        await query.edit_message_text(text=PROCESSING_MESSAGE)
        messages = await process_selection(user_id)
        
        keyboard = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data=CALLBACK_VOLVER)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text="\n\n".join(messages), reply_markup=reply_markup, parse_mode='Markdown')
        
        if not user.auto_send:
            user.auto_send = True
            await query.message.reply_text(AUTO_SEND_MESSAGE.format(user.notification_hour, user.notification_minute), parse_mode='Markdown')
        
        schedule_daily_update(user_id, user.notification_hour, user.notification_minute)

    elif data == CALLBACK_BORRAR:
        user.selections = ""
        user.auto_send = False
        remove_scheduled_job(user_id)
        await context.bot.answer_callback_query(query.id, "Selecciones y envío automático borrados.")
        await show_main_menu(update, context)

    elif data == CALLBACK_VOLVER:
        await show_main_menu(update, context)

    elif data == CALLBACK_COMUNIDADES or data == CALLBACK_PROVINCIAS:
        logger.warning(f"La seleccion de CCAA/Provincias no esta implementada en este ejemplo.")
        await query.edit_message_text(text=f"Esta función no está implementada en este ejemplo.")

    elif data.startswith(PREFIX_COMUNIDAD) or data.startswith(PREFIX_PROVINCIA) or data == CALLBACK_NACIONAL:
        selections = set(user.selections.split(',')) if user.selections else set()
        if data in selections:
            selections.remove(data)
        else:
            selections.add(data)
        user.selections = ",".join(filter(None, selections))
        await show_main_menu(update, context)

    session.commit()
    session.close()

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Excepción mientras se manejaba una actualización: {context.error}")
    logger.error(traceback.format_exc())

# =============================================================================
# --- MAIN EXECUTION ---
# =============================================================================
def main() -> None:
    logger.info("Iniciando el bot...")

    if not os.path.exists(CSV_FILE):
        download_and_load_csv()
    else:
        global df_momo
        try:
            df_momo = pd.read_csv(CSV_FILE)
            df_momo['fecha_defuncion'] = pd.to_datetime(df_momo['fecha_defuncion'])
            logger.info("Datos existentes cargados en memoria.")
        except pd.errors.EmptyDataError:
            logger.warning(f"El fichero {CSV_FILE} existe pero esta vacio. Se descargara de nuevo.")
            download_and_load_csv()

    scheduler.start()
    scheduler.add_job(download_and_load_csv, 'cron', hour=CSV_DOWNLOAD_HOUR, minute=CSV_DOWNLOAD_MINUTE, id='download_csv', replace_existing=True)
    scheduler.add_job(delete_csv, 'cron', hour=CSV_DELETE_HOUR, minute=CSV_DELETE_MINUTE, id='delete_csv', replace_existing=True)
    logger.info("Scheduler y trabajos base iniciados.")

    session = Session()
    users_with_autosend = session.query(User).filter_by(auto_send=True).all()
    for user in users_with_autosend:
        schedule_daily_update(user.user_id, user.notification_hour, user.notification_minute)
    session.close()
    logger.info(f"Cargados {len(users_with_autosend)} trabajos de usuarios existentes.")

    defaults = Defaults(parse_mode='Markdown', tzinfo=MADRID_TZ)
    application = Application.builder().token(BOT_TOKEN).defaults(defaults).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("settime", set_time))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_error_handler(error_handler)
    
    logger.info("Bot iniciado. Comenzando el polling...")
    application.run_polling()

if __name__ == '__main__':
    main()
