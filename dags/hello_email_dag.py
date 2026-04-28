"""
DAG de prueba: envía un correo electrónico fijo todos los días a las 09:00 UTC.

Propósito: validar que la configuración SMTP de Airflow funciona correctamente.
Primer caso de uso del patrón "DAG de notificación".
"""
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.email import EmailOperator

# Argumentos por defecto que se aplican a todos los tasks del DAG.
# En DAGs con muchos tasks esto evita repetir parámetros como owner, retries, etc.
default_args = {
    "owner": "ronaldo",  # Quien es el dueño del DAG
    "retries": 1,  # Rententos en caso de fallo
    "retry_delay": 300,  # 5 minutos entre reintentos
}

# Definición del DAG usando context manager (patron recomendado desde Airflow 2.0)
with DAG(
    dag_id="hello_email_dag",  # Identificador único del DAG
    description="DAG de prueba para enviar un correo electrónico",  # Descripción del DAG
    default_args=default_args,  # Argumentos por defecto para los tasks
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Santiago"),  # Fecha de inicio
    schedule="30 9 * * *",  # Cron para ejecutar a las 09:00 UTC todos los días
    catchup=False,  # No ejecutar tareas pasadas al iniciar el DAG
    tags=[
        "prueba",
        "email",
        "notificación",
        "test",
    ],  # Etiquetas para organizar y filtrar DAGs
) as dag:

    send_test_email = EmailOperator(
        task_id="send_test_email",  # Identificador del task
        to=["ronaldoserra1998@gmail.com", "anaysbravo15@gmail.com"],  # Destinatario del correo
        subject="[Notificacion de Recordatorio] - {{ ds }}",  # dia actual en el asunto del correo
        # html_content acepta HTML. {{ ds }} es una macro de Jinja que Airflow
        # reemplaza con la fecha de ejecución lógica del DAG (YYYY-MM-DD).
        # Esto te permite ver en el correo de qué run viene.
        html_content="""
        <h3>Hola</h3>
        <p>Este es un correo de recordatorio para que te tomes tu medicamento.</p>
        """,
    )