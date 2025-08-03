from datetime import datetime
import requests
from airflow.models import Variable

token = Variable.get('token')

def send_telegram_notification(message: str):
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    payload = {
        'chat_id': '-4805082079',
        'text': message,
    }

    response = requests.post(url, payload)
    response.raise_for_status()

def on_failure_callback(context):
    task_instance = context['task_instance']
    error_message = f"""
    [X] ЗАДАЧА НЕ ВЫПОЛНЕНА
    {context["dag"].dag_id} \n
    {task_instance.task_id} \n
    Ошибка: {str(context.get('exception'))}
    """
    send_telegram_notification(message=error_message)


def on_success_callback(context):
    task_instance = context['task_instance']
    success_message = f"""
    [V] ЗАДАЧА ВЫПОЛНЕНА
    {context["dag"].dag_id} \n
    {task_instance.task_id} \n
    Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
    send_telegram_notification(message=success_message)