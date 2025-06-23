from celery import Celery
from celery.schedules import crontab

celery_app = Celery(
    "option_pipeline",
    broker="redis://localhost:6379/0",  # or Redis cloud URL
    backend="redis://localhost:6379/0",
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Kolkata",
    enable_utc=False,
    beat_schedule={
        'rollup-daily-at-12-10am': {
            'task': 'rollup.historical_rollup.rollup_to_historical',
            'schedule': crontab(hour=11, minute=45),  # 11:45 PM IST
        }
    }
)
