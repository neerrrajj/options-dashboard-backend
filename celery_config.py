from celery import Celery

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
)
