"""
Global Config
"""

from os import getenv

CELERY_BROKER = getenv("CCG_CELERY_BROKER", 'redis://localhost')
CELERY_BACKEND = getenv("CCG_CELERY_BACKEND", 'redis://localhost')
