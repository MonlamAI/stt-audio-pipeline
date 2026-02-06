"""
Workers module for audio processing pipeline.
Stateless workers that process jobs from SQS queues.

Imports are lazy to avoid loading heavy dependencies (pydub, torch)
when only one worker type is needed.
"""

__all__ = [
    'run_download_worker',
    'run_split_worker',
    'run_upload_worker',
]


def run_download_worker():
    from .download_worker import run_download_worker as _run
    return _run()


def run_split_worker():
    from .split_worker import run_split_worker as _run
    return _run()


def run_upload_worker():
    from .upload_worker import run_upload_worker as _run
    return _run()
