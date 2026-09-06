from importlib import import_module

fetch_news = import_module("utils.minio.01_fetch_news").main
find_pdf = import_module("utils.minio.02_find_pdf").main
upload_minio = import_module("utils.minio.03_upload_minio").main
publish_pdf = import_module("utils.minio.04_publish_pdf").main

__all__ = [
    "fetch_news",
    "find_pdf",
    "upload_minio",
    "publish_pdf",
]