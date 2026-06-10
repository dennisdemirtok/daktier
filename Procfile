web: gunicorn edge_app:app --bind 0.0.0.0:$PORT --workers 2 --threads 6 --worker-class gthread --timeout 240 --graceful-timeout 30
