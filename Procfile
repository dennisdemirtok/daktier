web: MALLOC_ARENA_MAX=2 gunicorn edge_app:app --bind 0.0.0.0:$PORT --workers 1 --threads 8 --worker-class gthread --timeout 240 --graceful-timeout 30
