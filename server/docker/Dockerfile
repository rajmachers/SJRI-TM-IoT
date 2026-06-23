FROM python:3.11-slim

WORKDIR /app

COPY christ_bridge.py .
RUN pip install websocket-client requests

CMD ["python", "christ_bridge.py"]
