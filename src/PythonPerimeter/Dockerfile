FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY src/PythonPerimeter/requirements.txt ./requirements.txt

RUN pip install \
    --no-cache-dir \
    -r requirements.txt

COPY src/PythonPerimeter/perimeter ./perimeter

CMD ["python", "-m", "perimeter.dispatcher"]
