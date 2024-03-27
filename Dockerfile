FROM python:3.8.10

RUN apt update && apt install libgl1 libglib2.0-0 libsm6 libxrender1 libxext6 -y

WORKDIR /code

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD [ "python", "main.py" ]
