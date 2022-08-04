FROM python:3

WORKDIR /usr/src/app
COPY requirements.txt ./
COPY .env ./

RUN pip install --no-cache-dir -r requirements.txt
COPY image_management.py ./

CMD [ "python", "./image_management.py" ]

