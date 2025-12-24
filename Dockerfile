FROM apify/actor-python:3.12

ENV APIFY_PROXY_PASSWORD=auto

COPY . /usr/src/app
WORKDIR /usr/src/app
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
