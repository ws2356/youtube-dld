FROM python:3.8

COPY --chown=1000:1000 requirements.txt /app/

WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

USER 1000:1000

COPY --chown=1000:1000 src/main.py src/

CMD [ "python", "src/main.py" ]
