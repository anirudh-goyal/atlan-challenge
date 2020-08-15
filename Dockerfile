FROM python:3.6.5-alpine
WORKDIR /atlan-challenge
COPY . /atlan-challenge
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
CMD ["server.py"]