FROM python:3.9.5-slim-buster
COPY requirements.txt requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
COPY topic.py topic.py
CMD ["python3", "topic.py"]