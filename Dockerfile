FROM python:3.6
ADD . /code
WORKDIR /code
RUN pip install -r requirements.txt
EXPOSE 5000
CMD ["gunicorn"  , "-b", "0.0.0.0:5000", "app:app", "-t 0"]
CMD ["flask", "crontab add"]