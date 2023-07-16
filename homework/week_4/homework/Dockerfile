FROM svizor/zoomcamp-model:mlops-3.10.0-slim

RUN pip install -U pip & pip install pipenv

COPY ["Pipfile", "Pipfile.lock", "starter.py", "./"]

RUN pipenv install --deploy --system
RUN mkdir data

ENTRYPOINT ["python", "starter.py"]
