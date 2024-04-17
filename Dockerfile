FROM python:3.11-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
 
RUN apt-get update --yes --quiet
RUN apt-get install --yes --quiet --no-install-recommends \
    openssh-client \
    procps

RUN python -m pip install --upgrade pip

WORKDIR /app
COPY requirements.txt .
RUN python -m pip install -r requirements.txt
 
COPY harvest_transformer .
 
CMD ["python", "-m", "harvest_transformer", "https://dev.eodatahub.org.uk/catalogue-data"]
