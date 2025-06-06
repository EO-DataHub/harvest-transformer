FROM python:3.12-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
 
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update --yes --quiet \
    && apt-get install --yes --quiet --no-install-recommends \
    openssh-client \
    procps \
    git \
    g++

RUN --mount=type=cache,target=/root/.cache/pip python -m pip install --upgrade pip

WORKDIR /harvest_transformer
ADD LICENSE requirements.txt ./
ADD harvest_transformer ./harvest_transformer/
ADD pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements.txt .
 
ENTRYPOINT ["python", "-m", "harvest_transformer"]
