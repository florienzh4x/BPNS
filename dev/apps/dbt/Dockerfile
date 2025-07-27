ARG PYTHON_VERSION=3.12
ARG BASE_IMAGE=slim-bookworm

FROM python:${PYTHON_VERSION}-${BASE_IMAGE}

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    git \
    wget \
    htop \
    gcc \
    g++ \
    openssh-server \
    openssh-client \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ARG SSH_USER=dbtuser
ARG SSH_PASSWORD=dbtpass

RUN useradd -ms /bin/bash dbtuser && \
    echo "$SSH_USER:$SSH_PASSWORD" | chpasswd

RUN mkdir -p /var/run/sshd 

ARG DBT_ADAPTERS
ARG PIP_ADDITIONAL_REQUIREMENTS

RUN if [ -n "$PIP_ADDITIONAL_REQUIREMENTS" ]; then pip install --no-cache-dir $PIP_ADDITIONAL_REQUIREMENTS; fi

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir dbt-core && \
if [ -s /tmp/requirements.txt ]; then pip install --no-cache-dir -r /tmp/requirements.txt; fi && \
if [ -n "$DBT_ADAPTERS" ]; then pip install --no-cache-dir $DBT_ADAPTERS; fi

ARG DBT_PROFILES_DIR
ARG DBT_TARGET
ARG DBT_PROJECT_DIR

RUN echo "export DBT_PROFILES_DIR=$DBT_PROFILES_DIR" >> /home/dbtuser/.bashrc
RUN echo "export DBT_TARGET=$DBT_TARGET" >> /home/dbtuser/.bashrc
RUN echo "export DBT_PROJECT_DIR=$DBT_PROJECT_DIR" >> /home/dbtuser/.bashrc

EXPOSE 22

WORKDIR /home/dbtuser/dbt

CMD ["/usr/sbin/sshd", "-D"]