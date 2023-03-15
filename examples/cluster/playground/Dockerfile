FROM ubuntu:jammy

RUN apt-get update -yq && \
    apt-get install -yq \
        # needed to add a ppa
        software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa

RUN apt-get update -q && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        software-properties-common \
        debhelper dpkg-dev gcc gdebi-core git help2man libffi-dev \
        dh-virtualenv \
        libssl-dev libsasl2-modules libyaml-dev pyflakes3 python3.8-dev python3.8-distutils python3-pip python3-pytest python3-http-parser\
        tox python3-yaml wget zip zsh \
        openssh-server docker.io curl vim jq libsvn-dev \
    && apt-get clean

ARG PIP_INDEX_URL
ENV PIP_INDEX_URL=${PIP_INDEX_URL:-https://pypi.python.org/simple}
RUN pip3 install --index-url ${PIP_INDEX_URL} virtualenv==16.7.5

RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir /var/run/sshd

ADD . /src
ENV PYTHONPATH=/src
WORKDIR /src

# temporarily downpin cryptography until we can make it grab the correct pre-built wheel in itests
RUN pip3 install .
RUN pip3 install -r requirements-dev.txt
RUN pip3 install pymesos

CMD /bin/bash
