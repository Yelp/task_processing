FROM ubuntu:bionic

ARG PIP_INDEX_URL
ENV PIP_INDEX_URL=${PIP_INDEX_URL:-https://pypi.python.org/simple}

RUN apt-get update -q && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        software-properties-common \
        debhelper dpkg-dev gcc gdebi-core git help2man libffi-dev \
        libssl-dev libsasl2-modules libyaml-dev pyflakes python3-dev python3-pip python3-pytest \
        python-tox python-yaml wget zip zsh \
        openssh-server docker.io curl vim jq libsvn-dev \
    && apt-get clean

RUN apt-get update -q && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3.6 python3.6-dev python3.6-venv
RUN wget https://bootstrap.pypa.io/pip/3.6/get-pip.py
RUN python3.6 get-pip.py
RUN ln -s /usr/bin/python3.6 /usr/local/bin/python3

RUN cd /tmp && \
    wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
    gdebi -n dh-virtualenv*.deb && \
    rm dh-virtualenv_*.deb

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
