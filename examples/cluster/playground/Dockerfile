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
ARG NPM_CONFIG_REGISTRY
ENV PIP_INDEX_URL=${PIP_INDEX_URL:-https://pypi.python.org/simple}
ENV NPM_CONFIG_REGISTRY=${NPM_CONFIG_REGISTRY:-https://npm.yelpcorp.com}

# Get yarn
# I'd use ubuntu yarn (yarnpkg) but https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=1019291
RUN wget --quiet -O - https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb http://dl.yarnpkg.com/debian/ stable main" > /etc/apt/sources.list.d/yarn.list
RUN apt-get -q update && apt-get -q install -y --no-install-recommends yarn

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
