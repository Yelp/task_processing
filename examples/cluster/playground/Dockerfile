FROM ubuntu:trusty

RUN apt-get update -q && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        debhelper dpkg-dev gcc gdebi-core git help2man libffi-dev \
        libssl-dev libsasl2-modules libyaml-dev pyflakes python-dev python-pip python-pytest \
        python-setuptools python-tox python-yaml wget zip zsh \
        openssh-server docker.io curl vim jq libsvn-dev \
    && apt-get clean

RUN cd /tmp && \
    wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
    gdebi -n dh-virtualenv*.deb && \
    rm dh-virtualenv_*.deb

RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir /var/run/sshd

RUN pip install --upgrade pip==9.0.1
RUN pip install --upgrade virtualenv==15.1.0

# FIXME use actual virtualenv
RUN pip install pymesos
RUN pip install pyrsistent

ADD . /src
ENV PYTHONPATH=/src
WORKDIR /src

CMD /bin/bash
