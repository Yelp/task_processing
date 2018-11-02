FROM ubuntu:xenial

RUN apt-get update -q && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        software-properties-common \
        debhelper dpkg-dev gcc gdebi-core git help2man libffi-dev \
        libssl-dev libsasl2-modules libyaml-dev pyflakes python3-dev python3-pip python3-pytest \
        python-tox python-yaml wget zip zsh \
        openssh-server docker.io curl vim jq libsvn-dev \
    && apt-get clean

RUN add-apt-repository ppa:jonathonf/python-3.6
RUN apt-get update -q && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3.6 python3.6-dev python3.6-venv
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3.6 get-pip.py
RUN ln -s /usr/bin/python3.6 /usr/local/bin/python3

RUN cd /tmp && \
    wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
    gdebi -n dh-virtualenv*.deb && \
    rm dh-virtualenv_*.deb

RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir /var/run/sshd

RUN pip3 install --upgrade setuptools
RUN pip3 install --upgrade pip==10.0.1
RUN pip3 install --upgrade virtualenv==15.1.0

ADD . /src
ENV PYTHONPATH=/src
WORKDIR /src

RUN python3 setup.py install
RUN pip3 install -r requirements-dev.txt
RUN pip3 install pymesos

CMD /bin/bash
