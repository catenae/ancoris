FROM catenae/link:develop
RUN \
    mkdir /opt/ancoris \
    && pip install --upgrade pip \
    && pip install docker==4.0.2
COPY *.py /opt/ancoris/
WORKDIR /opt/ancoris
