# python ubuntu image
FROM python:3.12.1-alpine

# RUN apk add dpkg -y
RUN echo "America/Denver" > /etc/timezone
RUN apk add --no-cache tzdata

RUN apk update && \
    apk add postgresql \
    postgresql-contrib \
    postgresql-dev \ 
    python3-dev \
     musl-dev \
    libpq-dev \
    g++ \
    build-base \ 
    # mdbtools deps
    git \
    libtool \
    automake \
    autoconf \
    glib \
    glib-dev \
    unixodbc \ 
    unixodbc-dev \
    gawk \
    flex \
    bison \ 
    nano \
    bash && \ 
    pip3 install psycopg2 && \
    pip3 install --upgrade pip

# build mdbtools from source
WORKDIR /usr 
RUN git clone https://github.com/mdbtools/mdbtools.git 
RUN cd mdbtools && \
    autoreconf -i -f && \ 
    ./configure --with-unixodbc=/usr/local && \
    make && \
    make install

RUN export PATH="/usr/mdbtools/src/util":$PATH


# prepare environment
WORKDIR /usr/src
RUN mkdir src
COPY ./requirements.txt ./src

# install python dependencies
RUN pip install --no-cache-dir -r /usr/src/src/requirements.txt

#copy application
COPY / .

# run application
CMD ["python3", "/usr/src/index.py"]
# CMD ["tail","-f", "/dev/null"]
