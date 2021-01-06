FROM openjdk:11-jdk-slim

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install system dependencies
RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y \
    curl zip wget python3 python3-pip gcc && apt-get clean && rm -rf /var/lib/apt/lists/* && \
    wget https://github.com/sbt/sbt/releases/download/v1.4.4/sbt-1.4.4.zip && \
    unzip sbt-1.4.4.zip && ln -s /sbt/bin/sbt /usr/bin/sbt

# create same user as on host system
ARG HOST_USER
ARG HOST_UID
RUN useradd -u ${HOST_UID} -s /bin/sh -m ${HOST_USER}

# set build directory
COPY ./requirements.txt /app-build/requirements.txt
COPY ./build.* /app-build/
COPY ./project /app-build/project
COPY ./src /app-build/src
COPY ./docker-entry.sh /app-build/

RUN cd /app-build && \
         pip3 install -r requirements.txt && \
         mkdir -p /home/${HOST_USER}/.sbt && \
         mkdir -p /home/${HOST_USER}/.cache && \
         mkdir -p /app && \
         chown -R ${HOST_USER}:${HOST_USER} /app && \
         chown -R ${HOST_USER}:${HOST_USER} /home/${HOST_USER}/.sbt && \
         chown -R ${HOST_USER}:${HOST_USER} /home/${HOST_USER}/.cache && \
         chown -R ${HOST_USER}:${HOST_USER} /app-build

USER ${HOST_USER}
RUN cd /app-build && sbt assembly
         
WORKDIR /app

ENTRYPOINT ["/app-build/docker-entry.sh"]
CMD ["sbt"]
