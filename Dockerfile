FROM openjdk:11-jdk-slim

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install system dependencies
RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y \
    curl zip wget python3 python3-pip gcc && apt-get clean && rm -rf /var/lib/apt/lists/* && \
    wget https://github.com/sbt/sbt/releases/download/v1.4.4/sbt-1.4.4.zip && \
    unzip sbt-1.4.4.zip && ln -s /sbt/bin/sbt /usr/bin/sbt

# add user
ARG user=hire-cluster-sim
ARG uid=9000
RUN useradd -u ${uid} -s /bin/sh -m ${user}

# set build directory
COPY ./requirements.txt /app-build/requirements.txt
COPY ./build.* /app-build/
COPY ./project /app-build/project
COPY ./src /app-build/src
COPY ./docker-entry.sh /app-build/

RUN cd /app-build && \
         pip3 install -r requirements.txt && \
         mkdir -p /home/${user}/.sbt && \
         mkdir -p /home/${user}/.cache && \
         mkdir -p /app && \
         chown -R ${user}:${user} /app && \
         chown -R ${user}:${user} /home/${user}/.sbt && \
         chown -R ${user}:${user} /home/${user}/.cache && \
         chown -R ${user}:${user} /app-build

USER ${user}
RUN cd /app-build && sbt assembly
         
WORKDIR /app

ENTRYPOINT ["/app-build/docker-entry.sh"]
CMD ["sbt"]
