FROM openjdk:11-jdk-slim

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install system dependencies
RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y \
    curl zip wget python3 python3-pip gcc && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN wget https://github.com/sbt/sbt/releases/download/v1.4.4/sbt-1.4.4.zip
RUN unzip sbt-1.4.4.zip && ln -s /sbt/bin/sbt /usr/bin/sbt

# set work directory
WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN pip3 install -r requirements.txt

# Set user and group
ARG user=appuser
ARG group=appuser
ARG uid=1000
ARG gid=1000
RUN groupadd -g ${gid} ${group}
RUN useradd -u ${uid} -g ${group} -s /bin/sh -m ${user}

COPY ./build.* /app/
COPY ./project /app/project

RUN chown -R ${user}:${group} /app
USER ${uid}:${gid}

RUN sbt update

CMD ["sbt"]