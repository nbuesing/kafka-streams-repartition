FROM openjdk:18-jdk-slim AS build

WORKDIR /

RUN \
    apt-get update && \
    apt-get install -y curl && \
    curl -k -L -o jmx_prometheus_javaagent.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.1/jmx_prometheus_javaagent-0.16.1.jar

FROM openjdk:18-jdk-slim

WORKDIR /

RUN \
    apt-get update && \
    apt-get install -y curl net-tools dnsutils iproute2 iputils-ping jq

ENV \
    ENVIRONMENT="" \
    JAVA_OPTS="-javaagent:/jmx_prometheus_javaagent.jar=7071:/streams-config.yml"

COPY ./build/distributions/*.tar .
COPY ./docker/streams-config.yml /
COPY ./docker/all-config.yml /
COPY ./docker/entrypoint.sh /
COPY ./docker/healthcheck.sh /

COPY --from=build /jmx_prometheus_javaagent.jar /

RUN \
	tar xfvp /streams-1.0.tar

EXPOSE 7071/tcp 7072/tcp

ENTRYPOINT  ["/entrypoint.sh"]

CMD         ["/streams-1.0/bin/streams"]


#ENTRYPOINT ["java"]
#CMD /streams-1.0/bin/streams -b broker-1:9092,broker-2:9092,broker-3:9092
