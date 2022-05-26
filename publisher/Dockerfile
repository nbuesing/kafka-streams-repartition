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
    JAVA_OPTS=""

COPY ./build/distributions/*.tar .
COPY ./docker/client-config.yml /

COPY --from=build /jmx_prometheus_javaagent.jar /

RUN \
	tar xfvp /publisher-1.0.tar

EXPOSE 7071/tcp 7072/tcp

#ENTRYPOINT ["java"]


CMD ["/publisher-1.0/bin/streams"]
