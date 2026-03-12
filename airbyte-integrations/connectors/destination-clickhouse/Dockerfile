# syntax=docker/dockerfile:1
# Runtime-only image for destination-clickhouse (kueez fork).
# The connector TAR is built locally with:
#   ./gradlew :airbyte-integrations:connectors:destination-clickhouse:distTar -x test
# and placed at build/distributions/destination-clickhouse-*.tar before running this build.

FROM --platform=linux/amd64 docker.io/airbyte/java-connector-base:2.0.1@sha256:ec89bd1a89e825514dd2fc8730ba299a3ae1544580a078df0e35c5202c2085b3

ARG CONNECTOR_NAME=destination-clickhouse

RUN chmod +x /airbyte/base.sh /airbyte/javabase.sh && \
    chown airbyte:airbyte /airbyte/base.sh /airbyte/javabase.sh /airbyte/dd-java-agent.jar

ENV AIRBYTE_ENTRYPOINT="/airbyte/base.sh"
ENV APPLICATION="${CONNECTOR_NAME}"

COPY build/distributions/airbyte-app.tar /tmp/${CONNECTOR_NAME}.tar
RUN tar xf /tmp/${CONNECTOR_NAME}.tar --strip-components=1 -C /airbyte && \
    rm -rf /tmp/${CONNECTOR_NAME}.tar && \
    chown -R airbyte:airbyte /airbyte

USER airbyte
ENTRYPOINT ["/airbyte/base.sh"]
