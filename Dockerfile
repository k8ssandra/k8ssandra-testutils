FROM openjdk:jre-alpine

WORKDIR /opt/lib/testutils
ADD ./pulsar-cdc-testutil/app/build/install/pulsar-cdc-testutil/ .
RUN ["chown", "-R", "testutils:testutils", "."]
RUN mkdir  /opt/bin/testutils
RUN ln -s pulsar-cdc-testutil/bin/app /opt/bin/testutils/pulsar-cdc-testutil
USER testutils
CMD ["/opt/bin/testutils/pulsar-cdc-testutil"]