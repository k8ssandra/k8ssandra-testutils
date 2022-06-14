FROM eclipse-temurin:11-jre-focal
WORKDIR /opt/lib/testutils
ADD ./ .
RUN adduser testutils --disabled-password
RUN ["chown", "-R", "testutils:testutils", "."]
RUN mkdir  /opt/bin/
RUN ln -s /opt/lib/testutils/pulsar-cdc-testutil/bin/app /opt/bin/pulsar-cdc-testutil
USER testutils
CMD ["/opt/bin/pulsar-cdc-testutil"]