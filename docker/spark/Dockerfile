FROM apache/spark:4.0.4-scala2.13-java21-python3-r-ubuntu

USER root

RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    -o /opt/spark/jars/hadoop-aws-3.4.1.jar \
    && curl -fL \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar \
    -o /opt/spark/jars/bundle-2.24.6.jar

USER spark