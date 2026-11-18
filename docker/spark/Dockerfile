FROM apache/spark:4.0.4-scala2.13-java21-python3-r-ubuntu

COPY --from=ghcr.io/astral-sh/uv:0.12.18 /uv /usr/local/bin/uv

USER root

ENV UV_PYTHON_INSTALL_DIR=/opt/python
ENV PYSPARK_PYTHON=/opt/venv/bin/python

RUN uv python install 3.11 \
    && uv venv --python 3.11 /opt/venv \
    && uv pip install --python /opt/venv/bin/python \
        "numpy<3" \
    && chmod -R a+rX /opt/python /opt/venv

RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    -o /opt/spark/jars/hadoop-aws-3.4.1.jar \
    && curl -fL \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar \
    -o /opt/spark/jars/bundle-2.24.6.jar

USER spark

RUN /opt/venv/bin/python -c \
    "import sys, numpy; assert sys.version_info[:2] == (3, 11)"