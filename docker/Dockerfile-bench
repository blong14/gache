FROM jupyterhub/k8s-singleuser-sample:1.2.0

USER root

RUN apt-get update && \
    apt-get install -y gcc time

RUN wget https://golang.org/dl/go1.18.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.18.linux-amd64.tar.gz

ENV PATH="${PATH}:/usr/local/go/bin"

