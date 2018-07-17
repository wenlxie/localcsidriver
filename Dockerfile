# Update the README when this base image and/or the version of lvm2 (below) is updated.
FROM centos:7.3.1611

RUN yum install -y gcc-4.8.5 gcc-c++-4.8.5 make git util-linux xfsprogs file

ENV LVM2_DOWNLOAD_URL http://mirrors.kernel.org/sourceware/lvm2/LVM2.2.02.177.tgz

RUN curl -fsSL "$LVM2_DOWNLOAD_URL" -o LVM2.2.02.177.tgz && \
      tar -xzvf LVM2.2.02.177.tgz && \
      cd LVM2.2.02.177 && \
      ./configure && \
      make && \
      make install && \
      ldconfig && \
      cd .. && \
      rm -f LVM2.2.02.177.tgz

ENV GOLANG_VERSION 1.9.2
ENV GOLANG_DOWNLOAD_URL https://golang.org/dl/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 de874549d9a8d8d8062be05808509c09a88a248e77ec14eb77453530829ac02b

RUN rm -rf /usr/local/go && \
      curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz && \
      echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - && \
      tar -C /usr/local -xzf golang.tar.gz && \
      rm -f golang.tar.gz

ENV GOPATH /go
ENV PATH /go/bin:$PATH
ENV PATH /usr/local/go/bin:$PATH

RUN mkdir -p /go/src/github.com/alecthomas && \
    cd /go/src/github.com/alecthomas && \
    git clone https://github.com/alecthomas/gometalinter.git --branch=v1.2.1 && \
    go install -v github.com/alecthomas/gometalinter && \
    gometalinter --install && \
    go get -u golang.org/x/tools/cmd/goimports && \
    mkdir -p /go/src/github.com/mesosphere/csilvm

# We explicitly disable use of lvmetad as the cache appears to yield inconsistent results,
# at least when running in docker.
RUN sed -i 's/udev_rules = 1/udev_rules = 0/' /etc/lvm/lvm.conf && \
    sed -i 's/udev_sync = 1/udev_sync = 0/' /etc/lvm/lvm.conf && \
    sed -i 's/use_lvmetad = 1/use_lvmetad = 0/' /etc/lvm/lvm.conf

WORKDIR /go/src/github.com/mesosphere/csilvm
