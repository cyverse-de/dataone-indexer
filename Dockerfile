FROM discoenv/golang-base:go-1.8

ENV CONF_TEMPLATE=/go/src/github.com/cyverse-de/dataone-indexer/dataone-indexer.yml.tmpl
ENV CONF_FILENAME=dataone-indexer.yml
ENV PROGRAM=dataone-indexer

COPY . /go/src/github.com/cyverse-de/dataone-indexer
RUN go install github.com/cyverse-de/dataone-indexer

ARG git_commit=unknown
ARG version="2.9.0"

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"
