FROM golang:1.21-alpine

RUN apk --update add ca-certificates bash
RUN mkdir -p /tmp

USER root

#RUN go install github.com/go-delve/delve/cmd/dlv@latest

COPY otelcontribcol /
COPY otelcontribcol/config.yaml /etc/otel/config.yaml
COPY dlv-alpine /dlv
WORKDIR /
EXPOSE 4317 55680 55679 2345
ENTRYPOINT ["/dlv"]
# /dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient --log exec /otelcontribcol -- --config /etc/otel/config.yaml
CMD ["--listen=:2345", "--headless=true", "--api-version=2", "--accept-multiclient", "--log", "--log-output=debugger,gdbwire,lldbout,debuglineerr,rpc,dap,fncall,minidump,stack", "exec" , "/otelcontribcol", "--",  "--config", "/etc/otel/config.yaml"]
#ENTRYPOINT ["/otelcontribcol"]
#CMD ["--config", "/etc/otel/config.yaml"]