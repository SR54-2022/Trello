
FROM nginx:1.18

COPY ./api-gateway.conf /etc/nginx/conf.d/api_gateway.conf

COPY ./cert.crt /etc/nginx/ssl/cert.crt
COPY ./privat.key /etc/nginx/ssl/privat.key

RUN rm /etc/nginx/conf.d/default.conf

RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list \
 && sed -i 's|http://security.debian.org/debian-security|http://archive.debian.org/debian-security|g' /etc/apt/sources.list \
 && apt-get update && apt-get install -y unzip && apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

ADD https://github.com/open-telemetry/opentelemetry-cpp-contrib/releases/download/webserver%2Fv1.0.0/opentelemetry-webserver-sdk-x64-linux.tgz.zip /opt
RUN cd /opt && \
    unzip opentelemetry-webserver-sdk-x64-linux.tgz.zip && \
    tar xvfz opentelemetry-webserver-sdk-x64-linux.tgz

RUN cd /opt/opentelemetry-webserver-sdk && ./install.sh

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/opentelemetry-webserver-sdk/sdk_lib/lib

RUN echo "load_module /opt/opentelemetry-webserver-sdk/WebServerModule/Nginx/ngx_http_opentelemetry_module.so;\n$(cat /etc/nginx/nginx.conf)" > /etc/nginx/nginx.conf

COPY ./opentelemetry_module.conf /etc/nginx/conf.d/

COPY ./otel-collector-config.yaml /etc/otel-collector-config.yaml
