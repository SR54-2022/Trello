# Use NGINX 1.18 to match the OpenTelemetry SDK version
FROM nginx:1.18

# Copy the custom nginx configuration
COPY ./api-gateway.conf /etc/nginx/conf.d/api_gateway.conf

# Copy SSL certificate and private key
COPY ./cert.crt /etc/nginx/ssl/cert.crt
COPY ./privat.key /etc/nginx/ssl/privat.key

# Remove the default nginx configuration
RUN rm /etc/nginx/conf.d/default.conf

# Update apt-get and install unzip for handling the OpenTelemetry SDK
RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list \
 && sed -i 's|http://security.debian.org/debian-security|http://archive.debian.org/debian-security|g' /etc/apt/sources.list \
 && apt-get update && apt-get install -y unzip && apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

# Download and extract the OpenTelemetry WebServer SDK for NGINX 1.18
ADD https://github.com/open-telemetry/opentelemetry-cpp-contrib/releases/download/webserver%2Fv1.0.0/opentelemetry-webserver-sdk-x64-linux.tgz.zip /opt
RUN cd /opt && \
    unzip opentelemetry-webserver-sdk-x64-linux.tgz.zip && \
    tar xvfz opentelemetry-webserver-sdk-x64-linux.tgz

# Install the OpenTelemetry WebServer SDK
RUN cd /opt/opentelemetry-webserver-sdk && ./install.sh

# Update the environment's library path
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/opentelemetry-webserver-sdk/sdk_lib/lib

# Modify nginx configuration to load the OpenTelemetry module
RUN echo "load_module /opt/opentelemetry-webserver-sdk/WebServerModule/Nginx/ngx_http_opentelemetry_module.so;\n$(cat /etc/nginx/nginx.conf)" > /etc/nginx/nginx.conf

# Copy the OpenTelemetry nginx module configuration
COPY ./opentelemetry_module.conf /etc/nginx/conf.d/

# Copy the OpenTelemetry Collector configuration
COPY ./otel-collector-config.yaml /etc/otel-collector-config.yaml
