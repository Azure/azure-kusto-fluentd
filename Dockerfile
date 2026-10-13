FROM mcr.microsoft.com/devcontainers/ruby:1-3.2-bookworm@sha256:90380fc77c15d8261e054f7cc8ff81cbdd4c0d70955da2308e2d0465c71761be

WORKDIR /azure-kusto-fluentd

# Copy Gemfile and Gemfile.lock if present
COPY Gemfile Gemfile.lock 

# Install dependencies if Gemfile exists
RUN if [ -f Gemfile ]; then bundle install; fi

# Install Fluentd
RUN gem install fluentd -v ">= 1.19.3, < 2"

# Copy all plugin files except .env files (do NOT delete .conf files)
COPY . ./
RUN find . -type f -name '*.env' -delete

RUN gem install dotenv
RUN gem install azure-storage-blob
RUN gem install azure-storage-queue
RUN gem install azure-storage-table

# Set the default command to run Fluentd with your plugin configuration and plugin path
CMD ["fluentd", "-c", "plugin.conf", "-p", "lib/fluent/plugin"]
