FROM ruby:3.2

WORKDIR /azure-kusto-fluentd

# Copy Gemfile and Gemfile.lock if present
COPY Gemfile Gemfile.lock 

# Install dependencies if Gemfile exists
RUN if [ -f Gemfile ]; then bundle install; fi

# Install Fluentd
RUN gem install fluentd

# Copy all plugin files except .env files (do NOT delete .conf files)
COPY . ./
RUN find . -type f -name '*.env' -delete

RUN gem install dotenv
RUN gem install azure-storage-blob
RUN gem install azure-storage-queue
RUN gem install azure-storage-table

# Set the default command to run Fluentd with your plugin configuration and plugin path
CMD ["fluentd", "-c", "plugin.conf", "-p", "lib/fluent/plugin"]
