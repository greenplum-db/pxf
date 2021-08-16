FROM ruby:2.5

RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash - ; \
    apt-get install -y nodejs
RUN gem install bundler:1.17.3 \
    elasticsearch:2.0.2 \
    faraday:0.15.4 \
    nokogiri:1.11.2 \
    mimemagic:0.3.9 \
    sendgrid-ruby:1.1.6

RUN /bin/bash -l -c "cp /etc/hosts ~/hosts.new"
RUN /bin/bash -l -c 'sed -i -E "s/(::1\s)localhost/\1/g" ~/hosts.new'

EXPOSE 9292

ENTRYPOINT ["/bin/sh", "-c" , "cat ~/hosts.new > /etc/hosts && /bin/bash -l"]
