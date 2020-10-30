FROM ruby:2.3

RUN /bin/bash -l -c "gem install bundler --no-document -v '=1.16.1'"

RUN /bin/bash -l -c "mkdir /temprackup"

RUN /bin/bash -l -c "echo $'source \'http://rubygems.org\' \n\
    ruby \'~> 2.3.0\'\n\
    gem \'rack\', \'~> 2.0.1\'\n\
    gem \'rack-rewrite\'\n\
    gem \'puma\' \n\
    gem \'therubyracer\' \n\
    gem \'sendgrid-ruby\', \'< 3.0\' \n\
    gem \'elasticsearch\' ' > /temprackup/Gemfile"

RUN /bin/bash -l -c "cd /temprackup && bundle install"

RUN /bin/bash -l -c "mkdir /temp"
RUN echo 'source "https://rubygems.org"' > /temp/Gemfile
RUN echo "gem 'bookbindery'" >> /temp/Gemfile
RUN echo "gem 'libv8', '3.16.14.7'" >> /temp/Gemfile
RUN /bin/bash -l -c "cd /temp && bundle install"

RUN /bin/bash -l -c "cp /etc/hosts ~/hosts.new"
RUN /bin/bash -l -c 'sed -i -E "s/(::1\s)localhost/\1/g" ~/hosts.new'

RUN echo "export PATH=$PATH:/usr/local/bundle/bin" >> /etc/profile
RUN echo "alias rackup='rackup -o 0.0.0'" >> /etc/profile

EXPOSE 9292

ENTRYPOINT ["/bin/sh", "-c" , "cat ~/hosts.new > /etc/hosts && . /etc/profile && alias rackup='rackup -o 0.0.0.0' && /bin/bash -l" ]
