# PXF Documentation

This directory contains the book and markdown source for the PXF docs. You can build the markdown into HTML output using [Bookbinder](https://github.com/cloudfoundry-incubator/bookbinder).  

Bookbinder is a Ruby gem that binds together a unified documentation web application from markdown, html, and/or DITA source material. The source material for bookbinder must be stored either in local directories or in GitHub repositories. Bookbinder runs [middleman](http://middlemanapp.com/) to produce a Rackup app that can be deployed locally or as a Web application.

This document provides instructions for building the PXF documentation on your local system. It includes the sections:

* [About Bookbinder](#about)
* [Prerequisites](#prereq)
* [Building the Documentation](#building)
* [Getting More Information](#moreinfo)


<a name="about"></a>
## About Bookbinder

You use bookbinder from within a project called a **book**. The book includes a configuration file named `config.yml` that specifies the documentation repositories/directories to use as source material. Bookbinder provides a set of scripts to aggregate those repositories and publish them to various locations in your final web application.

PXF provides a preconfigured **book** in the `docs/book` directory of this repo.  You can use this configuration to build HTML for the PXF docs on your local system.


<a name="prereq"></a>
## Prerequisites

* Ruby version 2.3.x.
* Use a Ruby version manager such as rvm.
* Install a Ruby [bundler](http://bundler.io/) gem package management.
* Java 1.7 or higher
* Ant

### For mac users 
```
$ gem install bundler
$ brew install ant
$ brew install v8
$ gem install therubyracer
$ gem install libv8 -v '3.16.14.7' -- --with-system-v8
```


<a name="building"></a>
## Building the Documentation

1. Navigate to the `docs/book` directory of this repo.

2. Install bookbinder and its dependent gems. Make sure you are in the `book` directory and enter:

    ``` bash
    $ bundle install
    ```

3. The `config.yml` file configures the PXF book for building from your local source files.  Build the output HTML files by executing the command:

    ``` bash
    $ bundle exec bookbinder bind local
    ```

   Bookbinder converts the markdown source into HTML, and puts the final output in a directory named `final_app`.
  
5. The `final_app` directory stages the HTML into a web application that you can view using the `rack` gem. To view a PXF documentation build:

    ``` bash
    $ cd final_app
    $ bundle install
    $ rackup
    ```

6. A local version of the PXF documentation is now available for viewing at [http://localhost:9292](http://localhost:9292)


<a name="moreinfo"></a>  
## Getting More Information

Bookbinder provides additional functionality to construct books from multiple Github repos, to perform variable substitution, and also to automatically build documentation in a continuous integration pipeline.  For more information, see [https://github.com/pivotal-cf/bookbinder](https://github.com/pivotal-cf/bookbinder).

