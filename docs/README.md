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

<a name="building_docker"></a>
## Building the Documentation Using Docker

1. Navigate to the `concourse/docker/docs` directory of this repo.

2. Follow the instructions in the README.md file.

3. A local version of the documentation should be available for viewing at [http://localhost:9292](http://localhost:9292)


<a name="moreinfo"></a>  
## Getting More Information

Bookbinder provides additional functionality to construct books from multiple Github repos, to perform variable substitution, and also to automatically build documentation in a continuous integration pipeline.  For more information, see [https://github.com/pivotal-cf/bookbinder](https://github.com/pivotal-cf/bookbinder).

