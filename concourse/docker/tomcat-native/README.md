# How to build the Apache Tomcat Native Library locally

## Prerequisites

- Java 1.8 or Java 11
- Install OpenSSL version 1.0.2 or higher
- Install APR version 1.4.0 or higher

In Mac, run `openssl version` to check you have the required version of OpenSSL.

### Additional prerequisites for Mac

- autom4te: `brew install m4`
- Command Line Tools: `xcode-select --install`

## Download APR and tomcat-native sources

You will need Apache APR and Apache Tomcat Native sources

### APR

Use git clone to get Apache APR sources.

```shell
cd ~/workspace
git clone https://github.com/apache/apr
```

### Tomcat Native

Use git clone to get Apache Tomcat Native sources. We use the version that PXF
is using. (This step assumes that PXF sources are under `~/workspace/pxf`).

```shell
cd ~/workspace
git clone https://github.com/apache/tomcat-native
cd tomcat-native
git checkout $(cat ~/workspace/pxf/concourse/docker/tomcat-native/version)
```

### Build Tomcat Native

You will need to get the location of your OpenSSL installation. In Mac, run
`brew info openssl`:

```shell
±  |tag:1.2.26 ✓| → brew info openssl
openssl@1.1: stable 1.1.1i (bottled) [keg-only]
Cryptography and SSL/TLS Toolkit
https://openssl.org/
/usr/local/Cellar/openssl@1.1/1.1.1i (8,067 files, 18.5MB)
  Poured from bottle on 2021-02-09 at 11:40:47
From: https://github.com/Homebrew/homebrew-core/blob/HEAD/Formula/openssl@1.1.rb
License: OpenSSL
==> Caveats
A CA file has been bootstrapped using certificates from the system
keychain. To add additional certificates, place .pem files in
  /usr/local/etc/openssl@1.1/certs

and run
  /usr/local/opt/openssl@1.1/bin/c_rehash

openssl@1.1 is keg-only, which means it was not symlinked into /usr/local,
because macOS provides LibreSSL.

If you need to have openssl@1.1 first in your PATH, run:
  echo 'export PATH="/usr/local/opt/openssl@1.1/bin:$PATH"' >> /Users/<username>/.bash_profile

For compilers to find openssl@1.1 you may need to set:
  export LDFLAGS="-L/usr/local/opt/openssl@1.1/lib"
  export CPPFLAGS="-I/usr/local/opt/openssl@1.1/include"

For pkg-config to find openssl@1.1 you may need to set:
  export PKG_CONFIG_PATH="/usr/local/opt/openssl@1.1/lib/pkgconfig"

==> Analytics
install: 700,995 (30 days), 2,254,517 (90 days), 8,215,152 (365 days)
install-on-request: 108,243 (30 days), 356,561 (90 days), 1,188,466 (365 days)
build-error: 0 (30 days)
```

Identify the installation directory of `OpenSSL`, in the output above we can
see that `OpenSSL` installation dir is `/usr/local/Cellar/openssl@1.1/1.1.1i`.

```shell
cd ~/workspace/tomcat-native/native
sh buildconf --with-apr=${HOME}/workspace/apr
./configure --with-ssl=/usr/local/Cellar/openssl@1.1/1.1.1i
make
```

This should produce the native libraries inside `.libs/`.

Finally, copy all the files from `.libs` to your `$PXF_BASE/lib/native`.

```bash
cp .libs/* $PXF_BASE/lib/native
```