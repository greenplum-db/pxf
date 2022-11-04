# Running Automation on Linux

**Note:** This information was documented based on the steps taken to get automation running on a Debian Bookworm (12) system.

## Locale Setup

Automation creates a GPDB database using the `ru_RU.CP1251` locale. You can generate the required locale files with

```sh
sudo sed -i.bak -e 's/# ru_RU.CP1251.*/ru_RU.CP1251 CP1251/' /etc/locale.gen
sudo locale-gen
```

After generating the locale, restart your GPDB cluster

```sh
source $GPHOME/greenplum_path.sh
gpstop -a
gpstart -a
```

## SSH Setup

```sh
sudo tee /etc/ssh/sshd_config.d/pxf-automation.conf >/dev/null <EOF
# pxf automation uses an old SSH2 Java library that doesn't support newer KexAlgorithms
# this assumes that /etc/ssh/sshd_config contains "Include /etc/ssh/sshd_config.d/*.conf"
# if it doesn't, try adding this directly to /etc/ssh/sshd_config
KexAlgorithms +diffie-hellman-group-exchange-sha1,diffie-hellman-group14-sha1,diffie-hellman-group1-sha1
HostKeyAlgorithms +ssh-rsa,ssh-dss
PubkeyAcceptedAlgorithms +ssh-rsa,ssh-dss
EOF
```

## Python 2 Setup

If you are following [the python instructions](README.md#python-2-setup), but do not have pip installed.
The following is how I was able to install `pip` for python2 and the dependencies for `tinc` on my system and do so in a way that keeps it isolated from the system `python` installation.

```bash
curl 'https://bootstrap.pypa.io/pip/2.7/get-pip.py' | sed -e '1/python/python2/' > get-pip.py

# please review the contents of get-pip.py *before* executing this script
# in the version I downloaded, it passed all command line args to pip bootstrap process
chmod 0755 get-pip.py
./get-pip.py --user

python2 -m pip install --user paramiko
```
