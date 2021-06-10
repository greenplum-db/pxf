# Running Automation on Linux

**Note:** This information was documented based on the steps taken to get automation running on a Debian Bullseye system.

## SSH Setup

The PXF automation project uses an old SSH2 Java library that does not support newer key exchange algorithms (`KexAlgorithms`).
The ones that it does support are not enabled by default in Debian's openssh-server package (1:8.4p1-5).
You can check the supported algorithms with

```bash
sudo sshd -T | grep 'kexalgorithms'
```

The following algorithms _must_ be included:

- diffie-hellman-group-exchange-sha1
- diffie-hellman-group14-sha1
- diffie-hellman-group1-sha1

If they are not, you can enable them with the following config file:

```bash
sudo cat <<EOF >/etc/ssh/sshd_config.d
# pxf automation uses an old SSH2 Java library that doesn't support newer KexAlgorithms
# this assumes that /etc/ssh/sshd_config contains "Include /etc/ssh/sshd_config.d/*.conf"
# if it doesn't, try adding this directly to /etc/ssh/sshd_config
KexAlgorithms +diffie-hellman-group-exchange-sha1,diffie-hellman-group14-sha1,diffie-hellman-group1-sha1
EOF

sudo systemctl restart sshd
```

In addition, you must have an RSA key for the local system ([you're not still using RSA keys for SSH are you?][ssh-ed25519])

```bash
# requires an id_rsa key in PEM format
# private key *must* be stored in id_rsa
ssh-keygen -m PEM -t rsa -b 4096 -C "pxf-automation"
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```

## Python 2 Setup

While automation is a Java project, it uses a (no longer maintained) Python utility called [`tinc`](./tinc/main).
Unfortunately, this utility is still written in python 2.7.
This is problematic because:

- its dependencies (e.g., `paramiko` and `psi`) are no longer available as system packages
- `pip` is not available as a system package anymore (confirmed on Ubuntu 20.04)

The following is how I was able to install `pip` for python2 and the dependencies for `tinc` on my system and do so in a way that keeps it isolate from the system `python` installation.

```bash
curl 'https://bootstrap.pypa.io/pip/2.7/get-pip.py' -o get-pip.py

# please review the contents of get-pip.py *before* executing this script
# in the version I downloaded, it passed all command line args to pip bootstrap process
chmod 0755 get-pip.py
./get-pip.py --user

python2 -m pip install --user paramiko psi
```

[ssh-ed25519]: https://medium.com/risan/upgrade-your-ssh-key-to-ed25519-c6e8d60d3c54
