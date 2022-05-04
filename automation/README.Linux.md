# Running Automation on Linux

**Note:** This information was documented based on the steps taken to get automation running on a Debian Bullseye system.

## Additional SSH Setup

In addition to [updating the `sshd_config`](README.md#ssh-setup), you must have an RSA key for the local system ([you're not still using RSA keys for SSH are you?][ssh-ed25519])

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
