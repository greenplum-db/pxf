# Running Automation on Linux

**Note:** This information was documented based on the steps taken to get automation running on a Debian Bullseye system.

## Python 2 Setup

If you are following [the python instructions](README.md#python-2-setup), but do not have pip installed.
The following is how I was able to install `pip` for python2 and the dependencies for `tinc` on my system and do so in a way that keeps it isolate from the system `python` installation.

```bash
curl 'https://bootstrap.pypa.io/pip/2.7/get-pip.py' -o get-pip.py

# please review the contents of get-pip.py *before* executing this script
# in the version I downloaded, it passed all command line args to pip bootstrap process
chmod 0755 get-pip.py
./get-pip.py --user

python2 -m pip install --user paramiko
```
