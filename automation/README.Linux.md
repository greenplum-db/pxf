# Running Automation on Linux

**Note:** This information was documented based on the steps taken to get automation running on a Debian Bookworm (12) system.
They are intended to be used in tandem with the information in the main README file.

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
