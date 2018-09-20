from qautils.gppylib.commands.base import Command

def install_rpm(location, dir, name):
    """
    Install RPM package
    @param location: location of rpm packages
    @param dir: install directory
    @param name: package name
    @return: package name
    """
    package_location = location + '/' + name
    rpm_cmd = 'sudo rpm --force --prefix %s -i %s' % (dir, package_location)
    cmd = Command(name='install rpm', cmdStr=rpm_cmd)
    cmd.run(validateAfter=True)
    
    return get_package_name(package_location)

def delete_rpm(name):
    """
    Delete RPM package
    @param name: package name
    """
    rpm_cmd = 'sudo rpm -e %s' % (name)
    cmd = Command(name='delete rpm', cmdStr=rpm_cmd)
    cmd.run(validateAfter=True)

def get_package_name(name):
    """
    Get RPM package name
    @param dir: directory
    @param name: rpm packagge 
    """
    rpm_cmd = 'rpm -qp %s' % (name)
    cmd = Command(name='get rpm package name', cmdStr=rpm_cmd)
    cmd.run(validateAfter=True)
    result = cmd.get_results()
    return result.stdout
    
def run_gppkg(pgport, gphome, mdd, loc, options="--install"):
    gppkg_cmd = "export PGPORT=%s; export MASTER_DATA_DIRECTORY=%s; source %s/greenplum_path.sh; gppkg %s %s" % (pgport, mdd, gphome, options, loc)
    cmd = Command(name = "Run gppkg", cmdStr = gppkg_cmd)
    cmd.run(validateAfter = True)
    result = cmd.get_results()
    return result.stdout
