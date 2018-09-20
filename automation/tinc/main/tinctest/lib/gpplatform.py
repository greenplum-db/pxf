import os
import platform

from qautils.gppylib.commands.base import Command

def pulse_info():
    """
    @description: This is mainly for Pulse and Build integration.
        Pulse builds each platform accordingly:
        RHEL5 = (rhel5_x86_64, RHEL5-x86_64)
        RHEL6 = (rhel6_x86_64, RHEL6-x86_64)
        OSX = (osx105_x86, OSX-i386)
        SUSE = (suse10_x86_64, SuSE10-x86_64)
        SOL = (sol10_x86_64, SOL-x86_64)
    @note: Pulse uses the lower case platform while the GPDB binary build uses the upper case platform.
    @return: the platform and related installation file name
    @rtype : (String, String)
    """
    os_name = get_info()
    if os_name == 'RHEL5':
        url_platform = 'rhel5_x86_64'
        url_bin = 'RHEL5-x86_64'
    elif os_name == 'RHEL6':
        url_platform = 'rhel5_x86_64'
        url_bin = 'RHEL5-x86_64'
    elif os_name == 'OSX':
        url_platform = 'osx105_x86'
        url_bin = 'OSX-i386'
    elif os_name == 'SUSE':
        url_platform = 'suse10_x86_64'
        url_bin = 'SuSE10-x86_64'
    elif os_name == 'SOL':
        url_platform = 'sol10_x86_64'
        url_bin = 'SOL-x86_64'
    else:
        raise Exception("Continuous Integration (Pulse) does not support this platform")
    return (url_platform, url_bin)

def get_info():
    """
    Get the current platform
    @return: type platform of the current system
    @rtype : String
    """
    myos = platform.system()
    if myos == "Darwin":
        return 'OSX'
    elif myos == "Linux":
        if os.path.exists("/etc/SuSE-release"):
            return 'SUSE'
        elif os.path.exists("/etc/redhat-release"):
            cmd_str = "cat /etc/redhat-release"
            cmd = Command("run cat for RHEL version", cmd_str)
            cmd.run()
            result = cmd.get_results()
            msg = result.stdout
            if msg.find("5") != -1:
                return 'RHEL5'
            else:
                return 'RHEL6'
    elif myos == "SunOS":
        return 'SOL'
    return None
