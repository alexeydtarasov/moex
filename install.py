import sys
import os


if 'linux' in sys.platform.lower() or 'mac' in sys.platform.lower():
    version = sys.version_info
    version = str(version.major) + '.' + str(version.minor)
    print('Enter password to execute:')
    cmd = 'sudo cp ./moex.py /usr/lib/python' + version
    print(cmd)
    os.system(cmd)

print('Installation finished!')

