import os
import subprocess
import sys


cur_path = os.getcwd()

if 'linux' in sys.platform.lower() or 'mac' in sys.platform.lower():
    shell = subprocess.check_output('echo $SHELL', shell=True).decode().strip().split('/')[-1]
    print(shell)
    os.system(f'echo "export PYTHONPATH=$PYTHONPATH:{cur_path}" >> ~/.{shell}rc')

