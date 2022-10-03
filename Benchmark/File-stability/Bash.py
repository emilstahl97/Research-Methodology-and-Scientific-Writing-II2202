import os
import subprocess


class Bash:
    
    def __init__(self) -> None:
        self.cwd = os.getcwd()
    
    def execute(self, file: str, arg1 = None, arg2 = None, arg3 = None, arg4 = None) -> None:
        print("Running: ", file)
        os.chdir(self.cwd+"/scripts")
        subprocess.run(f"/bin/bash {file} {arg1} {arg2} {arg3} {arg4}", shell=True)
        print("Done: ", file)