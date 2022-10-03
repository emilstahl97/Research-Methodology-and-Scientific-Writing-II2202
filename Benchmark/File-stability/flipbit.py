import os
import shutil
from Bash import Bash
import pandas as pd


class FlipBit(Bash):
    def __init__(self) -> None:
        self.Bash = Bash()
        self.cwd = os.getcwd()
        
    
    def iterateFiles(self) -> None:
        path = os.getcwd()+"/data/"
        for file in os.listdir(path):
            print(path+file)
            self.copyFile(path+file, file)
            
    
    def copyFile(self, src: str, file:str) -> None:
        dst = os.getcwd()+"/flippedData/"+file
        shutil.copy(src, dst)        
    
    
    def flipBit(self) -> None:
        path = os.getcwd()+"/flippedData/"
        for file in os.listdir(path):
            print(path+file)
            self.Bash.execute("bitflip.sh", path+file)
            print("Flipped: ", file)
       
    
    def readTest(self):
        path = '/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/flippedData/'
        os.chdir(path)
        for file in os.listdir(path):
            if file.endswith('.csv'):
                 try:
                    df = pd.read_csv(file, engine='python')
                    print("File is readable")
                 except:
                        print("File is not readable")


if __name__ == "__main__":
    FlipBit().iterateFiles()
    FlipBit().flipBit()
    FlipBit().readTest()    