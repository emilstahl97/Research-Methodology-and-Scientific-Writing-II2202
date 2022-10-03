import os
import shutil
from Bash import Bash


class FlipBit(Bash):
    def __init__(self) -> None:
        self.Bash = Bash()
        
    
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
       
    


if __name__ == "__main__":
    FlipBit().iterateFiles()
    FlipBit().flipBit()