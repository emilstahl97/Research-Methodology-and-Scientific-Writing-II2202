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
       
    
    def readTest(self, path:str):
        os.chdir(path)
        CSVReadable = 0
        xlsxReadable = 0
        CSVUnreadable = 0
        xlsxUnreadable = 0
        parquetReadable = 0
        parquetUnreadable = 0
        for file in os.listdir(path):
            if file.endswith('.csv'):
                 try:
                    df = pd.read_csv(file, engine='python')
                    print(f"File {file} is readable")
                    CSVReadable += 1    
                 except:
                        print(f"File {file} is not readable")
                        CSVUnreadable += 1
            if file.endswith('.xlsx'):
                try:
                    df = pd.read_excel(file, engine='openpyxl', sheetname="sheet1")
                    print(f"File {file} is readable")
                    xlsxReadable += 1
                except:
                    print(f"File {file} is not readable")
                    xlsxUnreadable += 1
            if file.endswith('.parquet'):
                try:
                    df = pd.read_parquet(
                        path, 
                        engine='auto', 
                        columns=None, 
                        storage_options=None, 
                        use_nullable_dtypes=False
                        )
                    print(f"File {file} is readable")
                    parquetReadable += 1
                except:
                    print(f"File {file} is not readable")
                    parquetUnreadable += 1
                    
                    
        print(f"CSV readable: {CSVReadable}")
        print(f"CSV unreadable: {CSVUnreadable}")
        print(f"xlsx readable: {xlsxReadable}")
        print(f"xlsx unreadable: {xlsxUnreadable}")
        print(f"parquet readable: {parquetReadable}")
        print(f"parquet unreadable: {parquetUnreadable}")


if __name__ == "__main__":
    #FlipBit().iterateFiles()
    #FlipBit().flipBit()
    originalData = '/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/data/'
    print('Read test on original data')
    FlipBit().readTest(originalData)    
    flippedData = '/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/flippedData/'
    print('\n\nRead test on flipped data')
    FlipBit().readTest(flippedData)