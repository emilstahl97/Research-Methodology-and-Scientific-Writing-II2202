import os
import shutil
from Bash import Bash
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
from fastavro import writer, reader, parse_schema

class FlipBit(Bash):
    def __init__(self) -> None:
        self.Bash = Bash()
        self.cwd = os.getcwd()
        self.resultsDict = {}        
        if os.path.exists(self.cwd+"/flippedResults.txt"):
            os.remove(self.cwd+"/flippedResults.txt")
        
        
    
    def iterateFiles(self) -> None:
        path = self.cwd+"/data/"
        for file in os.listdir(path):
            print(path+file)
            self.copyFile(path+file, file)
            
    
    def copyFile(self, src: str, file:str) -> None:
        dst = self.cwd+"/flippedData/"+file
        shutil.copy(src, dst)        
    
    
    def flipBit(self) -> None:
        path = self.cwd+"/flippedData/"
        for file in os.listdir(path):
            print(path+file)
            self.Bash.execute("bitflip.sh", path+file)
            print("Flipped: ", file)
       
    
    def readTest(self, path:str):
        os.chdir(path)
        
        for file in os.listdir(path):
            print('Reading: ', file)
            if file.endswith('.csv'):
                df = self.read_csv_func(file)
                self.compareDFs(df, file)
            if file.endswith('.parquet'):
                df = self.read_parquet_func(file)
                self.compareDFs(df, file)
            if file.endswith('.avro'):
                df = self.read_avro_func(file)
                self.compareDFs(df, file)
                
    
    # CSV read func
    def read_csv_func(self, filename):
        try:
            df = pd.read_csv(filename, low_memory=False)
            print("✅CSV readable")
            self.resultsDict[str(filename)]['No error'] += 1
            return df
        except:
            print("❌CSV unreadable")
            self.resultsDict[str(filename)]['Error'] += 1
            return None
        

    def read_avro_func(self, filename):
        try:
            # 1. List to store the records
            avro_records = []
            # 2. Read the Avro file
            with open(filename.replace('.csv', '.avro'), 'rb') as fo:
                avro_reader = reader(fo)
                for record in avro_reader:
                    avro_records.append(record)
            # 3. Convert to pd.DataFrame
            df_avro = pd.DataFrame(avro_records)
            print("✅Avro readable")
            self.resultsDict[str(filename)]['No error'] += 1
            return df_avro
        except:
            print("❌Avro unreadable")
            self.resultsDict[str(filename)]['Error'] += 1
            return None
        
    # parquet read func
    def read_parquet_func(self, filename):
        try:
            df = pd.read_parquet(filename, engine='pyarrow')
            print("✅parquet readable")
            self.resultsDict[str(filename)]['No error'] += 1
            return df
        except:
            print("❌parquet unreadable")
            self.resultsDict[str(filename)]['Error'] += 1
            return None
        
    
    def compareDFs(self, flippedDF, filename) -> bool:
        if flippedDF is not None:
            originalDF = self.read_csv_func(filename)
            if flippedDF.equals(originalDF):
                self.resultsDict[str(filename)]['No effect'] += 1
                with open('/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/flippedResults.txt', 'a') as f:
                    print(f"{filename} had no effect of flipping bit")
                    f.write(f"✅{filename} had no effect of flipping bit\n")
                return True
            else:
                self.resultsDict[str(filename)]['Undetected effect'] += 1
                with open('/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/flippedResults.txt', 'a') as f:
                    print(f"{filename} had an undetected effect of flipping bit")
                    f.write(f"❌{filename} had undetected effect of flipping bit\n")
                return False
        

    def initDict(self, path:str):
        from collections import defaultdict
        self.resultsDict = defaultdict(dict)
        for file in os.listdir(path):
           self.resultsDict[str(file)]['No error'] = 0
           self.resultsDict[str(file)]['Error'] = 0
           self.resultsDict[str(file)]['No effect'] = 0
           self.resultsDict[str(file)]['Undetected effect'] = 0

if __name__ == "__main__":
    flipBit = FlipBit()
    flippedData = '/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/flippedData/'
    flipBit.initDict(flippedData)
    
    for i in range(1, 2):
        print('Iteration: ', i)
        flipBit.iterateFiles()
        flipBit.flipBit()
        print('\n\nRead test on flipped data')
        flipBit.readTest(flippedData)
        
        
    df = pd.DataFrame.from_dict(flipBit.resultsDict, orient='index')    
    df.to_csv('/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/flippedResults.csv')
    df.to_excel('/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/flippedResults.xlsx')
