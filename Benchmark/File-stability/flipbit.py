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
        self.CSVReadable = 0
        self.avroReadable = 0
        self.xlsxReadable = 0
        self.CSVUnreadable = 0
        self.avroUnreadable = 0
        self.xlsxUnreadable = 0
        self.parquetReadable = 0
        self.parquetUnreadable = 0
        
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
                    
                    
        print(f"CSV readable: {self.CSVReadable}")
        print(f"CSV unreadable: {self.CSVUnreadable}")
        print(f"xlsx readable: {self.xlsxReadable}")
        print(f"xlsx unreadable: {self.xlsxUnreadable}")
        print(f"parquet readable: {self.parquetReadable}")
        print(f"parquet unreadable: {self.parquetUnreadable}")
        print(f"avro readable: {self.avroReadable}")
        print(f"avro unreadable: {self.avroUnreadable}")
        
    
    # CSV read func
    def read_csv_func(self, filename):
        try:
            df = pd.read_csv(filename, low_memory=False)
            print("✅CSV readable")
            self.CSVReadable += 1
            return df
        except:
            self.CSVUnreadable += 1
            print("❌CSV unreadable")
            return None
    
    # xlsx read func
    def read_xlsx_func(self, filename):
        try:
            df = pd.read_excel(filename)
            self.xlsxReadable += 1
            print("✅xlsx readable")
            return df
        except:
            self.xlsxUnreadable += 1
            print("❌xlsx unreadable")
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
            self.avroReadable += 1
            return df_avro
        except:
            self.avroUnreadable += 1
            print("❌Avro unreadable")
            return None
        
    # parquet read func
    def read_parquet_func(self, filename):
        try:
            df = pd.read_parquet(filename, engine='pyarrow')
            print("✅parquet readable")
            self.parquetReadable += 1
            return df
        except:
            self.parquetUnreadable += 1
            print("❌parquet unreadable")
            return None
        
    
    def compareDFs(self, flippedDF, filename) -> bool:
        if flippedDF is not None:
            originalDF = self.read_csv_func(filename)
            if flippedDF.equals(originalDF):
                with open('/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/flippedResults.txt', 'a') as f:
                    print(f"{filename} had no effect of flipping bit")
                    f.write(f"✅{filename} had no effect of flipping bit\n")
                return True
            else:
                with open('/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/flippedResults.txt', 'a') as f:
                    print(f"{filename} had an undetected effect of flipping bit")
                    f.write(f"❌{filename} had undetected effect of flipping bit\n")
                return False
        


if __name__ == "__main__":
    FlipBit().iterateFiles()
    FlipBit().flipBit()
    originalData = '/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/data/'
    #print('Read test on original data')
    #FlipBit().readTest(originalData)    
    flippedData = '/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/flippedData/'
    print('\n\nRead test on flipped data')
    FlipBit().readTest(flippedData)