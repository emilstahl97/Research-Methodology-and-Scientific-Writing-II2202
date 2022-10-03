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
        CSVReadable = 0
        xlsxReadable = 0
        CSVUnreadable = 0
        xlsxUnreadable = 0
        parquetReadable = 0
        parquetUnreadable = 0
        for file in os.listdir(path):
            print('Reading: ', file)
            if file.endswith('.csv'):
                 self.read_csv_func(file)
            if file.endswith('.xlsx'):
                self.read_xlsx_func(file)
            if file.endswith('.parquet'):
                self.read_parquet_func(file)
            if file.endswith('.avro'):
                self.read_avro_func(file)
                    
                    
        print(f"CSV readable: {CSVReadable}")
        print(f"CSV unreadable: {CSVUnreadable}")
        print(f"xlsx readable: {xlsxReadable}")
        print(f"xlsx unreadable: {xlsxUnreadable}")
        print(f"parquet readable: {parquetReadable}")
        print(f"parquet unreadable: {parquetUnreadable}")
        
    
    # CSV read func
    def read_csv_func(self, filename):
        try:
            df = pd.read_csv(filename, low_memory=False)
            print("CSV readable")
            return df
        except:
            print("CSV unreadable")
            return None
    
    # xlsx read func
    def read_xlsx_func(self, filename):
        try:
            df = pd.read_excel(filename)
            print("xlsx readable")
            return df
        except:
            print("xlsx unreadable")
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
            print("Avro readable")
            return df_avro
        except:
            print("Avro unreadable")
            return None
        
    # parquet read func
    def read_parquet_func(self, filename):
        try:
            df = pd.read_parquet(filename, engine='pyarrow')
            print("parquet readable")
        except:
            print("parquet unreadable")
            return None
        


if __name__ == "__main__":
    #FlipBit().iterateFiles()
    #FlipBit().flipBit()
    originalData = '/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/data/'
    print('Read test on original data')
    FlipBit().readTest(originalData)    
    flippedData = '/Users/emilstahl/Documents/GitHub/Research-Methodology-and-Scientific-Writing-II2202/Benchmark/File-stability/flippedData/'
    print('\n\nRead test on flipped data')
    FlipBit().readTest(flippedData)