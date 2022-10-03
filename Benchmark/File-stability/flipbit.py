import os
from Bash import Bash


class FlipBit(Bash):
    def __init__(self) -> None:
        self.Bash = Bash()
        
    
    def flip(self, bit: int) -> None:
        for files in os.listdir('./data'):
            print(files)


if __name__ == "__main__":
    FlipBit().flip(1)