# Project Description
This project implements an interactive command-line program in Python that creates and manages index files using a B-Tree structure. You can create, insert, search, print and extract key/value pairs stored in a binary B-Tree format. Each index file contains a B-Tree of minimum degree 10, where nodes are stored in 512-byte blocks.

# Current Files

- `project3.py` : This file contains all of the code that implements this project. It has create, insert, search, load, print and extract functions. You interact with this file through the command line.
- `input.csv` : This is a premade csv file that can be used for the 'load' functionality of the B-Tree.

# How to Compile Program
All interactions happen through the command line:

- Create an index file: `python project3.py create <index_file.idx>`
- Insert a key/value pair: `python project3.py insert <index_file.idx> <key> <value>`
- Search for a key: `python project3.py search <index_file.idx> <key>`
- Load multiple key/value pairs from a CSV file: `python project3.py load <index_file.idx> <input_file.csv>`
- Print all key/value pairs in the B-Tree: `python project3.py print <index_file.idx>`
- Extract all key/value pairs to a new CSV file: `python project3.py extract <index_file.idx> <output_file.csv>`

# Additional Notes
- Each line in input csv for the load function should be comma separated with no other whitespace. Each key/value pair should be on a separate line. 