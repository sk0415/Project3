import sys
import os

BLOCK_SIZE = 512
EMPTY_ROOT_ID = 0
INITAL_NEXT_BLOCK_ID = 1

MAX_KEYS = 19
MAX_CHILDREN = 20

def create_file( filename ):
    if os.path.exists( filename ):
        print( "ERROR : Index file already exists.")
        return
    
    header_block = bytearray( BLOCK_SIZE )
    header_block[ 0 : 8 ] = b'4348PRJ3'
    header_block[ 8 : 16 ] = EMPTY_ROOT_ID.to_bytes( 8 , byteorder = 'big' )
    header_block[ 16 : 24 ] = INITAL_NEXT_BLOCK_ID.to_bytes( 8 , byteorder = 'big' )

    with open( filename , 'wb' ) as f:
        f.write( header_block )

def insert( filename, key , value ):
    print()
        

def read_block( file , block_id ):
    file.seek( block_id * 512 )
    return file.read( 512 )

def search( filename , search_key ):
    with open( filename , 'rb' ) as f:
        if f.read(8) != b'4348PRJ3':
            print( "ERROR : Not a valid index file." )
            return
        
        root_block = int.from_bytes( f.read(8) , 'big' )
        if root_block == 0:
            print( "ERROR : Tree is empty." )
            return
        
        curr_id = root_block
        while curr_id != 0:
            block = read_block( f , curr_id )

            num_keys = int.from_bytes( block[16:24] , 'big' )
            keys = [int.from_bytes(block[24 + i*8 : 32 + i*8], 'big') for i in range(num_keys)]
            values = [int.from_bytes(block[176 + i*8 : 184 + i*8], 'big') for i in range(num_keys)]
            children = [int.from_bytes(block[328 + i*8 : 336 + i*8], 'big') for i in range(num_keys + 1)]

            for i in range( num_keys ):
                if search_key == keys[i]:
                    print( f"Key {search_key} found with value {values[i]}.")
                    return
                elif search_key < keys[i]:
                    curr_id = children[i]
                    break
                else:
                    curr_id = children[num_keys]
        print( f"Key {search_key} not found.")

def load( index_filename , csv_filename ):
    if not os.path.exists( index_filename ):
        print(f"Error: Index file '{index_filename}' does not exist.")
        return
    
    if not os.path.exists(csv_filename):
        print(f"Error: CSV file '{csv_filename}' does not exist.")
        return
    
    with open( index_filename , 'rb' ) as f:
        if f.read(8) != b'4348PRJ3':
            print( "ERROR : Not a valid index file." )
            return
        
    with open( csv_filename , 'r' ) as csv_file:
        for line_num, line in enumerate( csv_file , 1 ):
            line = line.strip()
            if not line:
                continue
            try:
                key_str, value_str = line.split(',')
                key = int( key_str.strip())
                value = int( value_str.strip())
                insert( index_filename , key, value )
            except ValueError:
                print(f"Error: Invalid format in line {line_num}: '{line}'")
                continue
        
def print_index():
    print()

def extract():
    print()

def main():
    command = sys.argv[1]
    if command == 'create':
        create_file(sys.argv[2])
    elif command == 'insert':
        insert(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
    elif command == 'search':
        search(sys.argv[2], int(sys.argv[3]))
    elif command == 'load':
        load(sys.argv[2], sys.argv[3])
    elif command == 'print':
        print_index(sys.argv[2])
    elif command == 'extract':
        extract(sys.argv[2], sys.argv[3])
    else:
        print("Unknown command")


if __name__ == "__main__":
    main()