import sys
import os

BLOCK_SIZE = 512
EMPTY_ROOT_ID = 0
INITIAL_NEXT_BLOCK_ID = 1 

MAX_KEYS = 3
MAX_CHILDREN = 4

# Helper: Create a new node block
def create_node_block(block_id, parent_id, keys, values, children, num_keys=None):
    block = bytearray(BLOCK_SIZE)
    block[0:8] = block_id.to_bytes(8, 'big')
    block[8:16] = parent_id.to_bytes(8, 'big')
    if num_keys is None:
        num_keys = len(keys)
    block[16:24] = num_keys.to_bytes(8, 'big')

    for i in range(MAX_KEYS):
        if i < num_keys:
            block[24 + i*8 : 32 + i*8] = keys[i].to_bytes(8, 'big')
        else:
            block[24 + i*8 : 32 + i*8] = (0).to_bytes(8, 'big')

    for i in range(MAX_KEYS):
        if i < num_keys:
            block[24 + 152 + i*8 : 32 + 152 + i*8] = values[i].to_bytes(8, 'big')
        else:
            block[24 + 152 + i*8 : 32 + 152 + i*8] = (0).to_bytes(8, 'big')

    for i in range(MAX_CHILDREN):
        if i < len(children):
            block[24 + 152 + 152 + i*8 : 32 + 152 + 152 + i*8] = children[i].to_bytes(8, 'big')
        else:
            block[24 + 152 + 152 + i*8 : 32 + 152 + 152 + i*8] = (0).to_bytes(8, 'big')
    
    return block

def write_block(f, block_id, block):
    f.seek(block_id * BLOCK_SIZE)
    f.write(block)

def read_block(file, block_id):
    file.seek(block_id * BLOCK_SIZE)
    return file.read(BLOCK_SIZE)

def parse_node_block(block):
    block_id = int.from_bytes(block[0:8], 'big')
    parent_id = int.from_bytes(block[8:16], 'big')
    num_keys = int.from_bytes(block[16:24], 'big')
    
    keys = []
    values = []
    children = []
    
    for i in range(num_keys):
        keys.append(int.from_bytes(block[24 + i*8 : 32 + i*8], 'big'))
    
    for i in range(num_keys):
        values.append(int.from_bytes(block[24 + 152 + i*8 : 32 + 152 + i*8], 'big'))
    
    for i in range(MAX_CHILDREN):
        children.append(int.from_bytes(block[24 + 152 + 152 + i*8 : 32 + 152 + 152 + i*8], 'big'))
    
    return {
        'block_id': block_id,
        'parent_id': parent_id,
        'num_keys': num_keys,
        'keys': keys,
        'values': values,
        'children': children
    }

def create_file(filename):
    if os.path.exists(filename):
        print("ERROR : Index file already exists.")
        return

    header_block = bytearray(BLOCK_SIZE)
    header_block[0:8] = b'4348PRJ3'
    header_block[8:16] = EMPTY_ROOT_ID.to_bytes(8, 'big')
    header_block[16:24] = INITIAL_NEXT_BLOCK_ID.to_bytes(8, 'big')

    with open(filename, 'wb') as f:
        f.write(header_block)
    print(f"Created index file: {filename}")

def update_header(f, root_id, next_block_id):
    f.seek(8)
    f.write(root_id.to_bytes(8, 'big'))
    f.write(next_block_id.to_bytes(8, 'big'))

def key_exists(f, root_id, search_key):
    if root_id == 0:
        return False
        
    # At most 3 nodes in memory at a time
    # We'll use an iterative approach for the search
    curr_id = root_id
    while curr_id != 0:
        block = read_block(f, curr_id)
        node = parse_node_block(block)

        for i in range(node['num_keys']):
            if search_key == node['keys'][i]:
                return True
            elif search_key < node['keys'][i]:
                curr_id = node['children'][i]
                break
        else:
            curr_id = node['children'][node['num_keys']]
    return False

def insert(filename, key, value):
    if not os.path.exists(filename):
        print(f"ERROR: Index file '{filename}' does not exist.")
        return

    with open(filename, 'r+b') as f:
        # Read header
        f.seek(0)
        magic = f.read(8)
        if magic != b'4348PRJ3':
            print("ERROR: Not a valid index file.")
            return

        # Read root id and next block id
        root_id = int.from_bytes(f.read(8), 'big')
        next_block_id = int.from_bytes(f.read(8), 'big')

        # Check if key already exists
        if root_id != 0 and key_exists(f, root_id, key):
            print(f"Key {key} already exists. Insertion aborted.")
            return

        # Allocate a new block ID
        def allocate_block():
            nonlocal next_block_id
            block_id = next_block_id
            next_block_id += 1
            return block_id

        # Split a node and return the promoted key, value, and child IDs
        def split_node(node, new_key=None, new_value=None, new_child_left=None, new_child_right=None):
            # Create arrays with our new key/value to split
            keys = node['keys'].copy()
            values = node['values'].copy()
            children = node['children'].copy()
            
            # If we have a new key to insert, add it first
            if new_key is not None:
                i = 0
                while i < len(keys) and new_key > keys[i]:
                    i += 1
                    
                keys.insert(i, new_key)
                values.insert(i, new_value)
                
                # Update children if this is an internal node
                if new_child_left is not None:
                    children[i] = new_child_left
                    children.insert(i+1, new_child_right)
            
            # Find middle index for split
            mid = len(keys) // 2
            
            # Create left node
            left_id = allocate_block()
            left_keys = keys[:mid]
            left_values = values[:mid]
            left_children = children[:mid+1]
            
            # Create right node
            right_id = allocate_block()
            right_keys = keys[mid+1:]
            right_values = values[mid+1:]
            right_children = children[mid+1:]
            
            # Create and write the blocks, setting correct parent IDs
            # (parent will be updated later)
            left_block = create_node_block(left_id, node['parent_id'], left_keys, left_values, left_children, len(left_keys))
            write_block(f, left_id, left_block)
            
            right_block = create_node_block(right_id, node['parent_id'], right_keys, right_values, right_children, len(right_keys))
            write_block(f, right_id, right_block)
            
            # Update children's parent pointers
            for i, child_id in enumerate(left_children):
                if child_id != 0:
                    child_block = read_block(f, child_id)
                    child_node = parse_node_block(child_block)
                    child_node['parent_id'] = left_id
                    updated_block = create_node_block(
                        child_id, left_id, 
                        child_node['keys'], child_node['values'], 
                        child_node['children'], child_node['num_keys']
                    )
                    write_block(f, child_id, updated_block)
            
            for i, child_id in enumerate(right_children):
                if child_id != 0:
                    child_block = read_block(f, child_id)
                    child_node = parse_node_block(child_block)
                    child_node['parent_id'] = right_id
                    updated_block = create_node_block(
                        child_id, right_id, 
                        child_node['keys'], child_node['values'], 
                        child_node['children'], child_node['num_keys']
                    )
                    write_block(f, child_id, updated_block)
            
            # Return the promoted key, value, and the two new node IDs
            return keys[mid], values[mid], left_id, right_id

        def insert_recursive(node_id, key, value):
            # Read the node
            block = read_block(f, node_id)
            node = parse_node_block(block)
            
            # Find position to insert
            i = 0
            while i < node['num_keys'] and key > node['keys'][i]:
                i += 1
                
            # If this is a leaf node
            if node['children'][0] == 0:
                # If there's room, insert the key/value
                if node['num_keys'] < MAX_KEYS:
                    node['keys'].insert(i, key)
                    node['values'].insert(i, value)
                    node['num_keys'] += 1
                    
                    # Write the updated node
                    updated_block = create_node_block(
                        node['block_id'], node['parent_id'], 
                        node['keys'], node['values'], 
                        node['children'], node['num_keys']
                    )
                    write_block(f, node['block_id'], updated_block)
                    return None
                else:
                    # Node is full, split it
                    return split_node(node, key, value)
            else:
                # This is an internal node, recurse to the appropriate child
                child_id = node['children'][i]
                result = insert_recursive(child_id, key, value)
                
                # If no split occurred, we're done
                if result is None:
                    return None
                    
                # Otherwise, handle the split
                promoted_key, promoted_value, left_id, right_id = result
                
                # If there's room in this node, insert the promoted key
                if node['num_keys'] < MAX_KEYS:
                    node['keys'].insert(i, promoted_key)
                    node['values'].insert(i, promoted_value)
                    node['children'][i] = left_id
                    node['children'].insert(i+1, right_id)
                    node['num_keys'] += 1
                    
                    # Write the updated node
                    updated_block = create_node_block(
                        node['block_id'], node['parent_id'], 
                        node['keys'], node['values'], 
                        node['children'], node['num_keys']
                    )
                    write_block(f, node['block_id'], updated_block)
                    return None
                else:
                    # This node is full too, split it
                    return split_node(node, promoted_key, promoted_value, left_id, right_id)

        # Handle empty tree case
        if root_id == 0:
            # Create the root node
            root_id = allocate_block()
            root_block = create_node_block(root_id, 0, [key], [value], [0] * MAX_CHILDREN, 1)
            write_block(f, root_id, root_block)
            
            # Update the header
            update_header(f, root_id, next_block_id)
            print(f"Inserted key {key} as root.")
            return
        
        # Insert into the tree
        result = insert_recursive(root_id, key, value)
        
        # If a split occurred at the root, create a new root
        if result is not None:
            promoted_key, promoted_value, left_id, right_id = result
            
            # Create a new root
            new_root_id = allocate_block()
            children = [left_id, right_id] + [0] * (MAX_CHILDREN - 2)
            root_block = create_node_block(new_root_id, 0, [promoted_key], [promoted_value], children, 1)
            write_block(f, new_root_id, root_block)
            
            # Update the parent pointers of the children
            for node_id in [left_id, right_id]:
                if node_id != 0:
                    child_block = read_block(f, node_id)
                    child_node = parse_node_block(child_block)
                    child_node['parent_id'] = new_root_id
                    updated_block = create_node_block(
                        node_id, new_root_id, 
                        child_node['keys'], child_node['values'], 
                        child_node['children'], child_node['num_keys']
                    )
                    write_block(f, node_id, updated_block)
            
            # Update the header
            update_header(f, new_root_id, next_block_id)
            print(f"Root was split. New root created with key {promoted_key}.")
        else:
            # Update the header (next_block_id might have changed)
            update_header(f, root_id, next_block_id)
            print(f"Inserted key {key}.")

def create_file(filename):
    if os.path.exists(filename):
        print("ERROR : Index file already exists.")
        return

    header_block = bytearray(BLOCK_SIZE)
    header_block[0:8] = b'4348PRJ3'
    header_block[8:16] = EMPTY_ROOT_ID.to_bytes(8, 'big')
    header_block[16:24] = INITIAL_NEXT_BLOCK_ID.to_bytes(8, 'big')

    with open(filename, 'wb') as f:
        f.write(header_block)

def key_exists(f, root_id, search_key):
    curr_id = root_id
    while curr_id != 0:
        block = read_block(f, curr_id)
        node = parse_node_block(block)

        for i in range(node['num_keys']):
            if search_key == node['keys'][i]:
                return True
            elif search_key < node['keys'][i]:
                curr_id = node['children'][i]
                break
        else:
            curr_id = node['children'][node['num_keys']]
    return False

def search(filename, search_key):
    with open(filename, 'rb') as f:
        if f.read(8) != b'4348PRJ3':
            print("ERROR : Not a valid index file.")
            return

        root_block = int.from_bytes(f.read(8), 'big')
        if root_block == 0:
            print("ERROR : Tree is empty.")
            return

        curr_id = root_block
        while curr_id != 0:
            block = read_block(f, curr_id)

            num_keys = int.from_bytes(block[16:24], 'big')
            keys = [int.from_bytes(block[24 + i*8 : 32 + i*8], 'big') for i in range(num_keys)]
            values = [int.from_bytes(block[176 + i*8 : 184 + i*8], 'big') for i in range(num_keys)]
            children = [int.from_bytes(block[328 + i*8 : 336 + i*8], 'big') for i in range(num_keys + 1)]

            for i in range(num_keys):
                if search_key == keys[i]:
                    print(f"Key {search_key} found with value {values[i]}.")
                    return
                elif search_key < keys[i]:
                    curr_id = children[i]
                    break
            else:
                curr_id = children[num_keys]

        print(f"Key {search_key} not found.")

def load(index_filename, csv_filename):
    if not os.path.exists(index_filename):
        print(f"Error: Index file '{index_filename}' does not exist.")
        return

    if not os.path.exists(csv_filename):
        print(f"Error: CSV file '{csv_filename}' does not exist.")
        return

    with open(index_filename, 'rb') as f:
        if f.read(8) != b'4348PRJ3':
            print("ERROR : Not a valid index file.")
            return

    with open(csv_filename, 'r') as csv_file:
        for line_num, line in enumerate(csv_file, 1):
            line = line.strip()
            if not line:
                continue
            try:
                key_str, value_str = line.split(',')
                key = int(key_str.strip())
                value = int(value_str.strip())
                insert(index_filename, key, value)
            except ValueError:
                print(f"Error: Invalid format in line {line_num}: '{line}'")
                continue

def print_index(filename, file_handle=None):
    if not os.path.exists(filename):
        print(f"Error: Index file '{filename}' does not exist.")
        return

    with open(filename, 'rb') as f:
        if f.read(8) != b'4348PRJ3':
            print("ERROR : Not a valid index file.")
            return

        root_id = int.from_bytes(f.read(8), 'big')
        if root_id == 0:
            print("Index is empty.")
            return

        def traverse(f, block_id):
            block = read_block(f, block_id)
            num_keys = int.from_bytes(block[16:24], 'big')

            keys = [int.from_bytes(block[24 + i*8 : 32 + i*8], 'big') for i in range(num_keys)]
            values = [int.from_bytes(block[176 + i*8 : 184 + i*8], 'big') for i in range(num_keys)]
            children = [int.from_bytes(block[328 + i*8 : 336 + i*8], 'big') for i in range(num_keys + 1)]

            for i in range(num_keys):
                if children[0] != 0:
                    if children[i] != 0:
                        traverse(f, children[i])
                line = f"{keys[i]},{values[i]}"
                if file_handle:
                    file_handle.write(line + '\n')
                else:
                    print(line)
            if children[0] != 0 and children[num_keys] != 0:
                traverse(f, children[num_keys])

        traverse(f, root_id)

def extract(index_filename, csv_filename):
    if not os.path.exists(index_filename):
        print(f"Error: Index file '{index_filename}' does not exist.")
        return

    if os.path.exists(csv_filename):
        print(f"Error: Output file '{csv_filename}' already exists.")
        return

    with open(index_filename, 'rb') as f:
        if f.read(8) != b'4348PRJ3':
            print("ERROR : Not a valid index file.")
            return

    with open(csv_filename, 'w') as outfile:
        print_index(index_filename, outfile)

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
