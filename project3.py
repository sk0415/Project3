import sys
import os

BLOCK_SIZE = 512
EMPTY_ROOT_ID = 0
INITIAL_NEXT_BLOCK_ID = 1 

MAX_KEYS = 19
MAX_CHILDREN = 20

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

def update_header(f, root_id, next_block_id):
    f.seek(8)
    f.write(root_id.to_bytes(8, 'big'))
    f.write(next_block_id.to_bytes(8, 'big'))

def key_exists(f, root_id, search_key):
    if root_id == 0:
        return False
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
        f.seek(0)
        magic = f.read(8)
        if magic != b'4348PRJ3':
            print("ERROR: Not a valid index file.")
            return

        root_id = int.from_bytes(f.read(8), 'big')
        next_block_id = int.from_bytes(f.read(8), 'big')

        def allocate_block():
            nonlocal next_block_id
            block_id = next_block_id
            next_block_id += 1
            return block_id

        def update_header_local(root_id):
            nonlocal next_block_id
            f.seek(8)
            f.write(root_id.to_bytes(8, 'big'))
            f.write(next_block_id.to_bytes(8, 'big'))

        if root_id != 0 and key_exists(f, root_id, key):
            print(f"Key {key} already exists.")
            return

        if root_id == 0:
            root_id = allocate_block()
            block = create_node_block(root_id, 0, [key], [value], [0] * MAX_CHILDREN, 1)
            write_block(f, root_id, block)
            update_header_local(root_id)
            # print(f"Inserted key {key} as root.")
            return

        path = []
        curr_id = root_id

        while curr_id != 0:
            path.append(curr_id)
            block = read_block(f, curr_id)
            node = parse_node_block(block)
            i = 0
            while i < node['num_keys'] and key > node['keys'][i]:
                i += 1
            if node['children'][0] == 0:
                break
            curr_id = node['children'][i]

        curr_id = path[-1]
        block = read_block(f, curr_id)
        node = parse_node_block(block)

        i = 0
        while i < node['num_keys'] and key > node['keys'][i]:
            i += 1
        node['keys'].insert(i, key)
        node['values'].insert(i, value)
        node['num_keys'] += 1

        while True:
            if node['num_keys'] <= MAX_KEYS:
                updated_block = create_node_block(
                    node['block_id'], node['parent_id'],
                    node['keys'][:node['num_keys']],
                    node['values'][:node['num_keys']],
                    node['children'],
                    node['num_keys']
                )
                write_block(f, node['block_id'], updated_block)
                update_header_local(root_id)
                # print(f"Inserted key {key}.")
                return

            mid = node['num_keys'] // 2
            promoted_key = node['keys'][mid]
            promoted_value = node['values'][mid]

            left_keys = node['keys'][:mid]
            left_values = node['values'][:mid]
            left_children = node['children'][:mid + 1]
            right_keys = node['keys'][mid + 1:]
            right_values = node['values'][mid + 1:]
            right_children = node['children'][mid + 1:] + [0] * (MAX_CHILDREN - len(node['children'][mid + 1:]))

            left_id = node['block_id']
            right_id = allocate_block()

            if len(path) <= 1:
                parent_id = 0
                new_root_id = allocate_block()
                new_children = [left_id, right_id] + [0] * (MAX_CHILDREN - 2)
                new_root = create_node_block(
                    new_root_id, 0,
                    [promoted_key], [promoted_value],
                    new_children,
                    1
                )
                write_block(f, new_root_id, new_root)
                parent_id = new_root_id
                root_id = new_root_id
            else:
                parent_id = path[-2]

            write_block(f, left_id, create_node_block(
                left_id, parent_id, left_keys, left_values, left_children, len(left_keys)))
            write_block(f, right_id, create_node_block(
                right_id, parent_id, right_keys, right_values, right_children, len(right_keys)))

            for child_id in left_children:
                if child_id != 0:
                    child_block = read_block(f, child_id)
                    child_node = parse_node_block(child_block)
                    child_node['parent_id'] = left_id
                    updated_child = create_node_block(
                        child_id, left_id,
                        child_node['keys'][:child_node['num_keys']],
                        child_node['values'][:child_node['num_keys']],
                        child_node['children'],
                        child_node['num_keys']
                    )
                    write_block(f, child_id, updated_child)

            for child_id in right_children:
                if child_id != 0:
                    child_block = read_block(f, child_id)
                    child_node = parse_node_block(child_block)
                    child_node['parent_id'] = right_id
                    updated_child = create_node_block(
                        child_id, right_id,
                        child_node['keys'][:child_node['num_keys']],
                        child_node['values'][:child_node['num_keys']],
                        child_node['children'],
                        child_node['num_keys']
                    )
                    write_block(f, child_id, updated_child)

            if len(path) <= 1:
                update_header_local(root_id)
                # print(f"Root was split. New root created with key {promoted_key}.")
                return

            # print(f"Node {node['block_id']} split. Promoting key {promoted_key} to parent {parent_id}.")

            path.pop()
            parent_id = path[-1]
            block = read_block(f, parent_id)
            node = parse_node_block(block)

            insert_index = 0
            while insert_index < node['num_keys'] and promoted_key > node['keys'][insert_index]:
                insert_index += 1

            node['keys'].insert(insert_index, promoted_key)
            node['values'].insert(insert_index, promoted_value)
            node['children'][insert_index] = left_id
            node['children'].insert(insert_index + 1, right_id)
            if len(node['children']) > MAX_CHILDREN:
                node['children'] = node['children'][:MAX_CHILDREN]
            node['num_keys'] += 1

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
