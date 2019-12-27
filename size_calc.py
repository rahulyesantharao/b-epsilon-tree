block_bytes = 4096
block_data_bytes = 4096 - 2 * 4
unit_size = 2 * 4
upsert_size = 4 * 4
num_leaf_pairs = int((block_data_bytes - 4) / unit_size)

epsilon = 0.5
B = num_leaf_pairs
pivot_byte_size = int((B ** epsilon)) * unit_size
num_children = pivot_byte_size // unit_size
buffer_byte_size = block_data_bytes - pivot_byte_size
buffer_size = (buffer_byte_size - 2 * 4) // upsert_size

print(f"B={B}")
print(f"num_leaf_pairs = {num_leaf_pairs}")
print(f"pivot_byte_size={pivot_byte_size}")
print(f"num_children={num_children}")
print(f"buffer_size={buffer_size}")
print(f"B-B^e = {B - B**epsilon}")  # THIS COULD MESS IT UP - the upsert is 2*unit size

# size analysis
flush_threshold = int(buffer_size / num_children)
leaf_threshold = 1 + (num_leaf_pairs + 1) // 2
print(f"flush threshold = {flush_threshold}")
print(f"leaf threshold = {leaf_threshold}")
