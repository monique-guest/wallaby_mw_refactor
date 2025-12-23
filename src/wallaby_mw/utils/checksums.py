import hashlib

def md5sum(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def read_checksum_file(checksum_path):
    # Expects "<md5> <filename>"
    with open(checksum_path, "r") as f:
        return f.read().split()[0].strip()