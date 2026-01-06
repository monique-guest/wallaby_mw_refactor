import os
from urllib.parse import urlparse

def nonempty_file_exists(filepath):
    return os.path.exists(filepath) and os.path.getsize(filepath) > 0

def filename_from_url(url):
    return os.path.basename(urlparse(url).path)