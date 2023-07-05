# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import contextlib
import sys
import fsspec
from fsspec.utils import infer_storage_options
from urllib.parse import urlparse

_fs = None

# https://stackoverflow.com/questions/17602878/how-to-handle-both-with-open-and-sys-stdout-nicely
@contextlib.contextmanager
def smart_open(filename=None, mode='w', binary=False, create_parent_dirs=True):
    fh = get_file_handle(filename, mode, binary, create_parent_dirs)

    try:
        yield fh
    finally:
        fh.close()

def get_file_handle(filename, mode='w', binary=False, create_parent_dirs=True):
    global _fs
    if _fs == None:
        target_protocol = infer_storage_options(filename)['protocol']
        # _fs = fsspec.filesystem('simplecache', target_protocol=target_protocol)
        _fs = fsspec.filesystem(target_protocol)
    if create_parent_dirs and filename is not None:
        parsed = urlparse(filename)
        if (parsed.hostname is None):
            # TODO parse relative path
            _fs.mkdirs(parsed.path.rpartition('/')[0], exist_ok=True)
        else: 
            _fs.mkdirs(parsed.hostname + parsed.path.rpartition('/')[0], exist_ok=True)

    full_mode = mode + ('b' if binary else '')
    is_file = filename and filename != '-'
    if is_file:
        fh = _fs.open(filename, mode=full_mode)
    elif filename == '-':
        fd = sys.stdout.fileno() if mode == 'w' else sys.stdin.fileno()
        fh = _fs.open(fd, full_mode)
    else:
        fh = NoopFile()
    return fh

def rm(path, recursive=True):
    global _fs
    _fs.rm(path, recursive)

def close_silently(file_handle):
    if file_handle is None:
        pass
    try:
        file_handle.close()
    except OSError:
        pass

# Fsspec compatible
class NoopFile:
    def __enter__(self):
        pass

    def __exit__(self):
        pass

    def readable(self):
        pass

    def writable(self):
        pass

    def seekable(self):
        pass

    def close(self):
        pass

    def write(self, bytes):
        pass

    def path(self):
        pass

    def fs(self):
        pass
