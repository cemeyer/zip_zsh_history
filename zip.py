#!/usr/bin/env python3

import hashlib
import io
import os
import re
import sys

# py-zstandard, not py-zstd.
import zstd
assert zstd.ZstdDecompressor

def myhash(obj):
    if obj is None:
        return None

    if getattr(obj, '__hash__', None) is None:
        raise ValueError("not hashable type")

    try:
        # Python 3.6+
        hiter = hashlib.blake2b()
    except AttributeError as e:
        hiter = hashlib.sha512()

    # Not particularly elegant but AFAIK captures all the necessary
    # bytes for uniqueness of ("a", "b", "c") tuples.
    hiter.update(repr(obj).encode("utf-8"))
    return hiter.digest()[:32]

# Re-opens the file-like stream of string 'attr' in parent object 'pobj' with
# the open() builtin parameters 'args' and 'kwargs'.  Unix-specific.
def set_stream_errorh(pobj, attr, *args, **kwargs):
    obj = getattr(pobj, attr)
    fileno = obj.fileno()

    # Keep the OS file handle alive by dup'ing the fd while we close the Python
    # stream.
    fd = os.dup(fileno)
    assert fd >= 0

    obj.close()
    obj = None
    setattr(pobj, attr, None)

    # dup2 it back into its previous number and reopen the stream with the
    # error handling we wanted.
    os.dup2(fd, fileno)
    os.close(fd)

    setattr(pobj, attr, os.fdopen(fileno, *args, **kwargs))

zh_line = r'^: (?P<datetime>\d+):(?P<exetime>\d+);(?P<contents>.*)$'
zh_line_re = re.compile(zh_line)

# Attempts to detect and open compressed files in a read-only streaming
# fashion, such as Zstandard-compressed.  If that fails, falls back to ordinary
# uncompressed access.
def filereader(filename):
    bf = open(filename, "rb")
    try:
        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(bf)

        # Force detection of file magic header, or fail (raise ZstdError).
        # Without this, py-zstandard only lazily opens the stream and does not
        # verify the magic header.
        reader.read(1)

        # Rewind.  Py-Zstd doesn't support trivial rewind-to-beginning, so
        # emulate it.  Yes, closing the zstd streamer doesn't seem to close the
        # underlying stream (bf).
        reader.close()
        bf.seek(0, os.SEEK_SET)
        reader = dctx.stream_reader(bf)

        return io.TextIOWrapper(reader, encoding="utf-8",
                errors="surrogateescape")
    except zstd.ZstdError:
        bf.close()
        bf = None

    return open(filename, "r", encoding="utf-8", errors="surrogateescape")

def linereader(filename):
    continuation = False
    lno = 0
    with filereader(filename) as f:
        for line in f:
            lno += 1
            line = line.rstrip("\n")

            if not continuation:
                m = zh_line_re.match(line)
                assert m, "non-match on '%s' line %d" % (filename, lno)

                datetime, exetime, contents = \
                    m.group('datetime', 'exetime', 'contents')
                contents = contents.rstrip("\n")

                if contents.endswith('\\'):
                    continuation = True
                else:
                    yield (datetime, exetime, contents)

            # Continuation
            else:
                contents += "\n" + line
                if not line.endswith('\\'):
                    continuation = False
                    yield (datetime, exetime, contents)

    assert not continuation, "'%s' ended in incomplete continuation" % filename

# Assumes pre-zipped input; just formats output.
def linewriter(fstream, gen):
    for d, e, c in gen:
        fstream.write(": %s:%s;%s\n" % (d, e, c))

# Consume items from sequence 'rdr', stopping when StopIteration yields None,
# or we find a unique object not already present in 'dups.'  The myhash
# function is used to determine uniqueness.  The intended objects are
# tuples of strings.
#
# If a unique object is found, its hash is stored in 'dups' and the object
# itself stored in 'nexts'['idx'].  (If StopIteration is found, None is stored
# in 'nexts'['idx'].)
def dedupenext(nexts, idx, rdr, dups):
    x = next(rdr, None)
    h = myhash(x)
    if x is None:
        nexts[idx] = x
        return

    while h in dups:
        x = next(rdr, None)
        h = myhash(x)
        if x is None:
            break

    if x is not None:
        dups.add(h)
    nexts[idx] = x

def zipreaders(readers):
    nexts = [None] * len(readers)
    dups = set()

    for i, r in enumerate(readers):
        dedupenext(nexts, i, r, dups)

    while True:
        # No input remaining?
        if len(list(filter(lambda x: x is not None, nexts))) == 0:
            break

        lowest = float("inf")
        idx = None
        for i, r in enumerate(nexts):
            if r is None:
                continue

            datetime, etime, contents = r
            if int(datetime) < lowest:
                lowest = int(datetime)
                idx = i

        yield nexts[idx]
        dedupenext(nexts, idx, readers[idx], dups)

def main():
    sys.argv.pop(0)
    if len(sys.argv) < 2:
        print("Usage: zip.py zhistory1 zhistory2 [zhistoryN...]")
        print("")
        print("  Zips 2 or more zsh history files together, ordered by timestamp.")
        print("  Duplicate lines are only printed once.")
        exit(1)

    # Stupid hoops because Python stream error-handling is immutable after
    # creation.
    set_stream_errorh(sys, "stdout", "w", encoding="utf-8",
        errors="surrogateescape")
    set_stream_errorh(sys, "stderr", "w", encoding="utf-8",
        errors="surrogateescape")

    readers = [linereader(x) for x in sys.argv]
    zipped = zipreaders(readers)
    linewriter(sys.stdout, zipped)

if __name__ == "__main__":
    main()
