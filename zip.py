#!/usr/bin/env python3

import os
import re
import sys

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

def linereader(filename):
    continuation = False
    lno = 0
    with open(filename, "r", encoding="utf-8", errors="surrogateescape") as f:
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

def zipreaders(readers):
    nexts = [None] * len(readers)

    for i, r in enumerate(readers):
        nexts[i] = next(r, None)

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
        nexts[idx] = next(readers[idx], None)

def main():
    sys.argv.pop(0)
    if len(sys.argv) < 2:
        print("Usage: zip.py zhistory1 zhistory2 [zhistoryN...]")
        print("")
        print("  Zips 2 or more zsh history files together, ordered by timestamp.")
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
