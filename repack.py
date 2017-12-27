#! /usr/bin/python3

import getopt, sys
import re
import struct
import zipfile as zf

def usage():
     print("Usage:\n{0} [-f <filename]".format(sys.argv[0]))

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hf:v', ["help", "file=", "verbose"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    filename = None
    verbose = False
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-f", "--file"):
            filename = a
        else:
            assert False, "unhandled option"

    if not filename:
        usage()
        sys.exit(1)

    with zf.ZipFile(filename) as myzip:
        if verbose:
            for entry in myzip.infolist():
                print(entry)

        capturefiles = []
        cf = None
        unitsize = None
        probes = {}

        with myzip.open('metadata') as meta:
            d = meta.read()
            s = d.decode()
            if verbose:
                print("metadata:\n---\n{0}---\n".format(s))
            for l in s.splitlines():
                m = re.match(r'capturefile=(.*)', l)
                assert (not cf or not m), "capturefile already set"
                if m:
                    cf = m.group(1)
                m = re.match(r'unitsize=(.*)', l)
                if m:
                    unitsize = int(m.group(1))
                m = re.match(r'probe(.*)=(.*)', l)
                if m:
                    probes[int(m.group(1))] = m.group(2)

        for entry in myzip.infolist():
            if entry.filename.startswith(cf):
                #verbose and print("Capture segent: {0}".format(entry.filename))
                verbose and print("Capture segent: {0}".format(repr(entry)))
                capturefiles.append(entry.filename)

        assert max(probes.keys()) <= unitsize * 8, \
            "Last probe={0}, unitsize={1}".format(max(probes.keys()), unitsize)

        print(capturefiles)
        for cf in capturefiles:
            streams = None
            with myzip.open(cf) as cfdata:
                b = cfdata.read()
                streams = logicunzip(b, unitsize, probes)

            with zf.ZipFile(filename + ".zip.sr2", mode='a') as outzip:
                for s in streams:
                    outzip.writestr("ZIP/{0}-{1}".format(cf, s), streams[s], zf.ZIP_DEFLATED)
                for entry in outzip.infolist():
                    verbose and print(entry)

            with zf.ZipFile(filename + ".bz2.sr2", mode='a') as outzip:
                for s in streams:
                    outzip.writestr("BZ2/{0}-{1}".format(cf, s), streams[s], zf.ZIP_BZIP2)
                for entry in outzip.infolist():
                    verbose and print(entry)

            with zf.ZipFile(filename + ".lzma.sr2", mode='a') as outzip:
                for s in streams:
                    outzip.writestr("LZMA/{0}-{1}".format(cf, s), streams[s], zf.ZIP_LZMA)
                for entry in outzip.infolist():
                    verbose and print(entry)


def logicunzip(data, unitsize, probes):
    assert unitsize <= 2, "Only 16 channels or less supported"
    streams = {}
    for i in range(1, 1 + unitsize * 8):
        if i in probes:
            out = bytearray();
            print("Extracting probe index {0} [{1}]".format(i, probes[i]))
            full = (len(data) // (8 * unitsize)) * 8 * unitsize
            if unitsize == 1:
                it = struct.iter_unpack('BBBBBBBB', data[:full])
            else:
                it = struct.iter_unpack('HHHHHHHH', data[:full]) # TODO: check endian

            mask = 1 << i - 1
            for e in it:
                w = (e[0] & mask) << 7 | (e[1] & mask) << 6 | \
                    (e[2] & mask) << 5 | (e[3] & mask) << 4 | \
                    (e[4] & mask) << 3 | (e[5] & mask) << 2 | \
                    (e[6] & mask) << 1 | (e[7] & mask) << 0;
                w = w >> i - 1;
                assert w <= 255
                out += bytes([w])

            streams[i] = out
            # print("Samples: {0}:".format(len(out) * 8));

    return streams


if __name__ == "__main__":
    main()
