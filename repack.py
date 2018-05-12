#! /usr/bin/python3

import getopt, sys
import re
import struct
import itertools
import zipfile as zf

def usage():
     print("Usage:\n{0} [-f <filename]".format(sys.argv[0]))

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hf:vmr', ["help", "file=", "verbose", "remap", "raw"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    filename = None
    verbose = False
    remap = False
    reverse = False
    raw = False
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-f", "--file"):
            filename = a
        elif o in ("-m", "--remap"):
            remap = True
        elif o in ("-r", "--raw"):
            raw = True
        else:
            assert False, "unhandled option"

    if not filename:
        usage()
        sys.exit(1)

    with zf.ZipFile(filename) as myzip:
        if verbose:
            for entry in myzip.infolist():
                print(entry)

        segments = {}
        cf = None
        unitsize = None
        probes = {}
        metadata = None
        meta_dict = {}
        versiondata = None
        sizes = { "old" : 0 }

        with myzip.open('metadata') as meta:
            section = ""
            metadata = meta.read()
            s = metadata.decode()
            if verbose:
                print("metadata:\n---\n{0}---\n".format(s))
            for l in s.splitlines():
                if not len(l):
                    continue
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
                ml = re.match(r'(.*)=(.*)', l)
                ms = re.match(r'(\[.*\])', l)
                if not ms and not ml:
                    print("Unmatched: '{0}'".format(l))
                elif ml:
                    meta_dict.setdefault(section, {})
                    meta_dict[section][ml.group(1)] = ml.group(2)
                else:
                    section = ms.group(1)

        with myzip.open('version') as version:
            versiondata = version.read()
            versiondata = int(versiondata)
            if verbose:
                print("version:\n---\n{0}\n---\n".format(versiondata))
            if versiondata == 3:
                reverse = True


        assert max(probes.keys()) <= unitsize * 8, \
            "Last probe={0}, unitsize={1}".format(max(probes.keys()), unitsize)

        print("Active probes: {0}".format(len(probes)))
        if remap:
            print(repr(meta_dict["[device 1]"]))
            # modify metadata
            d1 = meta_dict["[device 1]"]
            meta_dict["[device 1]"] = {k:v for (k,v) in d1.items() if not k.startswith("probe")}

            unitsize = 1 if len(probes) <= 8 else 2
            meta_dict["[device 1]"]["unitsize"] = "{0}".format(unitsize)
            channelmap = []
            channelmap_none = []
            for i in range(1, 1 + unitsize * 8):
                if i in probes:
                    channelmap.append(i)
                else:
                    channelmap_none.append(i)
            for c in channelmap:
                probe = "probe{0}".format(c)
                meta_dict["[device 1]"][probe] = probes[c]
            channelmap += channelmap_none

        else:
            channelmap = list(range(1, 1 + unitsize * 8))

        metadata = []
        for section in ["[global]", "[device 1]"]:
            metadata.append(section)
            for key, val in meta_dict[section].items():
                metadata.append("{0}={1}".format(key, val))
            metadata.append("")
        metadata = "\n".join(metadata).encode()


        for entry in myzip.infolist():
            sizes["old"] += entry.compress_size
            if entry.filename.startswith(cf):
                #verbose and print("Capture segment: {0}".format(entry.filename))
                stream = 1
                if reverse == True:
                    pattern = cf + "-(\d+)-(\d+)$"
                    m = re.match(pattern, entry.filename)
                    assert m, "could not parse segment/stream number"
                    segment = int(m[1])
                    stream = int(m[2])
                else:
                    pattern = cf + "-(\d+)$"
                    m = re.match(pattern, entry.filename)
                    assert m, "could not parse segment number"
                    segment = int(m[1])
                verbose and print("Capture stream/segment [{0}][{1}]: {2}".format(stream, segment, repr(entry)))

                mapped_stream = channelmap[stream - 1]
                verbose and print("Mapping stream {0} to {1}".format(stream, mapped_stream))
                segments.setdefault(segment, {})
                segments[segment][mapped_stream] = entry.filename


        if raw == True:
            compressions = {}
            if (reverse == True):
                outfiles = [ filename ]
            else:
                outfiles = [ "{0}-{1}".format(filename, c) for c in probes.keys() ]
            for of in outfiles:
                with open("{0}.raw".format(of), mode='bw') as outraw:
                    verbose and print("Generating raw file {0}.raw".format(of))
        elif reverse == True:
            compressions = { zf.ZIP_DEFLATED : "zip" }
            suffix = ""
        else:
            compressions = { zf.ZIP_DEFLATED : "zip", zf.ZIP_BZIP2 : "bz2", zf.ZIP_LZMA : "lzma" }
            suffix = "2"

        for c in compressions:
            with zf.ZipFile("{0}.{1}.sr{2}".format(filename, compressions[c], suffix), mode='w') as outzip:
                if reverse == True:
                    outzip.writestr("version", "2", zf.ZIP_STORED)
                    outzip.writestr("metadata", metadata, zf.ZIP_STORED)
                else:
                    outzip.writestr("version", "3", zf.ZIP_STORED)
                    outzip.writestr("metadata", metadata, zf.ZIP_STORED)


        for segment in segments:

            if reverse == True:
                streams = {}
                for stream in segments[segment]:
                    sf = segments[segment][stream]
                    print(repr(sf))
                    with myzip.open(sf) as cfdata:
                        b = cfdata.read()
                        #streams.setdefault(stream)
                        streams[stream] = b

                b = logiczip(streams, unitsize, probes)

                if raw == True:
                    print("Appending raw {0}-{1} to {2}.raw".format(cf, segment, filename))
                    with open("{0}.raw".format(filename), mode='ba') as outraw:
                        l = outraw.write(b);

                else:
                    print("Writing {0}-{1}".format(cf, segment))
                    for c in compressions:
                        with zf.ZipFile("{0}.{1}.sr".format(filename, compressions[c]), mode='a') as outzip:
                            outzip.writestr("{0}-{1}".format(cf, segment), b, c)

            else:
                cf = segments[segment][1]
                streams = None
                with myzip.open(cf) as cfdata:
                    b = cfdata.read()
                    streams = logicunzip(b, unitsize, probes)

                if raw == True:
                    for s in streams:
                        print("Appending Probe {0} [{1}] from {2} to {3}-{4}.raw".format(s, probes[s], cf, filename, s))
                        with open("{0}-{1}.raw".format(filename, s), mode='ba') as outraw:
                            l = outraw.write(streams[s]);
                else:
                    for c in compressions:
                        with zf.ZipFile("{0}.{1}.sr2".format(filename, compressions[c]), mode='a') as outzip:
                            for s in streams:
                                outzip.writestr("{0}-{1}".format(cf, s), streams[s], c)

        for c in compressions:
            sizes[compressions[c]] = 0
            with zf.ZipFile("{0}.{1}.sr{2}".format(filename, compressions[c], suffix), mode='r') as outzip:
                for entry in outzip.infolist():
                    sizes[compressions[c]] += entry.compress_size
                    verbose and print(entry)

        print(sizes)

def logicunzip(data, unitsize, probes):
    def bitshuffle(e, mask):
        return (e[0] & mask) << 0 | (e[1] & mask) << 1 | \
               (e[2] & mask) << 2 | (e[3] & mask) << 3 | \
               (e[4] & mask) << 4 | (e[5] & mask) << 5 | \
               (e[6] & mask) << 6 | (e[7] & mask) << 7;

    assert unitsize <= 2, "Only 16 channels or less supported"
    streams = {}
    for i in range(1, 1 + unitsize * 8):
        if i in probes:
            out = bytearray();
            # print("Extracting probe index {0} [{1}]".format(i, probes[i]))
            print(" {0}".format(i), end='', flush=True)
            full = (len(data) // (8 * unitsize)) * 8 * unitsize
            if unitsize == 1:
                it = struct.iter_unpack('BBBBBBBB', data[:full])
            else:
                it = struct.iter_unpack('HHHHHHHH', data[:full]) # TODO: check endian

            mask = 1 << i - 1
            for e in it:
                w = bitshuffle(e, mask)
                w = w >> i - 1
                assert w <= 255
                out += bytes([w])

            r = len(data) - full
            if r:
                if unitsize == 1:
                    e = struct.unpack('BBBBBBBB', data[full:] + bytes(8 - r))
                else:
                    e = struct.unpack('HHHHHHHH', data[full:] + bytes(16 - r))
                w = bitshuffle(e, mask)
                w = w >> i - 1
                out += bytes([w])

            streams[i] = out
            # print("Samples: {0}:".format(len(out) * 8));
    print("")

    return streams

def logiczip(streams, unitsize, probes):

    def shuffle16(e):
        b = bytearray(16)
        b[0:16:2] = shuffle8(e[0:8])
        b[1:16:2] = shuffle8(e[8:16])
        return b

    def shuffle8(e):
        i64 = struct.unpack("<Q", e)[0]
        out =  (i64 & 0x8040201008040201)        | \
              ((i64 & 0x0080402010080402) <<  7) | \
              ((i64 & 0x0000804020100804) << 14) | \
              ((i64 & 0x0000008040201008) << 21) | \
              ((i64 & 0x0000000080402010) << 28) | \
              ((i64 & 0x0000000000804020) << 35) | \
              ((i64 & 0x0000000000008040) << 42) | \
              ((i64 & 0x0000000000000080) << 49) | \
              ((i64 >>  7) & 0x0080402010080402) | \
              ((i64 >> 14) & 0x0000804020100804) | \
              ((i64 >> 21) & 0x0000008040201008) | \
              ((i64 >> 28) & 0x0000000080402010) | \
              ((i64 >> 35) & 0x0000000000804020) | \
              ((i64 >> 42) & 0x0000000000008040) | \
              ((i64 >> 49) & 0x0000000000000080)
        # print("{0:16x} -> {1:16x}".format(i64, out))
        return struct.pack("<Q", out)

    assert unitsize <= 2, "Only 16 channels or less supported"

    in_it = [None] * unitsize * 8
    last = 0

    for i in range(1, 1 + unitsize * 8):
        if i in probes:
            print("Merging probe index {0} [{1}]".format(i, probes[i]))
            s = streams[i]
            in_it[i - 1] = iter(s)
        else:
            print("Merging dummy for index {0}".format(i))
            in_it[i - 1] = iter(int, 1)

    # interleave 8/16 streams
    interleaved = zip(*in_it)

    out = bytearray()

    # shuffle
    if unitsize == 1:
        shuffle_fn = shuffle8
    else:
        shuffle_fn = shuffle16

    for i in interleaved:
        l = bytes(i)
        b = shuffle_fn(l)
        out += b

    print(len(out))
    print("")

    return out

if __name__ == "__main__":
    main()
