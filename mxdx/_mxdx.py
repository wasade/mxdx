import sys
import os
import multiprocessing as mp
from itertools import chain
from functools import lru_cache
from collections import deque, defaultdict
import io
import glob
import re
import time
from multiprocessing.synchronize import SEM_VALUE_MAX

from ._io import IO, MuxFile
from ._constants import (INTERLEAVE, R1ONLY, R2ONLY, SEQUENTIAL,
                         READ_COMPLETE, R1, R2, MERGE, SEQUENTIAL,
                         SEPARATE, PARTIAL, PATH, DATA, ERROR, COMPLETE)


class Multiplex:
    """Multiplex records from a file batch."""

    def __init__(self, file_map, batch, paired_handling, output):
        self._file_map = file_map
        self._batch = batch
        self._mxfiles = file_map.batch(batch)
        self._paired_handling = paired_handling
        self._output = output

        open_f, read_f, write_f = IO.io_from_mx(self._mxfiles[0])
        self._open_f = open_f
        self._read_f = read_f
        self._write_f = write_f

        if not file_map.is_paired:
            if self._paired_handling in (R2ONLY, INTERLEAVE):
                raise ValueError("Data are not paired")

        if self._paired_handling == INTERLEAVE:
            if self._read_f not in IO.valid_interleave():
                raise ValueError("Unable to interleave format")

        self.queue = None

    def _read_interleaved(self, r1_reader, r2_reader):
        for rec1, rec2 in zip(r1_reader, r2_reader):
            yield rec1
            yield rec2

    def _read_sequential(self, r1_reader, r2_reader):
        for rec in chain(r1_reader, r2_reader):
            yield rec

    def read(self):
        """Read requested records, tag them, and emplace in a queue."""
        mode = 'rt'

        read_f = self._read_f
        open_f = self._open_f

        for mxfile in self._mxfiles:
            file1, file2, start, stop, tag, _ = mxfile

            # setup our record readers
            f1_opened = open_f(file1, mode)
            rec1_reader = IO.read(read_f, f1_opened, start, stop, R1)
            if file2 is None:
                rec2_reader = None
            else:
                f2_opened = open_f(file2, mode)
                rec2_reader = IO.read(read_f, f2_opened, start, stop, R2)

            # setup the reading mode relative to paired handling
            if self._paired_handling == INTERLEAVE:
                reader = self._read_interleaved(rec1_reader, rec2_reader)
            elif self._paired_handling == SEQUENTIAL:
                if rec2_reader is None:
                    reader = rec1_reader
                else:
                    reader = self._read_sequential(rec1_reader, rec2_reader)
            elif self._paired_handling == R1ONLY:
                reader = rec1_reader
            elif self._paired_handling == R2ONLY:
                reader = rec2_reader
            else:
                raise ValueError("Unknown paired handling mode.")

            # place our records into the queue
            for rec in reader:
                self.buffered_queue.put(rec.tag(tag))

        # signal that we are done reading
        self._read_complete()

    def write(self):
        """Write records from a queue to an output."""
        if self._output == '-':
            output = sys.stdout
        else:
            # the expected usecase is to output over standard output. however,
            # it is easy to support basic file handling which may be useful
            # for debugging. given the expected usecase is stdout, and the user
            # is free to compress or transform that stream however they want,
            # we are not investing additional development to support other
            # output formats
            output = open(self._output, 'w')

        while True:
            # get a block of records
            recs = self.buffered_queue.get()

            # if we are complete then terminate gracefully
            if recs == READ_COMPLETE:
                break

            # otherwise, write each record
            for rec in recs:
                try:
                    output.write(rec.write())
                except BrokenPipeError:
                    # something bad happened downstream
                    self._terminate()
                    break

        if self._output != '-':
            output.close()

        self._write_complete()

    def _terminate(self):
        self.msg_queue.put(ERROR)

    def _write_complete(self):
        self.msg_queue.put(COMPLETE)

    def _read_complete(self):
        self.buffered_queue.put(READ_COMPLETE)

    def start(self):
        """Start the Multiplexing."""
        ctx = mp.get_context('spawn')
        self.buffered_queue = BufferedQueue(ctx)
        self.msg_queue = ctx.Queue(maxsize=16)

        reader = ctx.Process(target=self.read)
        writer = ctx.Process(target=self.write)

        reader.start()
        writer.start()

        # monitor reader and writer externally and kill if requested
        while True:
            time.sleep(0.1)
            if self.msg_queue.empty():
                continue

            msg = self.msg_queue.get()

            if msg == ERROR:
                print(f"Error received in {self.__class__}; terminating",
                      file=sys.stderr, flush=True)
                reader.terminate()
                writer.terminate()
                break
            elif msg == COMPLETE:
                break
            time.sleep(0.1)

        reader.join()
        writer.join()


class BufferedQueue:
    """Implement a buffered shared queue.

    multiprocessing.Queue has an OS dependent max size, which can be
    unexpectedly small, see https://github.com/python/cpython/issues/119534.
    To adjust for this, we're increasing the amount of data which an
    individual queue item can hold.
    """

    BUFSIZE = 128

    def __init__(self, ctx):
        self._queue = ctx.Queue(maxsize=min(32767, SEM_VALUE_MAX))
        self._buf = None
        self._init_buf()

    def _init_buf(self):
        self._buf = deque(maxlen=self.BUFSIZE)

    def _place_buf(self):
        if len(self._buf) > 0:
            buf = tuple(self._buf)
            self._queue.put(buf)
            self._init_buf()

    def put(self, item):
        # if we receive a thread complete signal, make sure we drain our
        # buffer before passing on the completion message
        if item == READ_COMPLETE:
            self._place_buf()
            self._queue.put(READ_COMPLETE)
        else:
            self._buf.append(item)

            if len(self._buf) == self.BUFSIZE:
                self._place_buf()

    def get(self):
        return self._queue.get()


class Demultiplex:
    def __init__(self, file_map, batch, paired_handling, mux_input,
                 output_base, extension):
        self._file_map = file_map
        self._batch = batch
        self._mxfiles = file_map.batch(batch)
        self._tag_lookup = {mx.tag: mx for mx in self._mxfiles}
        self._valid_tags = frozenset(self._tag_lookup)
        self._paired_handling = paired_handling
        self._output_base = output_base
        self._extension = extension
        self._open_f = IO.opener(self._extension)
        self._mux_input = mux_input

        if not file_map.is_paired:
            if self._paired_handling in (R2ONLY, MERGE):
                raise ValueError("Data are not paired")

        self.queue = None
        self._open_files = {}

    def __del__(self):
        self._close_files()

    def _close_files(self):
        for v in self._open_files.values():
            if not v.closed:
                v.close()

    def start(self):
        """Start the Demultiplexing."""
        ctx = mp.get_context('spawn')
        self.buffered_queue = BufferedQueue(ctx)
        self.msg_queue = ctx.Queue(maxsize=16)

        reader = ctx.Process(target=self.read)
        writer = ctx.Process(target=self.write)

        reader.start()
        writer.start()

        while True:
            time.sleep(0.1)
            if self.msg_queue.empty():
                continue

            msg = self.msg_queue.get()

            if msg == ERROR:
                print(f"Error received in {self.__class__}; terminating",
                      file=sys.stderr, flush=True)
                reader.terminate()
                writer.terminate()
                break
            elif msg == COMPLETE:
                break

        reader.join()
        writer.join()

    def read(self):
        """Read from the input stream and queue."""
        # we can't pickle the streams so this has to be thread local
        if self._mux_input == '-':
            # see https://docs.python.org/3/library/multiprocessing.html#programming-guidelines
            # stdin is closed to avoid mangling, so we explicitly open it again
            mux_input = open(0, 'rt')
        elif isinstance(self._mux_input, io.StringIO):
            mux_input = self._mux_input
        else:
            mux_input = open(self._mux_input, 'rt')

        try:
            sniffed, read_f, _ = IO.io_from_stream(mux_input)
        except StopIteration:
            # stream is empty, upstream program likely bailed
            self._terminate()
            return

        mux_input = sniffed

        for rec in read_f(mux_input):
            self.buffered_queue.put(rec)

        self._read_complete()

    def _terminate(self):
        self.msg_queue.put(ERROR)

    def _read_complete(self):
        self.buffered_queue.put(READ_COMPLETE)

    def _write_complete(self):
        self.msg_queue.put(COMPLETE)

    @lru_cache()
    def _get_output_path(self, mx, orientation):
        if mx.complete:
            prefix = ''
        else:
            prefix = f'{PARTIAL}.{mx.tag}.'

        if self._paired_handling == MERGE:
            base = os.path.basename(mx.file1)
        elif self._paired_handling == SEPARATE:
            if orientation in (R1, None):
                base = os.path.basename(mx.file1)
            elif orientation == R2:
                base = os.path.basename(mx.file2)
            else:
                raise ValueError("Unknown orientation")
        else:
            raise ValueError("Unsupported pairing mode")

        return f"{self._output_base}/{prefix}{base}.{self._extension}"

    def _get_opened_file(self, path, mode='wt'):
        if path not in self._open_files:
            self._open_files[path] = self._open_f(path, mode)

        return self._open_files[path]

    def _write_rec(self, mx, rec):
        out_path = self._get_output_path(mx, rec.get_orientation())
        out_f = self._get_opened_file(out_path)
        out_f.write(rec.write())

    def write(self):
        """Write to the respective outputs."""
        default = MuxFile("badtag.r1", "badtag.r2", 0, sys.maxsize, "badtag",
                          False)

        while True:
            # get a block of records
            recs = self.buffered_queue.get()

            # if we are complete then terminate gracefully
            if recs == READ_COMPLETE:
                break

            # otherwise, write each record
            for rec in recs:
                tag, rec = rec.detag(self._valid_tags)
                mx = self._tag_lookup.get(tag, default)
                self._write_rec(mx, rec)

        self._write_complete()


class Consolidate:
    BUFSIZE = 1024 * 1024  # 1MB

    def __init__(self, output_base, extension):
        self._output_base = output_base
        self._extension = extension
        self._open_f = IO.opener(self._extension)
        self._groups = []
        self._init()

    def _tag_from_file(self, fp):
        tag_re = re.compile(r"^(" + PARTIAL + r"\.\d+\.\S{3}\.\d+)")
        search = tag_re.search(fp)
        if search is None:
            raise ValueError(f"Could not extract tag from: {fp}")
        tag = search.groups()[0]
        return tag, fp[len(tag) + 1:]

    def _init(self):
        files = glob.glob(f"{self._output_base}/*{self._extension}")
        current_files = [os.path.basename(f) for f in files]

        partials = defaultdict(list)
        for fp in current_files:
            if fp.startswith(PARTIAL):
                tag, untagged = self._tag_from_file(fp)
                if os.path.exists(f"{self._output_base}/{untagged}"):
                    raise IOError(f"Non-partial '{untagged}' unexpectedly exists")

                # by using the untagged name, we implicitly resolve paired handling
                # as these names will include R1/R2
                partials[untagged].append(f"{self._output_base}/{fp}")

        self._groups = [(k, sorted(v)) for k, v in partials.items()]

    def _bulk_read(self, fp):
        block = fp.read(self.BUFSIZE)
        while block != b"":
            yield block
            block = fp.read(self.BUFSIZE)

    def read(self):
        for out_f, files_to_read in self._groups:
            out_path = f"{self._output_base}/{out_f}"

            self.queue.put((PATH, out_path))
            for fp in files_to_read:
                for block in self._bulk_read(self._open_f(fp, 'rb')):
                    self.queue.put((DATA, block))

        self.queue.put(READ_COMPLETE)

    def write(self):
        current_handle = None
        while True:
            # get a block of records
            msg = self.queue.get()

            # if we are complete then terminate gracefully
            if msg == READ_COMPLETE:
                break
            else:
                dtype, data = msg

            if dtype == PATH:
                if current_handle is not None:
                    current_handle.close()
                current_handle = self._open_f(data, 'wb')
            elif dtype == DATA:
                current_handle.write(data)
            else:
                raise ValueError(f"Unrecognized dtype: {dtype}")

        if current_handle is not None:
            current_handle.close()

    def _work_to_do(self):
        return len(self._groups) > 0

    def start(self):
        """Start the Consolidating."""
        if not self._work_to_do():
            return

        ctx = mp.get_context('spawn')
        self.queue = ctx.Queue(maxsize=128)

        reader = ctx.Process(target=self.read)
        writer = ctx.Process(target=self.write)

        reader.start()
        writer.start()

        reader.join()
        writer.join()
