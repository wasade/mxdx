import os
import io
import hashlib
from collections import namedtuple
from dataclasses import dataclass
import gzip
import lzma
import bz2
import mimetypes
from math import ceil

import polars as pl

from ._constants import R1, R2

MuxFile = namedtuple("MuxFile", "file1 file2 start stop tag complete")


@dataclass
class _Record:
    id: str
    data: str

    ####
    # it would be pleasant to move read() into the dataclass as well
    # and ease some of the semantics, but that's a refactor for later
    ###
    def write(self):
        raise NotImplementedError()

    def tag(self, tag):
        self.id = f"{tag}_{self.id}"
        return self

    def detag(self, valid_tags):
        tag, original = self.id.split('_', 1)
        if tag not in valid_tags:
            raise ParseError(f"Unexpectedly observed tag: {tag}")
        self.id = original
        return (tag, self)

    def set_orientation(self, orientation):
        if orientation == R1:
            self.set_r1()
        elif orientation == R2:
            self.set_r2()

    def set_r1(self):
        if not self.id.endswith('/1'):
            self.id += '/1'

    def set_r2(self):
        if not self.id.endswith('/2'):
            self.id += '/2'

    def get_orientation(self):
        orientation = self.id[-2:]
        if orientation == '/1':
            return R1
        elif orientation == '/2':
            return R2
        else:
            return None


@dataclass
class FastaRecord(_Record):
    dtype = 'fasta'

    def write(self):
        return f">{self.id}\n{self.data}"


@dataclass
class FastqRecord(_Record):
    dtype = 'fastq'

    def write(self):
        return f"@{self.id}\n{self.data}"


@dataclass
class SamRecord(_Record):
    dtype = 'sam'

    def write(self):
        return f"{self.id}\t{self.data}"


class FileMap:
    _filename_1 = 'filename_1'
    _filename_2 = 'filename_2'
    _record_count = 'record_count'
    _record_cumsum = 'record_cumsum'
    _start = 'start'
    _stop = 'stop'
    _tmp = 'tmp'
    _hash_prefix = 'hash_prefix'
    _row_index = 'row_index'
    _hash_prefix_size = 3

    def __init__(self, df, batch_size):
        self._df = df
        self._batch_size = batch_size

        self._validate()
        self._is_paired = set(df.columns).issuperset({self._filename_1,
                                                      self._filename_2})
        self._init()

    @property
    def is_paired(self):
        return self._is_paired

    @property
    def cumsum(self):
        return list(self._df[self._record_cumsum])

    @property
    def number_of_batches(self):
        return ceil(self._df[self._record_count].sum() / self._batch_size)

    def _init(self):
        df = self._df.with_row_index(name=self._row_index, offset=1)
        df = df.with_columns(pl.col(self._record_count)
                                .shift(1)
                                .cum_sum()
                                .fill_null(0)
                                .alias(self._record_cumsum))

        if not self.is_paired:
            df = df.with_columns(pl.lit(None).alias(self._filename_2))

        self._df = df

    def batch(self, batch_number):
        def hash_prefix(f):
            h = hashlib.md5(f.encode('ascii')).hexdigest()
            return h[:self._hash_prefix_size]

        if batch_number < 0:
             raise IndexError("Batch number must be greater than zero")

        start = batch_number * self._batch_size
        stop = start + self._batch_size
        remaining = self._batch_size

        if start < 0 or stop <= 0:
            raise IndexError("Ranges must be positive")

        if stop <= start:
            raise IndexError("Stop is less than or equal to start")

        # find our starting row
        row_idx = self._df[self._record_cumsum].search_sorted(start, 'left')

        # shift to starting position and grab our rows
        row_idx = max(row_idx - 1, 0)
        rows = self._df[row_idx:]

        # the start position in the first file encountered
        lag = start - rows[0].select(self._record_cumsum).item()

        current_record_count = rows[0].select(self._record_count).item()
        if lag > current_record_count:
            # if our lag exceeds the number of available records
            # then there are none to read
            return tuple()
        elif lag == current_record_count:
            # boundary case, where the prior cumulative sum puts us at
            # the end of the current file, meaning it has been consumed
            row_idx += 1
            rows = self._df[row_idx:]
            lag = 0

        col_order = [self._row_index, self._filename_1, self._filename_2,
                     self._record_count, self._record_cumsum]

        tups = []
        for (ridx, f1, f2, cnt, cs) in rows.select(col_order).iter_rows():
            # adjust our start if needed
            file_start = lag

            # read either all the records or just what's remaining
            file_stop = min(cnt, file_start + remaining)
            is_complete = (file_stop - file_start) == cnt

            hp = hash_prefix(f1)
            tag = f"{ridx}.{hp}.{batch_number}"
            tups.append(MuxFile(file1=f1, file2=f2, start=file_start,
                                stop=file_stop, tag=tag,
                                complete=is_complete))

            # adjust the amount remaining
            remaining -= (file_stop - file_start)
            if remaining < 0:
                raise ValueError("Should not happen")
            elif remaining == 0:
                break
            else:
                lag = 0  # start at the beginning of a new file

        return tuple(tups)

    @classmethod
    def from_tsv(cls, data, batch_size):
        df = pl.read_csv(data, separator='\t', infer_schema_length=0,
                         has_header=True)
        df = df.with_columns(pl.col(cls._record_count).cast(int))
        return cls(df, batch_size)

    def _validate(self):
        self._validate_header()
        self._validate_files()
        self._validate_counts()
        self._validate_batch_size()

    def _validate_batch_size(self):
        if self._batch_size <= 0:
            raise ValueError("Batch size cannot be 0 or less")

    def _validate_files(self):
        if self._df[self._filename_1].null_count() > 0:
            raise ValueError("Null filenames in filename_1 found")

        if self._filename_2 in self._df.columns:
            if self._df[self._filename_2].null_count() > 0:
                raise ValueError("Null filenames in filename_2 found")

    def _validate_counts(self):
        if self._df[self._record_count].min() < 1:
            raise ValueError("Files with fewer than 1 record found")
        if self._df[self._record_count].null_count() > 0:
            raise ValueError("Files with a null record count found")

    def _validate_header(self):
        columns = set(self._df.columns)

        if len(columns) == 2:
            if columns != {self._filename_1, self._record_count}:
                raise ValueError("Header structure is unexpected")
        elif len(columns) == 3:
            if columns != {self._filename_1, self._filename_2,
                           self._record_count}:
                raise ValueError("Header structure is unexpected")
        else:
            raise ValueError("Header structure is unexpected")

    @property
    def first_file(self):
        return self._df[0].select(self._filename_1).item()

    def check_paths(self, raises=True):
        missing = []

        selection = self._df.select([self._filename_1, self._filename_2])
        for fp1, fp2 in selection.iter_rows():
            if not os.path.exists(fp1):
                missing.append(fp1)
            if self.is_paired and not os.path.exists(fp2):
                missing.append(fp2)

        if raises and len(missing) > 0:
            raise IOError(f"At least one path cannot be found, here are at "
                          f"most 5: {missing[:5]}")
        else:
            return tuple(missing)


class SniffError(Exception):
    pass


# differentiate a sniff failure from a parse failure
class ParseError(Exception):
    pass


class IO:
    @classmethod
    def valid_interleave(cls):
        return {cls.read_fasta, cls.read_fastq}

    @staticmethod
    def io_from_stream(stream, n_lines=4):
        """Sniff a datastream which cannot be seeked."""
        if hasattr(stream, 'buffer'):
            peek = stream.buffer.peek(1024)
        elif hasattr(stream, 'peek'):
            peek = stream.peek(1024)
        else:
            peek = stream.read(1024)
            stream.seek(0)

        if isinstance(peek, bytes):
            peek = peek.decode('utf-8')

        buf = io.StringIO(peek)
        read_f, write_f = IO.sniff(buf)

        return stream, read_f, write_f

    @staticmethod
    def io_from_mx(mxfile):
        open_f = IO.opener(mxfile.file1)
        with open_f(mxfile.file1, 'rt') as fp:
            read_f, write_f = IO.sniff(IO.read_n(fp))
        return (open_f, read_f, write_f)


    @staticmethod
    def opener(path):
        _, encoding = mimetypes.guess_type(path)
        if encoding is None:
            # maybe its just flat text...
            return open
        elif encoding == 'gzip':
            return gzip.open
        elif encoding == 'xz':
            return lzma.open
        elif ext == 'bzip2':
            return bz2.open
        else:
            return open

    @staticmethod
    def read_n(fp, n=40):
        return ''.join([fp.readline() for i in range(n)])

    @classmethod
    def sniff(cls, data):
        io_pairs = [(cls.read_fastq, cls.write_fastq),
                    (cls.read_sam, cls.write_sam),
                    (cls.read_fasta, cls.write_fasta)]

        if isinstance(data, str):
            buf = io.StringIO(data)
        else:
            buf = data

        for fn_read, fn_write in io_pairs:
            buf.seek(0)
            if next(fn_read(buf)) is not None:
                return (fn_read, fn_write)

        return None, None

    @staticmethod
    def read(fn, data, start, stop, orient):
        if start < 0:
            raise ValueError("start must be positive")

        count = 0
        record_gen = fn(data)
        while count < start:
            next(record_gen)
            count += 1

        for rec in record_gen:
            rec.set_orientation(orient)
            yield rec
            count += 1
            if count == stop:
                break

        if count < stop:
            raise ParseError("Reader exhausted but expected more records")

    @staticmethod
    def read_fasta(data):
        try:
            for id_, seq, qual in _readfq(data):
                if qual is not None:
                    raise SniffError("Data are fastq")
                data = seq + '\n'
                yield FastaRecord(id=id_, data=data)
        except (SniffError, ParseError):
            yield None

    @staticmethod
    def write_fasta(record):
        return f">{record.id}\n{record.data}"

    @staticmethod
    def read_fastq(data):
        try:
            for id_, seq, qual in _readfq(data):
                if qual is None:
                    raise SniffError("Data are not fastq")
                data = f"{seq}\n+\n{qual}\n"
                yield FastqRecord(id=id_, data=data)
        except (SniffError, ParseError):
            yield None

    @staticmethod
    def write_fastq(record):
        return f"@{record.id}\n{record.data}"

    @staticmethod
    def read_sam(data):
        try:
            for id_, data in _readsam(data):
                if id_ is None:
                    raise SniffError("Data are not headerless sam")
                yield SamRecord(id=id_, data=data)
        except SniffError:
            yield None

    @staticmethod
    def write_sam(record):
        return f"{record.id}\t{record.data}"


# from https://github.com/lh3/readfq/blob/master/readfq.py
# readme states released without a license, acknowledgement is not needed
# but we do so anyway
def _readfq(fp): # this is a generator function
    count = 0
    last = None # this is a buffer keeping the last unprocessed line
    while True: # mimic closure; is it a bad idea?
        if not last: # the first record or a record following a fastq
            for line in fp: # search for the start of the next record
                if line[0] in '>@': # fasta/q header line
                    last = line[:-1] # save this line
                    break
        if not last:
            break
        name, seqs, last = last[1:].partition(" ")[0], [], None
        for line in fp: # read the sequence
            if line[0] in '@+>':
                last = line[:-1]
                break
            seqs.append(line[:-1])
        if not last or last[0] != '+': # this is a fasta record
            yield name, ''.join(seqs), None # yield a fasta record
            count += 1
            if not last:
                break
        else: # this is a fastq record
            seq, leng, seqs = ''.join(seqs), 0, []
            for line in fp: # read the quality
                seqs.append(line[:-1])
                leng += len(line) - 1
                if leng >= len(seq): # have read enough quality
                    last = None
                    yield name, seq, ''.join(seqs) # yield a fastq record
                    count += 1
                    break
            if last: # reach EOF before reading enough quality
                yield name, seq, None # yield a fasta record instead
                count += 1
                break
    if count == 0:
        raise ParseError("Data do not appear to be fasta or fastq")


def _readsam(data):
    first = True
    for line in data:
        try:
            id_, remainder = line.split('\t', 1)
        except ValueError:
            yield None, None
            break

        if first:
            try:
                flag, ref, refstart, _ = remainder.split('\t', 3)
                if not flag.isdigit():
                    raise ParseError()

                if ref.isdigit():
                    raise ParseError()

                if not refstart.isdigit():
                    raise ParseError()
            except:  # noqa E722
                yield None, None
                break
            else:
                first = False

        yield (id_, remainder)
