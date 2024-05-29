import unittest
import io
import os

from mxdx._io import (FileMap, MuxFile, IO, ParseError, FastaRecord,
                      FastqRecord, SamRecord)


def _serialize(data):
    return io.StringIO('\n'.join(['\t'.join(v) for v in data]) + '\n')


class FileMapTests(unittest.TestCase):
    def setUp(self):
        fm_unpaired = [["filename_1", "record_count"],
                       ["foo", "100"],
                       ["bar", "200"],
                       ["baz", "1000"],
                       ["bing", "10"]]
        fm_paired = [["filename_1", "filename_2", "record_count"],
                     ["foo", "foo2", "100"],
                     ["bar", "bar2", "200"],
                     ["baz", "baz2", "1000"],
                     ["bing", "bing2", "10"]]
        self.fm_unpaired = _serialize(fm_unpaired)
        self.fm_paired = _serialize(fm_paired)

    def test_number_of_batches_bs1(self):
        obs = FileMap.from_tsv(self.fm_unpaired, 1).number_of_batches
        exp = 1310
        self.assertEqual(obs, exp)

    def test_number_of_batches_bs100(self):
        obs = FileMap.from_tsv(self.fm_unpaired, 100).number_of_batches
        exp = 14
        self.assertEqual(obs, exp)

    def test_filemap_from_tsv_construction_unpaired(self):
        obs = FileMap.from_tsv(self.fm_unpaired, 1)
        self.assertFalse(obs.is_paired)
        self.assertEqual(obs.cumsum, [0, 100, 300, 1300])

        with self.assertRaises(IndexError):
            obs.batch(-1)

        self.assertEqual(obs._df[obs._filename_2].null_count(),
                         len(obs._df))

    def test_filemap_from_tsv_construction_paired(self):
        obs = FileMap.from_tsv(self.fm_paired, 1)
        self.assertTrue(obs.is_paired)
        self.assertEqual(obs.cumsum, [0, 100, 300, 1300])

        with self.assertRaises(IndexError):
            obs.batch(-1)

        self.assertEqual(obs._df[obs._filename_2].null_count(), 0)

    def test_filemap_from_tsv_unpaired(self):
        obs = FileMap.from_tsv(self.fm_unpaired, 1)

        # we intentionally manipulate the private batch size variable
        # so we can test for expected behaviors with different batch
        # sizes and batch numbers
        obs._batch_size = 100
        self.assertEqual(obs.batch(0),
                         (MuxFile("foo", None, 0, 100, '1.acb.0', True), ))

        obs._batch_size = 101
        self.assertEqual(obs.batch(0),
                         (MuxFile("foo", None, 0, 100, '1.acb.0', True),
                          MuxFile("bar", None, 0, 1, '2.37b.0', False)))

        obs._batch_size = 100
        self.assertEqual(obs.batch(1),
                         (MuxFile("bar", None, 0, 100, '2.37b.1', False), ))

        obs._batch_size = 151
        self.assertEqual(obs.batch(1),
                         (MuxFile("bar", None, 51, 200, '2.37b.1', False),
                          MuxFile("baz", None, 0, 2, '3.73f.1', False)))

        obs._batch_size = 500
        self.assertEqual(obs.batch(0),
                         (MuxFile("foo", None, 0, 100, '1.acb.0', True),
                          MuxFile("bar", None, 0, 200, '2.37b.0', True),
                          MuxFile("baz", None, 0, 200, '3.73f.0', False)))
        self.assertEqual(obs.batch(1),
                         (MuxFile("baz", None, 200, 700, '3.73f.1', False), ))
        self.assertEqual(obs.batch(2),
                         (MuxFile("baz", None, 700, 1000, '3.73f.2', False),
                          MuxFile("bing", None, 0, 10, '4.738.2', True), ))
        self.assertEqual(obs.batch(3), tuple())

    def test_parse_file_map_paired(self):
        obs = FileMap.from_tsv(self.fm_paired, 1)
        # we intentionally manipulate the private batch size variable
        # so we can test for expected behaviors with different batch
        # sizes and batch numbers

        obs._batch_size = 100
        self.assertEqual(obs.batch(0),
                         (MuxFile("foo", "foo2", 0, 100, '1.acb.0', True), ))

        obs._batch_size = 101
        self.assertEqual(obs.batch(0),
                         (MuxFile("foo", "foo2", 0, 100, '1.acb.0', True),
                          MuxFile("bar", "bar2", 0, 1, '2.37b.0', False)))

        obs._batch_size = 100
        self.assertEqual(obs.batch(1),
                         (MuxFile("bar", "bar2", 0, 100, '2.37b.1', False), ))

        obs._batch_size = 151
        self.assertEqual(obs.batch(1),
                         (MuxFile("bar", "bar2", 51, 200, '2.37b.1', False),
                          MuxFile("baz", "baz2", 0, 2, '3.73f.1', False)))

        obs._batch_size = 500
        self.assertEqual(obs.batch(0),
                         (MuxFile("foo", "foo2", 0, 100, '1.acb.0', True),
                          MuxFile("bar", "bar2", 0, 200, '2.37b.0', True),
                          MuxFile("baz", "baz2", 0, 200, '3.73f.0', False)))
        self.assertEqual(obs.batch(1),
                         (MuxFile("baz", "baz2", 200, 700, '3.73f.1', False), ))
        self.assertEqual(obs.batch(2),
                         (MuxFile("baz", "baz2", 700, 1000, '3.73f.2', False),
                          MuxFile("bing", "bing2", 0, 10, '4.738.2', True), ))
        self.assertEqual(obs.batch(3), tuple())

    def test_filemap_check_paths(self):
        exp = ("foo", "bar", "baz", "bing")
        obs = FileMap.from_tsv(self.fm_unpaired, 1).check_paths(raises=False)
        self.assertEqual(obs, exp)

        exp = ("foo", "foo2", "bar", "bar2", "baz", "baz2", "bing", "bing2")
        obs = FileMap.from_tsv(self.fm_paired, 1).check_paths(raises=False)
        self.assertEqual(obs, exp)

        # make valid/invalid paths
        cur_file = __file__
        cur_dir = os.path.dirname(cur_file)
        fm_unpaired = _serialize([["filename_1", "record_count"],
                                  ["foo", "100"],
                                  [cur_file, "200"]])
        fm_paired = _serialize([["filename_1", "filename_2", "record_count"],
                                ["foo", "foo2", "100"],
                                [cur_file, cur_dir + '/../__init__.py', "200"],
                                ["bing", "bing2", "10"]])

        exp = ("foo", )
        obs = FileMap.from_tsv(fm_unpaired, 1).check_paths(raises=False)
        self.assertEqual(obs, exp)

        exp = ("foo", "foo2", "bing", "bing2")
        obs = FileMap.from_tsv(fm_paired, 1).check_paths(raises=False)
        self.assertEqual(obs, exp)

        fm_unpaired.seek(0)
        with self.assertRaises(IOError):
            FileMap.from_tsv(fm_unpaired, 1).check_paths(raises=True)

        fm_paired.seek(0)
        with self.assertRaises(IOError):
            FileMap.from_tsv(fm_paired, 1).check_paths(raises=True)


class RecordTests(unittest.TestCase):
    def test_tag(self):
        data = [FastaRecord('foo', 'atgc\n'),
                FastqRecord('bar', 'gg\n+\n##\n'),
                SamRecord('baz', 'blah\tblah\t\n')]
        exp = [FastaRecord('mark_foo', 'atgc\n'),
               FastqRecord('mark_bar', 'gg\n+\n##\n'),
               SamRecord('mark_baz', 'blah\tblah\t\n')]

        tag = 'mark'
        obs = [d.tag(tag) for d in data]
        self.assertEqual(obs, exp)

    def test_remove_tag(self):
        data = [FastaRecord('mark1_foo', 'atgc\n'),
                FastqRecord('mark2_bar', 'gg\n+\n##\n'),
                SamRecord('mark1_baz', 'blah\tblah\t\n')]
        exp = [('mark1', FastaRecord('foo', 'atgc\n')),
               ('mark2', FastqRecord('bar', 'gg\n+\n##\n')),
               ('mark1', SamRecord('baz', 'blah\tblah\t\n'))]

        class Any:
            def __contains__(self, other):
                return True

        valid_tags = Any()
        obs = [d.detag(valid_tags) for d in data]
        self.assertEqual(obs, exp)

        valid_tags = {'mark2', }
        data = [FastaRecord('mark1_foo', 'atgc\n'),
                FastqRecord('mark2_bar', 'gg\n+\n##\n'),
                SamRecord('mark1_baz', 'blah\tblah\t\n')]
        with self.assertRaises(ParseError):
            [d.detag(valid_tags) for d in data]


class IOTests(unittest.TestCase):
    def test_io_from_stream(self):
        data = '\n'.join([">1", "aatt", ">2", "aa", ">3", "tt", ">4", "gg",
                          ">5", "cc", ">6", "gc", ""])
        stream = io.StringIO(data)
        sniffed, r_f, w_f = IO.io_from_stream(stream, n_lines=4)
        self.assertEqual(sniffed.read(), data)
        self.assertEqual(r_f, IO.read_fasta)
        self.assertEqual(w_f, IO.write_fasta)

    def test_io_from_mx(self):
        cwd = os.path.dirname(__file__)
        mx = MuxFile(f"{cwd}/test_data/foo_r1.fasta", None, 0, 12, 'blah',
                     True)
        o_f, r_f, w_f = IO.io_from_mx(mx)
        self.assertEqual(o_f, open)
        self.assertEqual(r_f, IO.read_fasta)
        self.assertEqual(w_f, IO.write_fasta)

    def test_read(self):
        data = '\n'.join([">1", "aatt", ">2", "aa", ">3", "tt", ">4", "gg",
                          ">5", "cc", ">6", "gc", ""])
        data = io.StringIO(data)

        exp = [FastaRecord("3", "tt\n"),
               FastaRecord("4", "gg\n"),
               FastaRecord("5", "cc\n")]
        obs = list(IO.read(IO.read_fasta, data, 2, 5, None))
        self.assertEqual(obs, exp)

        data.seek(0)
        with self.assertRaises(ParseError):
            list(IO.read(IO.read_fasta, data, 2, 10, None))

    def test_sniff(self):
        tests = [(">foo bar\nATGC\nTTT\n>bar\nTTTT\n", IO.read_fasta),
                 ("@foo\nATGC\n+\n####\n@bar\nTTTT\n+\n####\n", IO.read_fastq),
                 (("HWI-ST208:453:C1T26ACXX:2:1108:8119:36567/1\t16\t"
                   "G010669145\t5212917\t"), IO.read_sam)]

        for data, exp in tests:
            obs_read, _ = IO.sniff(data)
            self.assertEqual(obs_read, exp)

        tests = ["blah", "foo\tbar\tbaz\tstuff\tcoo\t"]
        for data in tests:
            obs = IO.sniff(data)
            self.assertEqual(obs, (None, None))


    def test_read_fasta(self):
        # note comments are lost but comments are not ids
        data = '\n'.join([">foo bar", "atgc", ">baz", "gg", ""])
        exp = [FastaRecord('foo', 'atgc\n'),
               FastaRecord('baz', 'gg\n')]
        obs = list(IO.read_fasta(io.StringIO(data)))
        self.assertEqual(obs, exp)

    def test_read_fastq(self):
        # note comments are lost but comments are not ids
        data = '\n'.join(["@foo bar", "atgc", "+", "####",
                          "@baz", "ttgg", "+", "@@@@", ""])
        exp = [FastqRecord('foo', 'atgc\n+\n####\n'),
               FastqRecord('baz', 'ttgg\n+\n@@@@\n')]
        obs = list(IO.read_fastq(io.StringIO(data)))
        self.assertEqual(obs, exp)

    def test_read_sam(self):
        exp = [SamRecord("HWI-ST208:453:C1T26ACXX:2:1108:8119:36567/1",
                      "16	G010669145	5212917	35	51M	*	0	0	CGATCGATCTCCTCGACCTCCTGACTCTACTGCCAGAAGAATAGATAAGGA	EHIJIHGIJJIGGIGEHGIGIHFHJGJJHJIGDJJJIJGHHHHFFFFD@@B	AS:i:0	XS:i:-1	XN:i:0	XM:i:0	XO:i:0	XG:i:0	NM:i:0	MD:Z:51	YT:Z:UU\n"),  # noqa
               SamRecord("HWI-ST208:453:C1T26ACXX:2:1108:8119:36567/1",
                      "272	G005938105	3202055	255	51M	*	0	0	CGATCGATCTCCTCGACCTCCTGACTCTACTGCCAGAAGAATAGATAAGGA	EHIJIHGIJJIGGIGEHGIGIHFHJGJJHJIGDJJJIJGHHHHFFFFD@@B	AS:i:-1	XS:i:-1	XN:i:0	XM:i:1	XO:i:0	XG:i:0	NM:i:1	MD:Z:27G23	YT:Z:UU\n"),  # noqa
               SamRecord("HWI-ST208:453:C1T26ACXX:2:1108:7496:49397/1",
                      "16	G002897235	14444859	0	51M	*	0	0	AAGGGACTCTCAAGAGTCTTCTCCAACACCATAGTTCAAAAGCATCAATTC	GJJJJJIIGHGJJJJJIIIIIHIJJJIHIGH>JIIJJJHHHHHFDFFFC@@	AS:i:-1	XS:i:-1	XN:i:0	XM:i:1	XO:i:0	XG:i:0	NM:i:1	MD:Z:31C19	YT:Z:UU\n")]  # noqa
        obs = list(IO.read_sam(io.StringIO(example_sam)))
        self.assertEqual(obs, exp)


example_sam = """HWI-ST208:453:C1T26ACXX:2:1108:8119:36567/1	16	G010669145	5212917	35	51M	*	0	0	CGATCGATCTCCTCGACCTCCTGACTCTACTGCCAGAAGAATAGATAAGGA	EHIJIHGIJJIGGIGEHGIGIHFHJGJJHJIGDJJJIJGHHHHFFFFD@@B	AS:i:0	XS:i:-1	XN:i:0	XM:i:0	XO:i:0	XG:i:0	NM:i:0	MD:Z:51	YT:Z:UU
HWI-ST208:453:C1T26ACXX:2:1108:8119:36567/1	272	G005938105	3202055	255	51M	*	0	0	CGATCGATCTCCTCGACCTCCTGACTCTACTGCCAGAAGAATAGATAAGGA	EHIJIHGIJJIGGIGEHGIGIHFHJGJJHJIGDJJJIJGHHHHFFFFD@@B	AS:i:-1	XS:i:-1	XN:i:0	XM:i:1	XO:i:0	XG:i:0	NM:i:1	MD:Z:27G23	YT:Z:UU
HWI-ST208:453:C1T26ACXX:2:1108:7496:49397/1	16	G002897235	14444859	0	51M	*	0	0	AAGGGACTCTCAAGAGTCTTCTCCAACACCATAGTTCAAAAGCATCAATTC	GJJJJJIIGHGJJJJJIIIIIHIJJJIHIGH>JIIJJJHHHHHFDFFFC@@	AS:i:-1	XS:i:-1	XN:i:0	XM:i:1	XO:i:0	XG:i:0	NM:i:1	MD:Z:31C19	YT:Z:UU
"""  # noqa

if __name__ == '__main__':
    unittest.main()
