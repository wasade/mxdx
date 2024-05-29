import unittest
import io
import os
import shutil
import hashlib
import multiprocessing as mp
import tempfile
import gzip

from mxdx._mxdx import Multiplex, Demultiplex, Consolidate
from mxdx._io import FileMap
from mxdx._constants import (INTERLEAVE, SEQUENTIAL, R1ONLY, R2ONLY, MERGE,
                             SEPARATE)


def _serialize(data):
    return io.StringIO('\n'.join(['\t'.join(v) for v in data]) + '\n')


cwd = os.path.dirname(__file__)
fm_paired = [["filename_1", "filename_2", "record_count"],
             [f"{cwd}/test_data/foo_r1.fasta",
              f"{cwd}/test_data/foo_r2.fasta", "12"],
             [f"{cwd}/test_data/bar_r1.fasta",
              f"{cwd}/test_data/bar_r2.fasta", "7"]]

class MultiplexTests(unittest.TestCase):
    def setUp(self):
        self.fm_paired = _serialize(fm_paired)

        self.foo_hash = hashlib.md5(fm_paired[1][0].encode('ascii')).hexdigest()[:3]
        self.bar_hash = hashlib.md5(fm_paired[2][0].encode('ascii')).hexdigest()[:3]
        self.clean_up = []

    def tearDown(self):
        for f in self.clean_up:
            os.unlink(f)

    def test_integration_sequential(self):
        foo_hash = self.foo_hash
        bar_hash = self.bar_hash

        exp = '\n'.join([f">1.{foo_hash}.0_a/1", "ATGC",
                         f">1.{foo_hash}.0_b/1", "TTCC",
                         f">1.{foo_hash}.0_c/1", "TTAA",
                         f">1.{foo_hash}.0_d/1", "TTTA",
                         f">1.{foo_hash}.0_e/1", "TTAT",
                         f">1.{foo_hash}.0_f/1", "TTGA",
                         f">1.{foo_hash}.0_g/1", "TTAC",
                         f">1.{foo_hash}.0_h/1", "TTCA",
                         f">1.{foo_hash}.0_i/1", "TTTT",
                         f">1.{foo_hash}.0_j/1", "TTAA",
                         f">1.{foo_hash}.0_k/1", "TTA",
                         f">1.{foo_hash}.0_l/1", "TTC",
                         f">1.{foo_hash}.0_a/2", "ATGCT",
                         f">1.{foo_hash}.0_b/2", "TTCCT",
                         f">1.{foo_hash}.0_c/2", "TTAAT",
                         f">1.{foo_hash}.0_d/2", "TTTAT",
                         f">1.{foo_hash}.0_e/2", "TTATT",
                         f">1.{foo_hash}.0_f/2", "TTGAT",
                         f">1.{foo_hash}.0_g/2", "TTACT",
                         f">1.{foo_hash}.0_h/2", "TTCAT",
                         f">1.{foo_hash}.0_i/2", "TTTTT",
                         f">1.{foo_hash}.0_j/2", "TTAAT",
                         f">1.{foo_hash}.0_k/2", "TTAT",
                         f">1.{foo_hash}.0_l/2", "TTCT",
                         f">2.{bar_hash}.0_aa/1", "ATGC",
                         f">2.{bar_hash}.0_bb/1", "TTCC",
                         f">2.{bar_hash}.0_cc/1", "TTAA",
                         f">2.{bar_hash}.0_aa/2", "AATGC",
                         f">2.{bar_hash}.0_bb/2", "ATTCC",
                         f">2.{bar_hash}.0_cc/2", "ATTAA", ''])

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()
        self.clean_up.append(tmp.name)

        fm = FileMap.from_tsv(self.fm_paired, 15)
        mx = Multiplex(fm, 0, SEQUENTIAL, tmp.name)
        mx.start()

        with open(tmp.name) as data:
            obs = data.read()

        self.assertEqual(obs, exp)

    def test_integration_paired(self):
        foo_hash = self.foo_hash
        bar_hash = self.bar_hash

        exp = '\n'.join([f">1.{foo_hash}.0_a/1", "ATGC",
                         f">1.{foo_hash}.0_a/2", "ATGCT",
                         f">1.{foo_hash}.0_b/1", "TTCC",
                         f">1.{foo_hash}.0_b/2", "TTCCT",
                         f">1.{foo_hash}.0_c/1", "TTAA",
                         f">1.{foo_hash}.0_c/2", "TTAAT",
                         f">1.{foo_hash}.0_d/1", "TTTA",
                         f">1.{foo_hash}.0_d/2", "TTTAT",
                         f">1.{foo_hash}.0_e/1", "TTAT",
                         f">1.{foo_hash}.0_e/2", "TTATT",
                         f">1.{foo_hash}.0_f/1", "TTGA",
                         f">1.{foo_hash}.0_f/2", "TTGAT",
                         f">1.{foo_hash}.0_g/1", "TTAC",
                         f">1.{foo_hash}.0_g/2", "TTACT",
                         f">1.{foo_hash}.0_h/1", "TTCA",
                         f">1.{foo_hash}.0_h/2", "TTCAT",
                         f">1.{foo_hash}.0_i/1", "TTTT",
                         f">1.{foo_hash}.0_i/2", "TTTTT",
                         f">1.{foo_hash}.0_j/1", "TTAA",
                         f">1.{foo_hash}.0_j/2", "TTAAT",
                         f">1.{foo_hash}.0_k/1", "TTA",
                         f">1.{foo_hash}.0_k/2", "TTAT",
                         f">1.{foo_hash}.0_l/1", "TTC",
                         f">1.{foo_hash}.0_l/2", "TTCT",
                         f">2.{bar_hash}.0_aa/1", "ATGC",
                         f">2.{bar_hash}.0_aa/2", "AATGC",
                         f">2.{bar_hash}.0_bb/1", "TTCC",
                         f">2.{bar_hash}.0_bb/2", "ATTCC",
                         f">2.{bar_hash}.0_cc/1", "TTAA",
                         f">2.{bar_hash}.0_cc/2", "ATTAA", ''])

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()
        self.clean_up.append(tmp.name)

        fm = FileMap.from_tsv(self.fm_paired, 15)
        mx = Multiplex(fm, 0, INTERLEAVE, tmp.name)
        mx.start()

        with open(tmp.name) as data:
            obs = data.read()

        self.assertEqual(obs, exp)

    def test_integration_r1only(self):
        foo_hash = self.foo_hash
        bar_hash = self.bar_hash

        exp = '\n'.join([f">1.{foo_hash}.0_a/1", "ATGC",
                         f">1.{foo_hash}.0_b/1", "TTCC",
                         f">1.{foo_hash}.0_c/1", "TTAA",
                         f">1.{foo_hash}.0_d/1", "TTTA",
                         f">1.{foo_hash}.0_e/1", "TTAT",
                         f">1.{foo_hash}.0_f/1", "TTGA",
                         f">1.{foo_hash}.0_g/1", "TTAC",
                         f">1.{foo_hash}.0_h/1", "TTCA",
                         f">1.{foo_hash}.0_i/1", "TTTT",
                         f">1.{foo_hash}.0_j/1", "TTAA",
                         f">1.{foo_hash}.0_k/1", "TTA",
                         f">1.{foo_hash}.0_l/1", "TTC",
                         f">2.{bar_hash}.0_aa/1", "ATGC",
                         f">2.{bar_hash}.0_bb/1", "TTCC",
                         f">2.{bar_hash}.0_cc/1", "TTAA", ''])

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()
        self.clean_up.append(tmp.name)

        fm = FileMap.from_tsv(self.fm_paired, 15)
        mx = Multiplex(fm, 0, R1ONLY, tmp.name)
        mx.start()

        with open(tmp.name) as data:
            obs = data.read()

        self.assertEqual(obs, exp)

    def test_integration_r2only(self):
        foo_hash = self.foo_hash
        bar_hash = self.bar_hash

        exp = '\n'.join([
                         f">1.{foo_hash}.0_a/2", "ATGCT",
                         f">1.{foo_hash}.0_b/2", "TTCCT",
                         f">1.{foo_hash}.0_c/2", "TTAAT",
                         f">1.{foo_hash}.0_d/2", "TTTAT",
                         f">1.{foo_hash}.0_e/2", "TTATT",
                         f">1.{foo_hash}.0_f/2", "TTGAT",
                         f">1.{foo_hash}.0_g/2", "TTACT",
                         f">1.{foo_hash}.0_h/2", "TTCAT",
                         f">1.{foo_hash}.0_i/2", "TTTTT",
                         f">1.{foo_hash}.0_j/2", "TTAAT",
                         f">1.{foo_hash}.0_k/2", "TTAT",
                         f">1.{foo_hash}.0_l/2", "TTCT",
                         f">2.{bar_hash}.0_aa/2", "AATGC",
                         f">2.{bar_hash}.0_bb/2", "ATTCC",
                         f">2.{bar_hash}.0_cc/2", "ATTAA", ''])

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()
        self.clean_up.append(tmp.name)

        fm = FileMap.from_tsv(self.fm_paired, 15)
        mx = Multiplex(fm, 0, R2ONLY, tmp.name)
        mx.start()

        with open(tmp.name) as data:
            obs = data.read()

        self.assertEqual(obs, exp)


class DemultiplexTests(unittest.TestCase):
    def setUp(self):
        self.fm_paired = _serialize(fm_paired)

        self.foo_hash = hashlib.md5(fm_paired[1][0].encode('ascii')).hexdigest()[:3]
        self.bar_hash = hashlib.md5(fm_paired[2][0].encode('ascii')).hexdigest()[:3]
        try:
            self.clean_up = tempfile.TemporaryDirectory(delete=False)
        except TypeError:
            self.clean_up = tempfile.TemporaryDirectory()

    def tearDown(self):
        shutil.rmtree(self.clean_up.name)

    def test_demultiplex_separate(self):
        foo_hash = self.foo_hash
        bar_hash = self.bar_hash

        mux = '\n'.join([f">1.{foo_hash}.0_a/1", "ATGC",
                         f">1.{foo_hash}.0_a/2", "ATGCT",
                         f">1.{foo_hash}.0_b/1", "TTCC",
                         f">1.{foo_hash}.0_b/2", "TTCCT",
                         f">1.{foo_hash}.0_c/1", "TTAA",
                         f">1.{foo_hash}.0_c/2", "TTAAT",
                         f">1.{foo_hash}.0_d/1", "TTTA",
                         f">1.{foo_hash}.0_d/2", "TTTAT",
                         f">1.{foo_hash}.0_e/1", "TTAT",
                         f">1.{foo_hash}.0_e/2", "TTATT",
                         f">1.{foo_hash}.0_f/1", "TTGA",
                         f">1.{foo_hash}.0_f/2", "TTGAT",
                         f">1.{foo_hash}.0_g/1", "TTAC",
                         f">1.{foo_hash}.0_g/2", "TTACT",
                         f">1.{foo_hash}.0_h/1", "TTCA",
                         f">1.{foo_hash}.0_h/2", "TTCAT",
                         f">1.{foo_hash}.0_i/1", "TTTT",
                         f">1.{foo_hash}.0_i/2", "TTTTT",
                         f">1.{foo_hash}.0_j/1", "TTAA",
                         f">1.{foo_hash}.0_j/2", "TTAAT",
                         f">1.{foo_hash}.0_k/1", "TTA",
                         f">1.{foo_hash}.0_k/2", "TTAT",
                         f">1.{foo_hash}.0_l/1", "TTC",
                         f">1.{foo_hash}.0_l/2", "TTCT",
                         f">2.{bar_hash}.0_aa/1", "ATGC",
                         f">2.{bar_hash}.0_aa/2", "AATGC",
                         f">2.{bar_hash}.0_bb/1", "TTCC",
                         f">2.{bar_hash}.0_bb/2", "ATTCC",
                         f">2.{bar_hash}.0_cc/1", "TTAA",
                         f">2.{bar_hash}.0_cc/2", "ATTAA", ''])
        exp_foo1 = '\n'.join(['>a/1', 'ATGC',
                              '>b/1', 'TTCC',
                              '>c/1', 'TTAA',
                              '>d/1', 'TTTA',
                              '>e/1', 'TTAT',
                              '>f/1', 'TTGA',
                              '>g/1', 'TTAC',
                              '>h/1', 'TTCA',
                              '>i/1', 'TTTT',
                              '>j/1', 'TTAA',
                              '>k/1', 'TTA',
                              '>l/1', 'TTC', ''])
        exp_foo2 = '\n'.join(['>a/2', 'ATGCT',
                              '>b/2', 'TTCCT',
                              '>c/2', 'TTAAT',
                              '>d/2', 'TTTAT',
                              '>e/2', 'TTATT',
                              '>f/2', 'TTGAT',
                              '>g/2', 'TTACT',
                              '>h/2', 'TTCAT',
                              '>i/2', 'TTTTT',
                              '>j/2', 'TTAAT',
                              '>k/2', 'TTAT',
                              '>l/2', 'TTCT', ''])
        exp_bar1 = '\n'.join([">aa/1", "ATGC",
                              ">bb/1", "TTCC",
                              ">cc/1", "TTAA", ''])
        exp_bar2 = '\n'.join([">aa/2", "AATGC",
                              ">bb/2", "ATTCC",
                              ">cc/2", "ATTAA", ''])

        fm = FileMap.from_tsv(self.fm_paired, 15)
        dx = Demultiplex(fm, 0, SEPARATE, io.StringIO(mux),
                         self.clean_up.name, 'fna.gz')
        dx.start()

        obs_foo1 = gzip.open(self.clean_up.name + '/foo_r1.fasta.fna.gz',
                             'rt').read()
        obs_bar1 = gzip.open(self.clean_up.name + \
                f'/dx-partial.2.{bar_hash}.0.bar_r1.fasta.fna.gz',
                             'rt').read()

        self.assertEqual(obs_foo1, exp_foo1)
        self.assertEqual(obs_bar1, exp_bar1)

        obs_foo2 = gzip.open(self.clean_up.name + '/foo_r2.fasta.fna.gz',
                             'rt').read()
        obs_bar2 = gzip.open(self.clean_up.name + \
                f'/dx-partial.2.{bar_hash}.0.bar_r2.fasta.fna.gz',
                             'rt').read()
        self.assertEqual(obs_foo2, exp_foo2)
        self.assertEqual(obs_bar2, exp_bar2)

    def test_demultiplex_merge(self):
        foo_hash = self.foo_hash
        bar_hash = self.bar_hash

        mux = '\n'.join([f">1.{foo_hash}.0_a/1", "ATGC",
                         f">1.{foo_hash}.0_a/2", "ATGCT",
                         f">1.{foo_hash}.0_b/1", "TTCC",
                         f">1.{foo_hash}.0_b/2", "TTCCT",
                         f">1.{foo_hash}.0_c/1", "TTAA",
                         f">1.{foo_hash}.0_c/2", "TTAAT",
                         f">1.{foo_hash}.0_d/1", "TTTA",
                         f">1.{foo_hash}.0_d/2", "TTTAT",
                         f">1.{foo_hash}.0_e/1", "TTAT",
                         f">1.{foo_hash}.0_e/2", "TTATT",
                         f">1.{foo_hash}.0_f/1", "TTGA",
                         f">1.{foo_hash}.0_f/2", "TTGAT",
                         f">1.{foo_hash}.0_g/1", "TTAC",
                         f">1.{foo_hash}.0_g/2", "TTACT",
                         f">1.{foo_hash}.0_h/1", "TTCA",
                         f">1.{foo_hash}.0_h/2", "TTCAT",
                         f">1.{foo_hash}.0_i/1", "TTTT",
                         f">1.{foo_hash}.0_i/2", "TTTTT",
                         f">1.{foo_hash}.0_j/1", "TTAA",
                         f">1.{foo_hash}.0_j/2", "TTAAT",
                         f">1.{foo_hash}.0_k/1", "TTA",
                         f">1.{foo_hash}.0_k/2", "TTAT",
                         f">1.{foo_hash}.0_l/1", "TTC",
                         f">1.{foo_hash}.0_l/2", "TTCT",
                         f">2.{bar_hash}.0_aa/1", "ATGC",
                         f">2.{bar_hash}.0_aa/2", "AATGC",
                         f">2.{bar_hash}.0_bb/1", "TTCC",
                         f">2.{bar_hash}.0_bb/2", "ATTCC",
                         f">2.{bar_hash}.0_cc/1", "TTAA",
                         f">2.{bar_hash}.0_cc/2", "ATTAA", ''])
        exp_foo1 = '\n'.join(['>a/1', 'ATGC',
                              '>a/2', 'ATGCT',
                              '>b/1', 'TTCC',
                              '>b/2', 'TTCCT',
                              '>c/1', 'TTAA',
                              '>c/2', 'TTAAT',
                              '>d/1', 'TTTA',
                              '>d/2', 'TTTAT',
                              '>e/1', 'TTAT',
                              '>e/2', 'TTATT',
                              '>f/1', 'TTGA',
                              '>f/2', 'TTGAT',
                              '>g/1', 'TTAC',
                              '>g/2', 'TTACT',
                              '>h/1', 'TTCA',
                              '>h/2', 'TTCAT',
                              '>i/1', 'TTTT',
                              '>i/2', 'TTTTT',
                              '>j/1', 'TTAA',
                              '>j/2', 'TTAAT',
                              '>k/1', 'TTA',
                              '>k/2', 'TTAT',
                              '>l/1', 'TTC',
                              '>l/2', 'TTCT', ''])
        exp_bar1 = '\n'.join([">aa/1", "ATGC",
                              ">aa/2", "AATGC",
                              ">bb/1", "TTCC",
                              ">bb/2", "ATTCC",
                              ">cc/1", "TTAA",
                              ">cc/2", "ATTAA", ''])

        fm = FileMap.from_tsv(self.fm_paired, 15)
        dx = Demultiplex(fm, 0, MERGE, io.StringIO(mux),
                         self.clean_up.name, 'fna.gz')
        dx.start()

        obs_foo1 = gzip.open(self.clean_up.name + '/foo_r1.fasta.fna.gz',
                             'rt').read()
        obs_bar1 = gzip.open(self.clean_up.name + \
                f'/dx-partial.2.{bar_hash}.0.bar_r1.fasta.fna.gz',
                             'rt').read()
        self.assertEqual(obs_foo1, exp_foo1)
        self.assertEqual(obs_bar1, exp_bar1)

        foo2 = self.clean_up.name + '/foo_r2.fasta.fna.gz'
        bar2 = self.clean_up.name + '/bar_r2.fasta.fna.gz'
        self.assertFalse(os.path.exists(foo2))
        self.assertFalse(os.path.exists(bar2))


class ConsolidateTests(unittest.TestCase):
    def setUp(self):
        self.fm_paired = _serialize(fm_paired)

        self.foo_hash = hashlib.md5(fm_paired[1][0].encode('ascii')).hexdigest()[:3]
        self.bar_hash = hashlib.md5(fm_paired[2][0].encode('ascii')).hexdigest()[:3]
        try:
            self.clean_up = tempfile.TemporaryDirectory(delete=False)
        except TypeError:
            self.clean_up = tempfile.TemporaryDirectory()

    def tearDown(self):
        shutil.rmtree(self.clean_up.name)

    def test_consolidate(self):
        foo_hash = self.foo_hash
        bar_hash = self.bar_hash

        mux_batch_1 = '\n'.join([f">1.{foo_hash}.0_a/1", "ATGC",
                                 f">1.{foo_hash}.0_a/2", "ATGCT",
                                 f">1.{foo_hash}.0_b/1", "TTCC",
                                 f">1.{foo_hash}.0_b/2", "TTCCT",
                                 f">1.{foo_hash}.0_c/1", "TTAA",
                                 f">1.{foo_hash}.0_c/2", "TTAAT",
                                 f">1.{foo_hash}.0_d/1", "TTTA",
                                 f">1.{foo_hash}.0_d/2", "TTTAT",
                                 f">1.{foo_hash}.0_e/1", "TTAT",
                                 f">1.{foo_hash}.0_e/2", "TTATT",
                                 f">1.{foo_hash}.0_f/1", "TTGA",
                                 f">1.{foo_hash}.0_f/2", "TTGAT",
                                 f">1.{foo_hash}.0_g/1", "TTAC",
                                 f">1.{foo_hash}.0_g/2", "TTACT",
                                 f">1.{foo_hash}.0_h/1", "TTCA",
                                 f">1.{foo_hash}.0_h/2", "TTCAT",
                                 f">1.{foo_hash}.0_i/1", "TTTT",
                                 f">1.{foo_hash}.0_i/2", "TTTTT",
                                 f">1.{foo_hash}.0_j/1", "TTAA",
                                 f">1.{foo_hash}.0_j/2", "TTAAT",
                                 f">1.{foo_hash}.0_k/1", "TTA",
                                 f">1.{foo_hash}.0_k/2", "TTAT",
                                 f">1.{foo_hash}.0_l/1", "TTC",
                                 f">1.{foo_hash}.0_l/2", "TTCT",
                                 f">2.{bar_hash}.0_aa/1", "ATGC",
                                 f">2.{bar_hash}.0_aa/2", "AATGC",
                                 f">2.{bar_hash}.0_bb/1", "TTCC",
                                 f">2.{bar_hash}.0_bb/2", "ATTCC",
                                 f">2.{bar_hash}.0_cc/1", "TTAA",
                                 f">2.{bar_hash}.0_cc/2", "ATTAA", ''])

        mux_batch_2 = '\n'.join([f">2.{bar_hash}.1_dd/1", "TTTA",
                                 f">2.{bar_hash}.1_dd/2", "ATTTA",
                                 f">2.{bar_hash}.1_ee/1", "TTAT",
                                 f">2.{bar_hash}.1_ee/2", "ATTAT",
                                 f">2.{bar_hash}.1_ff/1", "TTGA",
                                 f">2.{bar_hash}.1_ff/2", "ATTGA",
                                 f">2.{bar_hash}.1_gg/1", "TTAC",
                                 f">2.{bar_hash}.1_gg/2", "ATTAC", ''])

        fm = FileMap.from_tsv(self.fm_paired, 15)
        dx = Demultiplex(fm, 0, SEPARATE, io.StringIO(mux_batch_1),
                         self.clean_up.name, 'fna.gz')
        dx.start()

        dx = Demultiplex(fm, 1, SEPARATE, io.StringIO(mux_batch_2),
                         self.clean_up.name, 'fna.gz')
        dx.start()

        cx = Consolidate(self.clean_up.name, 'fna.gz')
        cx.start()

        exp = {'foo_r1.fasta.fna.gz',
               'foo_r2.fasta.fna.gz',
               'bar_r1.fasta.fna.gz',
               'bar_r2.fasta.fna.gz',
               f'dx-partial.2.{bar_hash}.0.bar_r1.fasta.fna.gz',
               f'dx-partial.2.{bar_hash}.0.bar_r2.fasta.fna.gz',
               f'dx-partial.2.{bar_hash}.1.bar_r1.fasta.fna.gz',
               f'dx-partial.2.{bar_hash}.1.bar_r2.fasta.fna.gz'}
        obs = set(os.listdir(self.clean_up.name))
        self.assertEqual(obs, exp)

        with gzip.open(self.clean_up.name + '/foo_r1.fasta.fna.gz', 'rt') as f:
            obs_foo_r1 = f.read()
        with gzip.open(self.clean_up.name + '/foo_r2.fasta.fna.gz', 'rt') as f:
            obs_foo_r2 = f.read()
        with gzip.open(self.clean_up.name + '/bar_r1.fasta.fna.gz', 'rt') as f:
            obs_bar_r1 = f.read()
        with gzip.open(self.clean_up.name + '/bar_r2.fasta.fna.gz', 'rt') as f:
            obs_bar_r2 = f.read()

        with open(f"{cwd}/test_data/foo_r1.fasta") as f:
            exp_foo_r1 = f.read()
        with open(f"{cwd}/test_data/foo_r2.fasta") as f:
            exp_foo_r2 = f.read()
        with open(f"{cwd}/test_data/bar_r1.fasta") as f:
            exp_bar_r1 = f.read()
        with open(f"{cwd}/test_data/bar_r2.fasta") as f:
            exp_bar_r2 = f.read()

        self.assertEqual(obs_foo_r1, exp_foo_r1)
        self.assertEqual(obs_foo_r2, exp_foo_r2)
        self.assertEqual(obs_bar_r1, exp_bar_r1)
        self.assertEqual(obs_bar_r2, exp_bar_r2)


if __name__ == '__main__':
    unittest.main()
