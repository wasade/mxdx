# ![mxdx](https://raw.githubusercontent.com/wasade/mxdx/main/mxdx_compressed.jpg)

# mxdx
Generalized multiplexing / demultiplexing for FASTQ/FASTA/SAM

`mxdx` allows for multiplexing paired or unpaired data at the sequence level. 
The specific problem `mxdx` solves running the need to run sets of samples
against tools with high start up costs. By multiplexing, the start up cost
(e.g., `bowtie2` with a large database) can be amortized. Multiplexing is 
performed at the per-record basis which balances the cost of processing
an individual work unit, and avoids intermediate (write) IO. 

`mxdx` supports multiplexing and demultiplexing FASTQ/FASTA/SAM data. The user
can control how paired data are emitted:

- interleaved on a per record basis
- sequentially, first R1 then R2 reads are multiplexed
- R1 only where only R1 reads are multiplexed
- R2 only where only R2 reads are multiplexed

Because `mxdx` operates on a per-record basis, it is necessary to know the 
number of total records up front in order to determine which specific records
to pull from a particular file. 

# Installation

We currently test on OSX (x86_64) and Linux (x86_64). Windows passed unit tests
but failed integration test as bash is not installed by default -- this is 
likely an easy fix but not a current need. OSX (M1) failed in testing from an
illegal instruction, which most likely is from a bad wheel in PyPI for a 
dependency.

To install from pypi:

```
$ pip install mxdx
```

To install from github:

```
$ git clone https://github.com/wasade/mxdx.git
$ cd mxdx
$ pip install -e .
```

# Design considerations

IO is handled in separate processes for all actions. Specifically, we read 
and decompress in one process, and write in a separate process. We do not use
threads to avoid the GIL -- while generally, the GIL is fine for IO bound
tasks, some compression schemes like `lzma` are costly. The use of multiple 
processes allows, if necessary in the future, an easier trajectory to read
or write many files at once which may be viable on high performance 
file systems.

The specific type of file being processed, and its compression, is inferred. 
As a result, the user does not need to provide these details. However, `mxdx`
cannot mix and match compression schemes or data types.

On demultiplexing, samples which are completely represented within a batch
are written entirely. Samples which are partially represented by a batch,
such that some records are processed in batch N and some in batch N+1,
are written as partial files. Partials are an unfortunate necessity without
a mechanism to orchestrate IO across independent jobs in a high performance
compute environment. To resolve partials, we provide a command with `mxdx`
called `consolidate-partials` to concatenate them in batch order. 
**NOTE**: `mxdx` does not `rm` the partial files, that is the responsibility
of the user.

# Usage example bash

A round trip usage example can be found in `usage-test.sh`. This test is 
executed by CI.

# Usage example SLURM

A usage example using SLURM, which is not executed by CI, can be found in
`examples/`.
