"""mxdx: multiplexing and demultiplexing."""
import click
import sys
import pathlib

from ._io import FileMap
from ._mxdx import Multiplex, Demultiplex, Consolidate
from ._constants import (INTERLEAVE, R1ONLY, R2ONLY, SEQUENTIAL,
                         MERGE, SEPARATE)

@click.group()
def cli():
    """mxdx: multiplexing and demultiplexing."""
    pass


@cli.command()
@click.option('--file-map', type=click.Path(exists=True), required=True,
              help="Files with record counts for processing")
@click.option('--batch', type=int, required=True,
              help="0-based index for batch offset")
@click.option('--batch-size', type=int, required=True,
              help="Number of records per batch")
@click.option('--output', type=click.Path(exists=False), required=False,
              default='-', help="Where to write, '-' for stdout")
@click.option('--paired-handling',
              type=click.Choice([INTERLEAVE, R1ONLY, R2ONLY, SEQUENTIAL]),
              default=SEQUENTIAL, required=False,
              help="How to handle paired data")
def mux(file_map, batch, batch_size, output, paired_handling):
    """Multiplex a set of files into a single stream."""
    file_map = FileMap.from_tsv(file_map, batch_size)

    if batch == 0:
        file_map.check_paths()

    mxfile_batch = file_map.batch(batch)
    if not mxfile_batch:
        click.echo("Nothing to do...", err=True)
        sys.exit(0)

    mx = Multiplex(file_map, batch, paired_handling, output)
    mx.start()


@cli.command()
@click.option('--mux-input', type=str, required=False,
              default='-', help="The multiplexed data, '-' for stdin")
@click.option('--file-map', type=click.Path(exists=True), required=True,
              help="Files with record counts for processing")
@click.option('--batch', type=int, required=True,
              help="0-based index for batch offset")
@click.option('--batch-size', type=int, required=True,
              help="Number of records per batch")
@click.option('--output-base', type=click.Path(exists=False), required=True,
              help="Where to write")
@click.option('--paired-handling',
              type=click.Choice([SEPARATE, MERGE]),
              default=SEPARATE, required=False,
              help="How to handle paired data")
@click.option('--extension', type=str, required=True,
              help=("The output file extension to use, which determines "
                    "what compression to use"))
def demux(mux_input, file_map, batch, batch_size, output_base,
          paired_handling, extension):
    """Demultiplex a stream into a set of files."""
    file_map = FileMap.from_tsv(file_map, batch_size)

    mxfile_batch = file_map.batch(batch)
    if not mxfile_batch:
        click.echo("Nothing to do...", err=True)
        sys.exit(0)

    pathlib.Path(output_base).mkdir(parents=True, exist_ok=True)

    dx = Demultiplex(file_map, batch, paired_handling, mux_input, output_base,
                     extension)
    dx.start()


@cli.command()
@click.option('--output-base', type=click.Path(exists=True), required=True,
              help="Where to write")
@click.option('--extension', type=str, required=True,
              help=("The output file extension to use, which determines "
                    "what compression to use"))
def consolidate_partials(output_base, extension):
    """Consolidate partial files."""
    cx = Consolidate(output_base, extension)
    cx.start()


@cli.command()
@click.option('--file-map', type=click.Path(exists=True), required=True,
              help="Files with record counts for processing")
@click.option('--batch-size', type=int, required=True,
              help="Number of records per batch")
@click.option('--is-one-based', is_flag=True, default=False,
              help="Whether indexing is zero or one based")
def get_max_batch_number(file_map, batch_size, is_one_based):
    """Determine the maximal batch number."""
    file_map = FileMap.from_tsv(file_map, batch_size)
    num_batches = file_map.number_of_batches
    if is_one_based:
        click.echo(num_batches)
    else:
        click.echo(num_batches - 1)


if __name__ == '__main__':
    cli()
