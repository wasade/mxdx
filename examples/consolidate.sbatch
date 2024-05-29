#!/bin/bash -l
#SBATCH --time 24:00:00
#SBATCH --mem 8gb
#SBATCH -N 1
#SBATCH -c 2
#SBATCH --output %x-%A.out
#SBATCH --error %x-%A.err

if [[ -z ${EXT} ]]; then
    echo "EXT not set"
    exit 1
fi

if [[ -z ${OUTPUT_BASE} ]]; then
    echo "OUTPUT_BASE not set"
    exit 1
fi

mamba activate bowtie2

set -x 
set -e
set -o pipefail

mxdx consolidate-partials \
    --output-base ${OUTPUT_BASE} \
    --extension ${EXT}
