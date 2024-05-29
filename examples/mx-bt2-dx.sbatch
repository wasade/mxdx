#!/bin/bash -l
#SBATCH --time 24:00:00
#SBATCH --mem 8gb
#SBATCH -N 1
#SBATCH -c 4
#SBATCH --mail-type=FAIL,TIME_LIMIT_80,INVALID_DEPEND
#SBATCH --output %x-%A_%a.out
#SBATCH --error %x-%A_%a.err

if [[ -z ${SLURM_ARRAY_TASK_ID} ]]; then
    echo "Not operating in an array"
    exit 1
fi

if [[ -z ${FILES} ]]; then
    echo "FILES not set"
    exit 1
fi

if [[ -z ${DBIDX} ]]; then
    echo "DBIDX not set"
    exit 1
fi

if [[ -z ${BATCHSIZE} ]]; then
    echo "BATCHSIZE not set"
    exit 1
fi

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

# allow mxdx some cycles
bt2_cores=$((${SLURM_CPUS_PER_TASK} - 2))
mxdx mux \
    --file-map ${FILES} \
    --batch ${SLURM_ARRAY_TASK_ID} \
    --batch-size ${BATCHSIZE} | \
        bowtie2 \
            -p ${bt2_cores} \
            -x ${DBIDX} \
            -f - \
            --no-head \
            --no-unal | \
                mxdx demux \
                    --file-map ${FILES} \
                    --batch ${SLURM_ARRAY_TASK_ID} \
                    --batch-size ${BATCHSIZE} \
                    --output-base ${OUTPUT_BASE} \
                    --extension ${EXT}
