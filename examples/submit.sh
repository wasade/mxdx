#!/bin/bash

files=../usage-test-files.tsv
dbidx=/path/to/bt2-db
batchsize=15
batchmax=$(mxdx get-number-of-batches --file-map ${files})
output_base=$(pwd)
ext=fna.gz

j=$(sbatch \
      --parsable \
      --array 0-${batchmax} \
      -J mxdx-test \
      --export FILES=${files},DBIDX=${dbidx},BATCHSIZE=${batchsize},EXT=${ext},OUTPUT_BASE=${output_base} \
      mx-bt2-dx.sbatch)
sbatch \
    -J mxdx-test-consolidate \
    --dependency afterok:${j} \
    --export OUTPUT_BASE=${output_base},EXT=${ext} \
    consolidate.sbatch
