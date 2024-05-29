#!/bin/bash

files=../usage-test-files.tsv
dbidx=/path/to/db
batchsize=15
batchmax=$(mxdx get-max-batch-number --file-map ${files} --batch-size ${batchsize})
output_base=$(pwd)
ext=sam.xz

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
