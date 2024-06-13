#!/bin/bash

if [[ "$(uname)" == "Darwin" ]]; then
    md5=md5
    zcat=funzip
else
    md5=md5sum
    zcat=zcat
fi


set -x
set -e
set -o pipefail

cut -f 1 usage-test-files.tsv > usage-test-files-no-count.tsv
mxdx set-record-count \
    --file-map usage-test-files-no-count.tsv \
    --output usage-test-files-with-count.tsv
obs=$(cat usage-test-files-with-count.tsv | ${md5})
exp=$(cat usage-test-files.tsv | ${md5})

if [[ ${obs} != ${exp} ]]; then
    echo ${obs}
    echo ${exp}
    exit 1
fi

# round trip
mxdx mux \
    --file-map usage-test-files.tsv \
    --batch 0 \
    --batch-size 15 | \
        mxdx demux \
            --file-map usage-test-files.tsv \
            --batch 0 \
            --batch-size 15 \
            --output-base usage-test \
            --extension fna.gz
mxdx mux \
    --file-map usage-test-files.tsv \
    --batch 1 \
    --batch-size 15 | \
        mxdx demux \
            --file-map usage-test-files.tsv \
            --batch 1 \
            --batch-size 15 \
            --output-base usage-test \
            --extension fna.gz
mxdx consolidate-partials \
    --output-base usage-test \
    --extension fna.gz

obs_foo=$(${zcat} usage-test/foo_r1.fasta.fna.gz | ${md5})
obs_bar=$(${zcat} usage-test/bar_r1.fasta.fna.gz | ${md5})
exp_foo=$(cat mxdx/tests/test_data/foo_r1.fasta | ${md5})
exp_bar=$(cat mxdx/tests/test_data/bar_r1.fasta | ${md5})

if [[ ${obs_foo} != ${exp_foo} ]]; then
    echo ${obs_foo}
    echo ${exp_foo}
    exit 1
fi
if [[ ${obs_bar} != ${exp_bar} ]]; then
    echo ${obs_bar}
    echo ${exp_bar}
    exit 1
fi
