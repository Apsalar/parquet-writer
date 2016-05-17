
Prerequisites
----------------------------------------------------------------

    thrift-devel


Building
----------------------------------------------------------------

    git submodule update --init
    gmake


Running the Sample Program
----------------------------------------------------------------

    cd sample/OBJDIR
    ./sample | \
        ../../proto2parq/OBJDIR/proto2parq --outfile=sample.parquet
