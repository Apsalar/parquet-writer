
Introduction
----------------------------------------------------------------

This project provides libparquetfile, a C++ library which generates
parquet files.

Additionally the proto2parq application is provided which can convert
a data files or streams containing protobuf defined records into
parquet format.


Acknowledgements
----------------------------------------------------------------

The early development on this project was inspired by
[Neal Sidhwaney's cpp-parquet project](https://github.com/nealsid/cpp-parquet).

Some of the parquet writing C++ components are extracted from the
[Impala Database Project](https://github.com/cloudera/Impala).


Building
----------------------------------------------------------------

You'll need the Thrift development tools installed:

    sudo dnf install thrift-devel

Update the parquet-format submodule:

    git submodule update --init

Build with make from the top-level directory:

    gmake


Running the Sample Program
----------------------------------------------------------------

The sample program generates the sample data described in the
[Dremel paper](http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf) and the [parquet-mr annotation document](https://github.com/Parquet/parquet-mr/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper) in protobuf format.

The output of this program can be piped into proto2parq and converted
to parquet format:

    cd sample/OBJDIR
    ./sample | \
        ../../proto2parq/OBJDIR/proto2parq --outfile=sample.parquet

The protobuf schema is prepended to the begining of the protobuf data
output.
