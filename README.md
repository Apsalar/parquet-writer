
Prerequisites
----------------------------------------------------------------

    thrift-devel
    

Running the Sample Program
----------------------------------------------------------------

    cd sample/OBJDIR
    ./sample | \
    ../../proto2parq/OBJDIR/proto2parq --outfile=sample.parquet

    export COLUMNS=200
    export PQTOOLS=~/apsalar/parquet-mr/parquet-tools/target/parquet-tools-1.8.2-SNAPSHOT.jar
    java -jar $PQTOOLS dump sample.parquet
