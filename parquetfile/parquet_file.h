//
// Parquet File Writer
//
// Copyright (c) 2015, 2016 Apsalar Inc.
// All rights reserved.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TFDTransport.h>

#include "parquet_column.h"

using apache::thrift::transport::TFDTransport;
using apache::thrift::protocol::TCompactProtocol;

using parquet::FileMetaData;

namespace parquet_file {

class ParquetFile
{
public:
    ParquetFile(std::string const & i_path, size_t i_rowgrpsz);

    void set_root(ParquetColumnHandle const & rh);

    void check_rowgrp_size();

    void write_file();

private:
    void write_row_group();
    
    std::string m_path;
    size_t m_rowgrpsz;
    
    int m_fd;
    FileMetaData m_file_meta_data;
    boost::shared_ptr<TFDTransport> m_file_transport;
    boost::shared_ptr<TCompactProtocol> m_protocol;
    ParquetColumnHandle m_root;

    ParquetColumnSeq m_leaf_cols;

    size_t m_num_rows;
    
    std::vector<parquet::RowGroup> m_row_groups;

    size_t m_nchecks;
};

} // end namespace parquet_file

// Local Variables:
// mode: C++
// End:
