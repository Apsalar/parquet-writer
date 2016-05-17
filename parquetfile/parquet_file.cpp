//
// Parquet File Writer
//
// Copyright (c) 2015, 2016 Apsalar Inc.
// All rights reserved.
//

#include <fcntl.h>

#include <iostream>
#include <set>

#include "parquet_file.h"

using namespace std;
using namespace parquet;

namespace parquet_file {

char const * PARQUET_MAGIC = "PAR1";

ParquetFile::ParquetFile(string const & i_path, size_t i_rowgrpsz)
    : m_path(i_path)
    , m_rowgrpsz(i_rowgrpsz)
    , m_num_rows(0)
    , m_nchecks(0)
{
    m_fd = open(i_path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0664);

    if (m_fd == -1) {
        cerr << "trouble creating file " << i_path.c_str()
             << ": " << strerror(errno);
        exit(1);
    }

    write(m_fd, PARQUET_MAGIC, strlen(PARQUET_MAGIC));

    m_file_transport.reset(new TFDTransport(m_fd));
    m_protocol.reset(new TCompactProtocol(m_file_transport));

    // Parquet-specific metadata for the file.
    m_file_meta_data.__set_version(1);
    m_file_meta_data.__set_created_by("Apsalar");
}

class SchemaBuilder : public ParquetColumn::Traverser
{
public:
    virtual void operator()(ParquetColumnHandle const & ch) {
        m_schema.push_back(ch->schema_element());
    }
    vector<SchemaElement> m_schema;
};

class ColumnListing : public ParquetColumn::Traverser
{
public:
    virtual void operator()(ParquetColumnHandle const & ch) {
        m_cols.push_back(ch);
    }
    ParquetColumnSeq m_cols;
};

void
ParquetFile::set_root(ParquetColumnHandle const & col)
{
    m_root = col;

    SchemaBuilder sb;
    sb(m_root);
    m_root->traverse(sb);
    m_file_meta_data.__set_schema(sb.m_schema);

    // Enumerate the leaf columns
    ColumnListing cl;
    cl(m_root);
    m_root->traverse(cl);
    for (auto it = cl.m_cols.begin(); it != cl.m_cols.end(); ++it) {
        ParquetColumnHandle const & ch = *it;
        if (ch->is_leaf())
            m_leaf_cols.push_back(ch);
    }
}

class RowGroupSizer : public ParquetColumn::Traverser
{
public:
    RowGroupSizer() : m_rowgrp_size(0L) {}
    
    virtual void operator()(ParquetColumnHandle const & ch) {
        m_rowgrp_size += ch->estimated_rowgrp_size();
    }
    size_t m_rowgrp_size;
};

void
ParquetFile::check_rowgrp_size()
{
    // Only check every Nth time we are called.
    if (++m_nchecks % 100 != 0)
        return;
    
    // Check the aggregate row group size and write if we are getting
    // too big.

    RowGroupSizer sizer;
    sizer(m_root);
    m_root->traverse(sizer);

    if (sizer.m_rowgrp_size >= m_rowgrpsz)
        write_row_group();
}

void
ParquetFile::write_file()
{
    write_row_group();
    
    m_file_meta_data.__set_num_rows(m_num_rows);
    m_file_meta_data.__set_row_groups(m_row_groups);
    
    uint32_t file_metadata_length = m_file_meta_data.write(m_protocol.get());
    write(m_fd, &file_metadata_length, sizeof(file_metadata_length));
    write(m_fd, PARQUET_MAGIC, strlen(PARQUET_MAGIC));
    close(m_fd);
}

void
ParquetFile::write_row_group()
{
    // Make sure we have the same number of records in all leaf
    // columns.
    set<size_t> colnrecs;
    for (auto it = m_leaf_cols.begin(); it != m_leaf_cols.end(); ++it) {
        ParquetColumnHandle const & ch = *it;
        colnrecs.insert(ch->num_rowgrp_records());
    }
    if (colnrecs.size() > 1) {
        cerr << "all leaf columns must have the same number of record; "
             << "saw " << colnrecs.size() << " different sizes";
        exit(1);
    }
    size_t numrecs = *(colnrecs.begin());

    m_num_rows += numrecs;
    
    RowGroup row_group;
    row_group.__set_num_rows(numrecs);
    vector<ColumnChunk> column_chunks;
    for (auto it = m_leaf_cols.begin(); it != m_leaf_cols.end(); ++it) {
        ParquetColumnHandle const & ch = *it;

        ColumnMetaData column_metadata =
            ch->write_row_group(m_fd, m_protocol.get());

        row_group.__set_total_byte_size
            (row_group.total_byte_size +
             column_metadata.total_uncompressed_size);

        ColumnChunk column_chunk;
        column_chunk.__set_file_path("");	// Neal didn't initialize
        column_chunk.__set_file_offset(column_metadata.data_page_offset);
        column_chunk.__set_meta_data(column_metadata);
        column_chunks.push_back(column_chunk);
    }
    row_group.__set_columns(column_chunks);

    m_row_groups.push_back(row_group);
}

} // end namespace parquet_file
