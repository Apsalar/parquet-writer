//
// Parquet Column Writer
//
// Copyright (c) 2015, 2016 Apsalar Inc.
// All rights reserved.
//

#pragma once

#include <deque>
#include <fstream>
#include <string>
#include <vector>

#include "util/rle-encoding.h"

#include <thrift/protocol/TCompactProtocol.h>

#include "parquet_types.h"

#include "compressor.h"
#include "dictionary_encoder.h"

namespace parquet_file {

typedef std::vector<std::string> StringSeq;

class ParquetColumn;
typedef std::shared_ptr<ParquetColumn> ParquetColumnHandle;
typedef std::vector<ParquetColumnHandle> ParquetColumnSeq;

class ParquetColumn
{
public:
    ParquetColumn(StringSeq const & i_path,
                  parquet::Type::type i_data_type,
                  parquet::ConvertedType::type i_converted_type,
                  int i_maxreplvl,
                  int i_maxdeflvl,
                  parquet::FieldRepetitionType::type i_repetition_type,
                  parquet::Encoding::type i_encoding,
                  parquet::CompressionCodec::type i_compression_codec);

    void add_child(ParquetColumnHandle const & ch);

    void add_datum(void const * i_ptr, size_t i_size, bool i_isvarlen,
                   int i_replvl, int i_deflvl);

    void add_boolean_datum(bool i_val, int i_replvl, int i_deflvl);

    std::string name() const;

    parquet::Type::type data_type() const;

    parquet::ConvertedType::type converted_type() const;

    parquet::FieldRepetitionType::type repetition_type() const;
    
    std::string path_string() const;

    ParquetColumnSeq const & children() const;

    bool is_leaf() const;
    
    size_t num_rowgrp_records() const;

    size_t rowgrp_size() const;

    size_t estimated_rowgrp_size() const;

    class Traverser
    {
    public:
        virtual void operator()(ParquetColumnHandle const & ch) = 0;
    };

    void traverse(Traverser & tt);

    parquet::ColumnMetaData write_row_group(int fd,
                    apache::thrift::protocol::TCompactProtocol * protocol);

    parquet::SchemaElement schema_element() const;

private:
    static size_t const PAGE_SIZE = 64 * 1024;

    struct DataPage
    {
        parquet::PageHeader	m_page_header;
        std::string			m_page_data;

        size_t
        write_page(int fd,
                   apache::thrift::protocol::TCompactProtocol * protocol);
    };
    typedef std::shared_ptr<DataPage> DataPageHandle;
    typedef std::deque<DataPageHandle> DataPageSeq;

    inline void check_full(size_t i_size)
    {
        if (m_data.size() + i_size > PAGE_SIZE ||
            m_rep_enc.IsFull() ||
            m_def_enc.IsFull() ||
            m_val_enc.IsFull())
            finalize_page();
    }
    
    void add_levels(int i_replvl, int i_deflvl);

    void finalize_page();
    
    void concatenate_page_data(std::string & buffer);

    void reset_page_state();
    
    void reset_row_group_state();

    StringSeq m_path;
    int m_maxreplvl;
    int m_maxdeflvl;
    parquet::Type::type m_data_type;
    parquet::ConvertedType::type m_converted_type;
    parquet::FieldRepetitionType::type m_repetition_type;
    parquet::Encoding::type m_original_encoding;
    parquet::Encoding::type m_encoding;
    std::vector<parquet::Encoding::type> m_encodings;
    parquet::CompressionCodec::type m_compression_codec;

    Compressor m_compressor;
    
    ParquetColumnSeq m_children;

    // Page accumulation
    std::string m_data;
    size_t m_num_page_values;
    impala::RleEncoder m_rep_enc;	// Repetition Level
    impala::RleEncoder m_def_enc;	// Definition Level
    impala::RleEncoder m_val_enc;	// Dictionary Encoded Values
    uint8_t m_rep_buf[PAGE_SIZE];
    uint8_t m_def_buf[PAGE_SIZE];
    uint8_t m_val_buf[PAGE_SIZE];
    std::string m_concat_buffer;
    uint8_t m_bool_buf;
    int m_bool_cnt;
    
    // Row-Group accumulation
    DataPageSeq m_pages;
    size_t m_num_rowgrp_recs;
    size_t m_num_rowgrp_values;
    off_t m_column_write_offset;
    size_t m_uncompressed_size;
    size_t m_compressed_size;
    DictionaryEncoder m_dict_enc;
};

} // end namespace parquet_file

// Local Variables:
// mode: C++
// End:
