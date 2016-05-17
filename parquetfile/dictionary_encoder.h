//
// Parquet Dictionary Encoder
//
// Copyright (c) 2016 Apsalar Inc.
// All rights reserved.
//

#pragma once

#include <stdint.h>

#include <stdexcept>
#include <string>
#include <unordered_map>

namespace parquet_file {

typedef std::unordered_map<std::string, uint32_t> ValueIndexMap;

class DictionaryEncoder
{
public:
    DictionaryEncoder();

    uint32_t encode_datum(void const * i_ptr, size_t i_size, bool i_isvarlen)
        throw(std::overflow_error);

    void clear();

    static size_t const MAX_NVALS = 40 * 1000;
    
    size_t m_nvals;
    std::string m_data;

private:
    ValueIndexMap m_map;
};

} // end namespace parquet_file

// Local Variables:
// mode: C++
// End:
