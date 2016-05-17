//
// Parquet Dictionary Encoder
//
// Copyright (c) 2016 Apsalar Inc.
// All rights reserved.
//

#include "dictionary_encoder.h"

using namespace std;

namespace parquet_file {

DictionaryEncoder::DictionaryEncoder()
    : m_nvals(0)
    , m_map(MAX_NVALS)
{
}

uint32_t
DictionaryEncoder::encode_datum(void const * i_ptr,
                                size_t i_size,
                                bool i_isvarlen)
    throw(overflow_error)
{
    string val(static_cast<char const *>(i_ptr),
               static_cast<char const *>(i_ptr) + i_size);

    auto pos = m_map.find(val);
    if (pos != m_map.end()) {
        return pos->second;
    }
    else {
        if (m_nvals >= MAX_NVALS)
            throw overflow_error("too many encoded values");
        
        if (i_isvarlen) {
            uint32_t len = i_size;
            uint8_t * lenptr = (uint8_t *) &len;
            m_data.insert(m_data.end(), lenptr, lenptr + sizeof(len));
        }
        m_data.insert(m_data.end(),
                      static_cast<char const *>(i_ptr),
                      static_cast<char const *>(i_ptr) + i_size);
        uint32_t ndx = m_nvals++;
        m_map.insert(make_pair(val, ndx));
        return ndx;
    }
}

void
DictionaryEncoder::clear()
{
    m_nvals = 0;
    m_data.clear();
    m_map.clear();
}

} // end namespace parquet_file
