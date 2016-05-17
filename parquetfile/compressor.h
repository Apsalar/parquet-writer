//
// Parquet Compression Utility
//
// Copyright (c) 2016 Apsalar Inc.
// All rights reserved.
//

#pragma once

#include <string>
#include <vector>

#include "parquet_types.h"

namespace parquet_file {

class Compressor
{
public:
    Compressor(parquet::CompressionCodec::type i_compression_codec);

    void compress(std::string & in, std::string & out);

private:
    parquet::CompressionCodec::type m_compression_codec;
    std::string m_tmp;
};
    
} // end namespace parquet_file

// Local Variables:
// mode: C++
// End:
