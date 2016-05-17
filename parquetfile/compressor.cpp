//
// Parquet Compression Utility
//
// Copyright (c) 2016 Apsalar Inc.
// All rights reserved.
//

#include <string.h>
#include <stdlib.h>

#include <iostream>

#include <snappy.h>
#include <zlib.h>

#include "compressor.h"

using namespace std;
using namespace parquet;

namespace parquet_file {

Compressor::Compressor(parquet::CompressionCodec::type i_compression_codec)
    : m_compression_codec(i_compression_codec)
{
}

void
Compressor::compress(std::string & in, std::string & out)
{
    switch (m_compression_codec) {
    case CompressionCodec::UNCOMPRESSED:
        {
            // Just swap the input and output collections ...
            out.swap(in);
        }
        break;

    case CompressionCodec::SNAPPY:
        {
            size_t compressed_size;
            m_tmp.resize(snappy::MaxCompressedLength(in.size()));
            snappy::RawCompress((char const *) in.data(),
                                in.size(),
                                (char *) m_tmp.data(),
                                &compressed_size);
            m_tmp.resize(compressed_size);
            out.assign(m_tmp.begin(), m_tmp.end());
        }
        break;
        
    case CompressionCodec::GZIP:
        {
            int window_bits = 15 + 16; // maximum window + GZIP
            z_stream stream;
            memset(&stream, '\0', sizeof(stream));
            int rv = deflateInit2(&stream,
                                  Z_DEFAULT_COMPRESSION,
                                  Z_DEFLATED,
                                  window_bits,
                                  9,
                                  Z_DEFAULT_STRATEGY);
            if (rv != Z_OK) {
                cerr << "deflateInit2 failed: " << rv;
                exit(1);
            }
            m_tmp.resize(deflateBound(&stream, in.size()));
            stream.next_in =
                const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
            stream.avail_in = in.size();
            stream.next_out = (Bytef*) m_tmp.data();
            stream.avail_out = m_tmp.size();
            rv = deflate(&stream, Z_FINISH);
            if (rv != Z_STREAM_END) {
                cerr << "gzip deflate failed: " << rv;
                exit(1);
            }

            out.assign(m_tmp.begin(), m_tmp.begin() + stream.total_out);
            deflateEnd(&stream);
        }
        break;

    default:
        cerr << "unsupported compression codec: " << int(m_compression_codec);
        exit(1);
        break;
    }
}

} // end namespace parquet_file
