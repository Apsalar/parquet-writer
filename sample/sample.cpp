//
// Generate the Dremel paper sample data set in protobuf.
//
// Copyright (c) 2015-2016 Apsalar Inc. All rights reserved.
//

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

#include "sample.pb.h"

using namespace std;
using namespace google;

namespace {

string const g_fdspath = "./sample.fds";

void
write_tlv(uint8_t tag, uint32_t len, void const * data)
{
    // uint8_t		tag
    // uint32_t		len
    // uint8_t[]	val
    //
    cout.write((char const *) &tag, sizeof(tag));
    cout.write((char const *) &len, sizeof(len));
    cout.write((char const *) data, len);
}

void
output_header()
{
    // The header consists of:
    //
    // uint8_t		FileDescriptorSet tag (0)
    // uint32_t		FileDescriptorSet size
    // uint8_t[]	FileDescriptorSet data
    //
    // uint8_t		root message name tag (1)
    // uint32_t		root message name size
    // uint8_t[]	root message name data
    
    int fd = open(g_fdspath.c_str(), O_RDONLY);
    if (fd == -1) {
        cerr << "Trouble opening " << g_fdspath
             << ": " << strerror(errno) << endl;
        exit(2);
    }
    struct stat stat_buf;
    fstat(fd, &stat_buf);
    size_t fdssz = stat_buf.st_size;
    vector<uint8_t> buf(fdssz);
    read(fd, buf.data(), fdssz);

    write_tlv(0, fdssz, buf.data());

    string const rootmsg = "Document";

    write_tlv(1, rootmsg.size(), rootmsg.data());
}

void
output_document(sample::Document const & doc)
{
    // Each record consists of:
    //
    // uint8_t		record tag (2)
    // uint32_t		record size
    // uint8_t[]	record data

    ostringstream ostrm;
    if (! doc.SerializeToOstream(&ostrm)) {
        cerr << "trouble serializing Document" << endl;
        exit(1);
    }

    write_tlv(2, ostrm.str().size(), ostrm.str().data());
}

} // end namespace

int
main(int argc, char ** argv)
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    output_header();
    
    sample::Document document;
    sample::Document_mlinks * links;
    sample::Document_mname * name;
    sample::Document_mname_mlanguage * lang;

    document.Clear();
    document.set_docid(10);

    links = document.mutable_links();
    links->add_forward(20);
    links->add_forward(40);
    links->add_forward(60);

    name = document.add_name();
    lang = name->add_language();
    lang->set_code("en-us");
    lang->set_country("us");
    lang = name->add_language();
    lang->set_code("en");
    name->set_url("http://A");

    name = document.add_name();
    name->set_url("http://B");

    name = document.add_name();
    lang = name->add_language();
    lang->set_code("en-gb");
    lang->set_country("gb");

    output_document(document);

    document.Clear();
    document.set_docid(20);

    links = document.mutable_links();
    links->add_backward(10);
    links->add_backward(30);
    links->add_forward(80);

    name = document.add_name();
    name->set_url("http://C");

    output_document(document);
}
