//
// Schema related functions
//
// Copyright (c) 2015, 2016 Apsalar Inc. All rights reserved.
//

#include <arpa/inet.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "parquet_types.h"

#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/descriptor.h>

#include "protobuf-schema-walker.h"

using namespace std;
using namespace google::protobuf;
using namespace google::protobuf::compiler;

using namespace protobuf_schema_walker;
using namespace parquet;
using namespace parquet_file;

namespace {

typedef vector<uint8_t> OctetSeq;
typedef pair<uint8_t, OctetSeq> TLV;

string
pathstr(StringSeq const & path)
{
    ostringstream ostrm;
    for (size_t ndx = 0; ndx < path.size(); ++ndx) {
        if (ndx != 0)
            ostrm << '.';
        ostrm << path[ndx];
    }
    return ostrm.str();
}

string
repstr(FieldDescriptor const * fd)
{
    // Return string expressing the field repetition type.
    if (fd->is_required())
        return "REQ";
    else if (fd->is_optional())
        return "OPT";
    else if (fd->is_repeated())
        return "RPT";
    else
        return "???";
}

TLV
read_record(istream & istrm)
{
    uint8_t tag;
    uint32_t len;
    OctetSeq val;

    if (!istrm.good())
        return move(make_pair(0xff, val));
    
    istrm.read((char *) &tag, sizeof(tag));
    istrm.read((char *) &len, sizeof(len));
    val.resize(len);
    istrm.read((char *) val.data(), len);

    if (!istrm.good()) {
        val.clear();
        return move(make_pair(0xff, val));
    }
    
    return move(make_pair(tag, val));
}

class FieldDumper : public NodeTraverser
{
public:
    FieldDumper(ostream & ostrm) : m_ostrm(ostrm) {}

    virtual void visit(SchemaNode const * np)
    {
        if (np->m_fdp) {
            m_ostrm << pathstr(np->m_path)
                    << ' ' << np->m_fdp->cpp_type_name()
                    << ' ' << repstr(np->m_fdp)
                    << endl;
        }
    }

private:
    ostream & m_ostrm;
};

SchemaNodeHandle
traverse_leaf(StringSeq & path,
              FieldDescriptor const * i_fd,
              int i_maxreplvl,
              int i_maxdeflvl,
              bool i_dotrace)
{
    int maxreplvl = i_fd->is_repeated() ? i_maxreplvl + 1 : i_maxreplvl;
    int maxdeflvl = i_fd->is_required() ? i_maxdeflvl : i_maxdeflvl + 1;

    SchemaNodeHandle retval =
        make_shared<SchemaNode>(path, (Descriptor *) NULL, i_fd,
                                maxreplvl, maxdeflvl, i_dotrace);
    return move(retval);
}

SchemaNodeHandle
traverse_group(StringSeq & path,
               FieldDescriptor const * i_fd,
               int i_maxreplvl,
               int i_maxdeflvl,
               bool i_dotrace)
{
    Descriptor const * dd = i_fd->message_type();

    int maxreplvl = i_fd->is_repeated() ? i_maxreplvl + 1 : i_maxreplvl;
    int maxdeflvl = i_fd->is_required() ? i_maxdeflvl : i_maxdeflvl + 1;

    SchemaNodeHandle retval =
        make_shared<SchemaNode>(path, dd, i_fd,
                                maxreplvl, maxdeflvl, i_dotrace);
    
    for (int ndx = 0; ndx < dd->field_count(); ++ndx) {
        FieldDescriptor const * fd = dd->field(ndx);
        path.push_back(fd->name());
        switch (fd->cpp_type()) {
        case FieldDescriptor::CPPTYPE_MESSAGE:
            {
                SchemaNodeHandle child =
                    traverse_group(path, fd, maxreplvl, maxdeflvl, i_dotrace);
                retval->add_child(child);
            }
            break;
        default:
            {
                SchemaNodeHandle child =
                    traverse_leaf(path, fd, maxreplvl, maxdeflvl, i_dotrace);
                retval->add_child(child);
            }
            break;
        }
        path.pop_back();
    }

    return move(retval);
}

SchemaNodeHandle
traverse_root(StringSeq & path, Descriptor const * dd, bool dotrace)
{
    SchemaNodeHandle retval =
        make_shared<SchemaNode>(path, dd, (FieldDescriptor *) NULL,
                                0, 0, dotrace);
    
    for (int ndx = 0; ndx < dd->field_count(); ++ndx) {
        FieldDescriptor const * fd = dd->field(ndx);
        path.push_back(fd->name());
        switch (fd->cpp_type()) {
        case FieldDescriptor::CPPTYPE_MESSAGE:
            {
                SchemaNodeHandle child =
                    traverse_group(path, fd, 0, 0, dotrace);
                retval->add_child(child);
            }
            break;
        default:
            {
                SchemaNodeHandle child =
                    traverse_leaf(path, fd, 0, 0, dotrace);
                retval->add_child(child);
            }
            break;
        }
        path.pop_back();
    }

    return move(retval);
}

class MyErrorCollector : public MultiFileErrorCollector
{
    virtual void AddError(string const & filename,
                          int line,
                          int column,
                          string const & message)
    {
        cerr << filename
             << ':' << line
             << ':' << column
             << ':' << message << endl;
    }
};

} // end namespace

namespace protobuf_schema_walker {

SchemaNode::SchemaNode(StringSeq const & i_path,
                       Descriptor const * i_dp,
                       FieldDescriptor const * i_fdp,
                       int i_maxreplvl,
                       int i_maxdeflvl,
                       bool i_dotrace)
        : m_path(i_path)
        , m_dp(i_dp)
        , m_fdp(i_fdp)
        , m_maxreplvl(i_maxreplvl)
        , m_maxdeflvl(i_maxdeflvl)
        , m_dotrace(i_dotrace)
{
    // Are we the root node?
    if (m_fdp == NULL) {
        parquet::Type::type data_type = parquet::Type::INT32;
        parquet::ConvertedType::type converted_type =
            parquet::ConvertedType::INT_32;
        FieldRepetitionType::type repetition_type =
            FieldRepetitionType::REQUIRED;
        Encoding::type encoding = Encoding::PLAIN;
        CompressionCodec::type compression_codec =
            CompressionCodec::UNCOMPRESSED;

        m_pqcol =
            make_shared<ParquetColumn>(i_path,
                                       data_type,
                                       converted_type,
                                       m_maxreplvl,
                                       m_maxdeflvl,
                                       repetition_type,
                                       encoding,
                                       compression_codec);
    }
    else {
        parquet::Type::type data_type;
        parquet::ConvertedType::type converted_type =
            parquet::ConvertedType::type(-1);
        Encoding::type encoding = Encoding::PLAIN_DICTIONARY;

        switch (m_fdp->type()) {
        case FieldDescriptor::TYPE_DOUBLE:
            data_type = parquet::Type::DOUBLE;
            break;
        case FieldDescriptor::TYPE_FLOAT:
            data_type = parquet::Type::FLOAT;
            break;
        case FieldDescriptor::TYPE_INT64:
        case FieldDescriptor::TYPE_SINT64:
        case FieldDescriptor::TYPE_SFIXED64:
            data_type = parquet::Type::INT64;
            break;
        case FieldDescriptor::TYPE_UINT64:
        case FieldDescriptor::TYPE_FIXED64:
            data_type = parquet::Type::INT64;
            converted_type = parquet::ConvertedType::UINT_64;
            break;
        case FieldDescriptor::TYPE_INT32:
        case FieldDescriptor::TYPE_SINT32:
        case FieldDescriptor::TYPE_SFIXED32:
            data_type = parquet::Type::INT32;
            break;
        case FieldDescriptor::TYPE_UINT32:
        case FieldDescriptor::TYPE_FIXED32:
            data_type = parquet::Type::INT32;
            converted_type = parquet::ConvertedType::UINT_32;
            break;
        case FieldDescriptor::TYPE_BOOL:
            data_type = parquet::Type::BOOLEAN;
            encoding = Encoding::PLAIN;
            break;
        case FieldDescriptor::TYPE_STRING:
            data_type = parquet::Type::BYTE_ARRAY;
            converted_type = parquet::ConvertedType::UTF8;
            break;
        case FieldDescriptor::TYPE_BYTES:
            data_type = parquet::Type::BYTE_ARRAY;
            break;
        case FieldDescriptor::TYPE_MESSAGE:
        case FieldDescriptor::TYPE_GROUP:
            // This strikes me as bad; is there an out-of-band value instead?
            data_type = parquet::Type::INT32;
            converted_type = parquet::ConvertedType::INT_32;
            break;
        case FieldDescriptor::TYPE_ENUM:
            cerr << "enum currently unsupported";
            exit(1);
            converted_type = parquet::ConvertedType::ENUM;
            break;
        default:
            cerr << "unsupported type: " << int(m_fdp->type());
            exit(1);
            break;
        }
        
        FieldRepetitionType::type repetition_type =
            m_fdp->is_required() ? FieldRepetitionType::REQUIRED :
            m_fdp->is_optional() ? FieldRepetitionType::OPTIONAL :
            FieldRepetitionType::REPEATED;

        CompressionCodec::type compression_codec =
            CompressionCodec::SNAPPY;

        m_pqcol = make_shared<ParquetColumn>(i_path,
                                             data_type,
                                             converted_type,
                                             m_maxreplvl,
                                             m_maxdeflvl,
                                             repetition_type,
                                             encoding,
                                             compression_codec);
    }
}

void
SchemaNode::add_child(SchemaNodeHandle const & i_child)
{
    m_children.push_back(i_child);
    m_pqcol->add_child(i_child->m_pqcol);
}

void
SchemaNode::traverse(NodeTraverser & nt)
{
    nt.visit(this);
    for (SchemaNodeSeq::iterator it = m_children.begin();
         it != m_children.end();
         ++it) {
        SchemaNodeHandle const & np = *it;
        np->traverse(nt);
    }
}

void
SchemaNode::propagate_message(Message const * i_msg,
                              int replvl, int deflvl)
{
    Reflection const * reflp =
        i_msg ? i_msg->GetReflection() : NULL;

    for (SchemaNodeSeq::iterator it = m_children.begin();
         it != m_children.end();
         ++it) {
        SchemaNodeHandle const & np = *it;
        np->propagate_field(reflp, i_msg, replvl, deflvl);
    }
}

void
SchemaNode::propagate_field(Reflection const * i_reflp,
                            Message const * i_msg,
                            int replvl, int deflvl)
{
    if (m_fdp->is_required()) {
        propagate_value(i_reflp, i_msg, -1, replvl, deflvl);
    }
    else if (m_fdp->is_optional()) {
        if (i_msg != NULL &&
            i_reflp->HasField(*i_msg, m_fdp)) {
            propagate_value(i_reflp, i_msg, -1, replvl, deflvl+1);
        }
        else {
            if (m_fdp->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                propagate_message(NULL, replvl, deflvl);
            }
            else {
                propagate_value(NULL, NULL, -1, replvl, deflvl);
            }
        }
    }
    else if (m_fdp->is_repeated()) {
        size_t nvals = i_reflp->FieldSize(*i_msg, m_fdp);
        if (nvals > 0) {
            for (size_t ndx = 0; ndx < nvals; ++ndx) {
                if (ndx == 0)
                    propagate_value(i_reflp, i_msg, ndx, replvl, deflvl+1);
                else
                    propagate_value(i_reflp, i_msg, ndx, m_maxreplvl, deflvl+1);
            }
        }
        else {
            if (m_fdp->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                propagate_message(NULL, replvl, deflvl);
            }
            else {
                propagate_value(NULL, NULL, -1, replvl, deflvl);
            }
        }
    }
    else {
        cerr << "field " << pathstr(m_path)
             << " isn't required, optional or repeated";
        exit(1);
    }
}

void
SchemaNode::propagate_value(Reflection const * i_reflp,
                            Message const * i_msg,
                            int ndx,
                            int i_replvl, int deflvl)
{
    int replvl = ndx == 0 ? 0 : i_replvl;

    if (!i_reflp) {
        if (m_dotrace) {
            cerr << pathstr(m_path) << ": " << "NULL"
                 << ", R:" << replvl << ", D:" << deflvl
                 << endl;
        }
        switch (m_fdp->cpp_type()) {
        case FieldDescriptor::CPPTYPE_STRING:
            m_pqcol->add_datum(NULL, 0, true, replvl, deflvl);
            break;
        default:
            m_pqcol->add_datum(NULL, 0, false, replvl, deflvl);
            break;
        }
    }
    else {
        switch (m_fdp->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            {
                int32_t val = ndx == -1
                    ? i_reflp->GetInt32(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedInt32(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_datum(&val, sizeof(val), false, replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            {
                int64_t val = ndx == -1
                    ? i_reflp->GetInt64(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedInt64(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_datum(&val, sizeof(val), false, replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            {
                uint32_t val = ndx == -1
                    ? i_reflp->GetUInt32(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedUInt32(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_datum(&val, sizeof(val), false, replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            {
                uint64_t val = ndx == -1
                    ? i_reflp->GetUInt64(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedUInt64(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_datum(&val, sizeof(val), false, replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_DOUBLE:
            {
                double val = ndx == -1
                    ? i_reflp->GetDouble(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedDouble(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_datum(&val, sizeof(val), false, replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_FLOAT:
            {
                float val = ndx == -1
                    ? i_reflp->GetFloat(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedFloat(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_datum(&val, sizeof(val), false, replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_BOOL:
            {
                bool val = ndx == -1
                    ? i_reflp->GetBool(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedBool(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_boolean_datum(val, replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_ENUM:
            cerr << "field " << pathstr(m_path)
                 << " is of unknown type: " << m_fdp->cpp_type_name();
            exit(1);
            break;
        case FieldDescriptor::CPPTYPE_STRING:
            {
                string val = ndx == -1
                    ? i_reflp->GetString(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedString(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_datum(val.data(), val.size(), true,
                                   replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_MESSAGE:
            {
                Message const & cmsg = ndx == -1
                    ? i_reflp->GetMessage(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedMessage(*i_msg, m_fdp, ndx);
                propagate_message(&cmsg, i_replvl, deflvl);
            }
            break;
        default:
            cerr << "field " << pathstr(m_path)
                 << " is of unknown type: " << int(m_fdp->cpp_type());
            exit(1);
            break;
        }
    }
}

Schema::Schema(string const & i_protodir,
               string const & i_protofile,
               string const & i_rootmsg,
               string const & i_infile,
               string const & i_outfile,
               size_t i_rowgrpsz,
               bool i_dotrace)
    : m_protofile(i_protofile)
    , m_nrecs(0ULL)
    , m_dotrace(i_dotrace)
{
    if (i_infile == "-") {
        m_istrmp = &cin;
    }
    else {
        m_ifstrm.open(i_infile.c_str(), ifstream::in | ifstream::binary);
        if (!m_ifstrm.good()) {
            cerr << "trouble opening input file: " << i_infile;
            exit(1);
        }
        m_istrmp = &m_ifstrm;
    }

    m_poolp.reset(new DescriptorPool());

    string rootmsg;
    
    // Is the proto description in the data file or specified explicitly?
    FileDescriptor const * fdp;
    if (i_protofile.empty()) {
        fdp = process_header(*m_istrmp);
        rootmsg = process_rootmsg(*m_istrmp);
    }
    else {
        m_srctree.MapPath("", i_protodir);
        m_errcollp.reset(new MyErrorCollector());
        m_importerp.reset(new Importer(&m_srctree, m_errcollp.get()));
        fdp = m_importerp->Import(i_protofile);
        if (!fdp) {
            cerr << "trouble opening proto file " << i_protofile
                 << " in directory " << i_protodir;
            exit(1);
        }
        rootmsg = i_rootmsg;
    }

    m_typep = fdp->FindMessageTypeByName(rootmsg);
    if (!m_typep) {
        cerr << "couldn't find root message: " << rootmsg;
        exit(1);
    }

    m_proto = m_dmsgfact.GetPrototype(m_typep);

    unlink(i_outfile.c_str());

    m_output.reset(new ParquetFile(i_outfile, i_rowgrpsz));

    StringSeq path = { m_typep->full_name() };
    m_root = traverse_root(path, m_typep, m_dotrace);

    m_output->set_root(m_root->column());
}

void
Schema::dump(ostream & ostrm)
{
    FieldDumper fdump(ostrm);
    traverse(fdump);
    ostrm << endl;
}

void
Schema::convert()
{
    bool more = true;
    while (more) {
        more = process_record(*m_istrmp);
        if (m_dotrace) {
            cerr << endl;
        }
    }
    m_output->write_file();
    cerr << "processed " << m_nrecs << " records" << endl;
}

void
Schema::traverse(NodeTraverser & nt)
{
    m_root->traverse(nt);
}

FileDescriptor const *
Schema::process_header(istream & istrm)
{
    TLV rec = read_record(istrm);
    if (rec.first != 0) {
        cerr << "expecting FileDescriptorSet (0), saw " << rec.first;
        exit(1);
    }
    
    FileDescriptorSet fds;
    fds.ParseFromArray(rec.second.data(), rec.second.size());

    FileDescriptorProto const & fdproto = fds.file(0);

    return m_poolp->BuildFile(fdproto);
}

string
Schema::process_rootmsg(istream & istrm)
{
    TLV rec = read_record(istrm);
    if (rec.first != 1) {
        cerr << "expecting root msg name (1), saw " << rec.first;
        exit(1);
    }
    
    return string((char const *) rec.second.data(),
                  (char const *) rec.second.data() + rec.second.size());
}

bool
Schema::process_record(istream & istrm)
{
    m_output->check_rowgrp_size();

    unique_ptr<Message> inmsg(m_proto->New());

    if (!m_protofile.empty()) {
        // Use the original protocol.

        // Read the record header, swap bytes as necessary.
        int16_t proto;
        int8_t type;
        int32_t size;
 
        istrm.read((char *) &proto, sizeof(proto));
        istrm.read((char *) &type, sizeof(type));
        istrm.read((char *) &size, sizeof(size));

        if (!istrm.good())
            return false;

        string buffer(size_t(size), '\0');
        istrm.read(&buffer[0], size);
        if (!istrm.good())
            return false;

        ++m_nrecs;
        
        inmsg->ParseFromString(buffer);

    } else {
        // Use the new protocol.
        
        TLV rec = read_record(istrm);

        switch (rec.first) {
        case 0xff:	// EOF
            return false;
        
        case 0:	// FileDescriptorSet header
            // Must have been called w/ explicit, skip ...
            return true;
        
        case 1:	// root message name
            // Must have been called w/ explicit, skip ...
            return true;
        
        case 2:
            // This is our record!
            ++m_nrecs;
            break;
        
        default:
            cerr << "expecting data record (2), saw " << rec.first;
            exit(1);
            break;
        }

        inmsg->ParseFromArray(rec.second.data(), rec.second.size());
    }
    if (m_dotrace)
        cerr << "Record: " << m_nrecs << endl
             << endl
             << inmsg->DebugString() << endl;

    m_root->propagate_message(inmsg.get(), 0, 0);

    return true;
}

} // end namespace protobuf_schema_walker
