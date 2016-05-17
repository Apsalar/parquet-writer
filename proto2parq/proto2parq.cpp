//
// Convert protobuf files to parquet
//
// Copyright (c) 2015, 2016 Apsalar Inc. All rights reserved.
//

#include <getopt.h>

#include <iostream>
#include <stdexcept>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/stubs/common.h>

#include "protobuf-schema-walker.h"

using namespace std;

using namespace google::protobuf;

using namespace protobuf_schema_walker;

namespace {

char const * DEF_PROTODIR = "./";
char const * DEF_PROTOFILE = "";
char const * DEF_ROOTMSG = "";
char const * DEF_INFILE = "-";
char const * DEF_OUTFILE = "";
double const DEF_ROWGRPMB = 256.0;

string g_protodir = DEF_PROTODIR;
string g_protofile = DEF_PROTOFILE;
string g_rootmsg = DEF_ROOTMSG;
string g_infile = DEF_INFILE;
string g_outfile = DEF_OUTFILE;
double g_rowgrpmb = DEF_ROWGRPMB;
bool g_dodump = false;    
bool g_dotrace = false;    
    
void
usage(int & argc, char ** & argv)
{
    cerr << "usage: " << argv[0] << " [options] [-p <protofile> -m <rootmsg>] -o <outfile> [-i <infile>]" << endl
         << "  options:" << endl
         << "    -h, --help            display usage" << endl
         << "    -d, --protodir=DIR    protobuf src dir    [" << DEF_PROTODIR << "]" << endl
         << "    -p, --protofile=FILE  protobuf src file   [" << DEF_PROTOFILE << "]" << endl
         << "    -m, --rootmsg=MSG     root message name   [" << DEF_ROOTMSG << "]" << endl
         << "    -i, --infile=PATH     protobuf data input [" << DEF_INFILE << "]" << endl
         << "    -o, --outfile=PATH    parquet output file [" << DEF_OUTFILE << "]" << endl
         << "    -s, --row-group-mb=MB row group size (MB) [" << DEF_ROWGRPMB << "]" << endl
         << "    -u, --dump            pretty print the schema to stderr" << endl
         << "    -t, --trace           trace input traversal" << endl
        ;
}

void
parse_arguments(int & argc, char ** & argv)
{
    char * endp;

    static struct option long_options[] =
        {
	  {(char *) "usage",                   no_argument,        0, 'h'},
	  {(char *) "protodir",                required_argument,  0, 'd'},
	  {(char *) "protofile",               required_argument,  0, 'p'},
	  {(char *) "rootmsg",                 required_argument,  0, 'm'},
	  {(char *) "infile",                  required_argument,  0, 'i'},
	  {(char *) "outfile",                 required_argument,  0, 'o'},
	  {(char *) "row-group-mb",            required_argument,  0, 's'},
	  {(char *) "dump",                    no_argument,        0, 'u'},
	  {(char *) "trace",                   no_argument,        0, 't'},
	  {0, 0, 0, 0}
        };

    while (true)
    {
        int optndx = 0;
        int opt = getopt_long(argc, argv, "hd:p:m:i:o:s:ut",
                              long_options, &optndx);

        // Are we done processing arguments?
        if (opt == -1)
            break;

        switch (opt) {
        case 'h':
            usage(argc, argv);
            exit(0);
            break;

        case 'd':
            g_protodir = optarg;
            break;

        case 'p':
            g_protofile = optarg;
            break;

        case 'm':
            g_rootmsg = optarg;
            break;

        case 'i':
            g_infile = optarg;
            break;

        case 'o':
            g_outfile = optarg;
            break;

        case 'u':
            g_dodump = true;
            break;

        case 's':
            g_rowgrpmb = strtod(optarg, &endp);
            if (*endp != '\0') {
                cerr << "trouble parsing row-group-mb argument" << endl;
                exit(1);
            }
            break;

        case 't':
            g_dotrace = true;
            break;

        case'?':
            // getopt_long already printed an error message
            usage(argc, argv);
            exit(1);
            break;

        default:
            cerr << "unexpected option: " << char(opt) << endl;
            usage(argc, argv);
            exit(1);
            break;
        }
    }

    if (g_outfile.empty()) {
        cerr << "missing outfile argument" << endl;
        usage(argc, argv);
        exit(1);
    }
}

    
int run(int & argc, char ** & argv)
{
    parse_arguments(argc, argv);

    size_t rowgrpsz = size_t(g_rowgrpmb * 1024 * 1024);
    
    Schema schema(g_protodir,
                  g_protofile,
                  g_rootmsg,
                  g_infile,
                  g_outfile,
                  rowgrpsz,
                  g_dotrace);

    if (g_dodump)
        schema.dump(cerr);

    schema.convert();

    ShutdownProtobufLibrary();
    
    return 0;
}
    
}  // end namespace

int
main(int argc, char ** argv)
{
    try
    {
        return run(argc, argv);
    }
    catch (exception const & ex)
    {
        cerr << "EXCEPTION: " << ex.what() << endl;
        return 1;
    }
}
