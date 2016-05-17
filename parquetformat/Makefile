ROOTDIR =	..

include $(ROOTDIR)/config/define.mk

THRIFTSRC = $(ROOTDIR)/modules/parquet-format/src/thrift/parquet.thrift

LIBA = 		libparquetformat

GENHDR =	\
			parquet_types.h \
			parquet_constants.h \
			$(NULL)

GENSRC =	\
			parquet_types.cpp \
			parquet_constants.cpp \
			$(NULL)

ALLTRG =	$(BLTLIBA)

CLEANFILES = $(BLTGENSRC) $(BLTLIBOBJ) $(BLTGENOBJ) $(BLTLIBA) $(BLTDEP)

include $(ROOTDIR)/config/depend.mk
