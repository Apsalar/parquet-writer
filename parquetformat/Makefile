ROOTDIR =	..

include $(ROOTDIR)/config/define.mk

THRIFTSRC = $(ROOTDIR)/modules/parquet-format/src/main/thrift/parquet.thrift

LIBSO = 	libparquetformat

GENHDR =	\
			parquet_types.h \
			parquet_constants.h \
			$(NULL)

GENSRC =	\
			parquet_types.cpp \
			parquet_constants.cpp \
			$(NULL)

ALLTRG =	$(BLTLIBSO)

CLEANFILES = $(BLTGENSRC) $(BLTLIBOBJ) $(BLTGENOBJ) $(BLTLIBSO) $(BLTDEP)

include $(ROOTDIR)/config/depend.mk
