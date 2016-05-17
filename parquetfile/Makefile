ROOTDIR =	..

include $(ROOTDIR)/config/define.mk

LIBSO = 	libparquetfile

LIBSRC =	\
			compressor.cpp \
			dictionary_encoder.cpp \
			parquet_column.cpp \
			parquet_file.cpp \
			$(NULL)

CPPFLAGS +=	\
			-std=gnu++11 \
			-Wno-sign-compare \
			$(NULL)

INCS +=		-I. -I$(ROOTDIR)/parquetformat/GENSRC

ALLTRG =	$(BLTLIBSO)

CLEANFILES = $(BLTLIBOBJ) $(BLTLIBSO) $(BLTDEP)

include $(ROOTDIR)/config/depend.mk
