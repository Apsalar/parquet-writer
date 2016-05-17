ROOTDIR =	..

include $(ROOTDIR)/config/define.mk

PRGEXE =	proto2parq

PRGSRC =	\
			proto2parq.cpp \
			protobuf-schema-walker.cpp \
			$(NULL)

CPPFLAGS +=	\
			-std=gnu++11 \
			-Wno-sign-compare \
			$(NULL)

INCS +=		\
			-I. \
			-I$(ROOTDIR)/parquetfile \
			-I$(ROOTDIR)/parquetformat/GENSRC \
			$(NULL)

LIBS +=		\
			-L$(ROOTDIR)/parquetfile/OBJDIR -lparquetfile \
			-L$(ROOTDIR)/parquetformat/OBJDIR -lparquetformat \
			-lprotobuf \
			-lthrift \
			-lsnappy \
			-lz \
			-L/usr/local/ssl/lib -lssl -lcrypto \
			$(NULL)

ALLTRG =	$(BLTPRGEXE)

CLEANFILES = $(BLTPRGEXE) $(BLTPRGOBJ) $(BLTDEP)

include $(ROOTDIR)/config/depend.mk

$(BLTPRGEXE):	$(ROOTDIR)/parquetformat/OBJDIR/libparquetformat.a

$(BLTPRGEXE):	$(ROOTDIR)/parquetfile/OBJDIR/libparquetfile.a
