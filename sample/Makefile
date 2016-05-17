ROOTDIR =	..

include $(ROOTDIR)/config/define.mk

PRGEXE =	sample

PROTO =		sample.proto

PRGSRC =	\
			sample.cpp \
			$(NULL)

CPPFLAGS +=	\
			-std=gnu++11 \
			$(NULL)

INCS +=		-I$(GENDIR)

LIBS +=		\
			$(NULL)

ALLTRG =	$(BLTPRGEXE)

CLEANFILES = $(BLTPRGEXE) $(BLTPRGOBJ) $(BLTDEP) $(GENHDRSRC) $(BLTPBOBJ)

include $(ROOTDIR)/config/depend.mk
