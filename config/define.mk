GENDIR =	GENSRC
OBJDIR =	OBJDIR

SYSNAME = $(subst /,_,$(shell uname -s))

BLTGENSRC =	\
			$(GENHDR:%=$(GENDIR)/%) \
			$(GENSRC:%=$(GENDIR)/%) \
			$(NULL)

BLTGENOBJ =	$(GENSRC:%.cpp=$(OBJDIR)/%.o)

DEPSRC =	$(GENSRC:%=$(GENDIR)/%) $(LIBSRC) $(PRGSRC)
BLTDEP0 =	$(DEPSRC:$(GENDIR)/%.cpp=$(OBJDIR)/%.d)
BLTDEP =	$(BLTDEP0:%.cpp=$(OBJDIR)/%.d)

BLTLIBA =	$(LIBA:%=$(OBJDIR)/%.a)
BLTLIBOBJ =	$(LIBSRC:%.cpp=$(OBJDIR)/%.o)

BLTPRGOBJ =	$(PRGSRC:%.cpp=$(OBJDIR)/%.o)
BLTPRGEXE =	$(PRGEXE:%=$(OBJDIR)/%)

BLTPBHDR = $(PROTO:%.proto=$(GENDIR)/%.pb.h)
BLTPBSRC = $(PROTO:%.proto=$(GENDIR)/%.pb.cpp)
BLTPBOBJ = $(BLTPBSRC:$(GENDIR)/%.cpp=$(OBJDIR)/%.o)

BLTPBGEN = $(BLTPBHDR) $(BLTPBSRC)

GENHDRSRC +=	$(BLTPBHDR) $(BLTPBSRC)

BLTDEP +=		$(BLTPBSRC:$(GENDIR)/%.cpp=$(OBJDIR)/%.dx)

ifneq (,$(BLTPBGEN))
CLEANFILES += $(BLTPBGEN)
CLOBBERFILES += $(BLTPBGEN)
endif

ifeq (,$(filter-out Linux, $(SYSNAME)))
PROTOC = protoc
LIBS += -lprotobuf
endif

MAKE =			gmake

CPPCMD =		g++

ifeq ($(BUILD),DEBUG)
CPPFLAGS =		-g -Wall -Werror
else
CPPFLAGS =		-g -Wall -Werror -O3
endif

ifeq ($(SYSNAME), SunOS)
ARCMD =			gar
ARFLAGS =		rcs
else
ARCMD =			ar
ARFLAGS =		rcs
endif

LDCMD =			g++
LDFLAGS =

DEPFLT =		xyzzy
