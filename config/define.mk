GENDIR =	GENSRC
OBJDIR =	OBJDIR

BLTGENSRC =	\
			$(GENHDR:%=$(GENDIR)/%) \
			$(GENSRC:%=$(GENDIR)/%) \
			$(NULL)

BLTGENOBJ =	$(GENSRC:%.cpp=$(OBJDIR)/%.o)

DEPSRC =	$(GENSRC:%=$(GENDIR)/%) $(LIBSRC) $(PRGSRC)
BLTDEP0 =	$(DEPSRC:$(GENDIR)/%.cpp=$(OBJDIR)/%.d)
BLTDEP =	$(BLTDEP0:%.cpp=$(OBJDIR)/%.d)

BLTLIBSO =	$(LIBSO:%=$(OBJDIR)/%.so)
BLTLIBOBJ =	$(LIBSRC:%.cpp=$(OBJDIR)/%.o)

BLTPRGOBJ =	$(PRGSRC:%.cpp=$(OBJDIR)/%.o)
BLTPRGEXE =	$(PRGEXE:%=$(OBJDIR)/%)

MAKE =			gmake

CPPCMD =		g++

ifeq ($(BUILD),DEBUG)
CPPFLAGS =		-g -Wall -fPIC
else
CPPFLAGS =		-g -Wall -fPIC -Werror -O3
endif

SOCMD =			g++
SOFLAGS =		-shared

LDCMD =			g++
LDFLAGS =

DEPFLT =		xyzzy
