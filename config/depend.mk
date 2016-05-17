all::	INIT $(ALLTRG)
		+$(LOOP_SUBDIRS)

clean::
ifdef CLEANFILES
		rm -f $(CLEANFILES)
endif
ifdef CLEANDIRS
		rm -rf $(CLEANDIRS)
endif
		+$(LOOP_SUBDIRS)

INIT::

FORCE:

# include dependencies if we are not cleaning or clobbering
ifneq ($(findstring clean,$(MAKECMDGOALS)),clean)
ifneq ($(findstring clobber,$(MAKECMDGOALS)),clobber)
-include $(BLTDEP)
endif
endif

# macro to construct needed target directories
define CHKDIR
if test ! -d $(@D); then mkdir $(@D); else true; fi
endef

# macro which recurses into SUBDIRS
ifdef SUBDIRS
LOOP_SUBDIRS = \
	@for d in $(SUBDIRS); do \
		set -e; \
		echo "cd $$d; $(MAKE) $@"; \
		$(MAKE) -C $$d $@; \
		set +e; \
	done
endif

$(BLTPRGEXE):	$(BLTPRGOBJ) $(BLTGENOBJ)
	@$(CHKDIR)
	$(LDCMD) -o $@ $(CPPFLAGS) $(DEFS) $(LDFLAGS) $(INCS) $(BLTPRGOBJ) $(BLTGENOBJ) $(LIBS)

$(BLTLIBSO):	$(BLTLIBOBJ) $(BLTGENOBJ)
	@$(CHKDIR)
	$(SOCMD) $(SOFLAGS) -o $@ $(CPPFLAGS) $(BLTLIBOBJ) $(BLTGENOBJ) $(LIBS)

$(BLTGENSRC):	$(THRIFTSRC)
	@$(CHKDIR)
	thrift -gen cpp -out $(GENDIR) $(THRIFTSRC)

$(OBJDIR)/%.o:		%.cpp
	@$(CHKDIR)
	$(CPPCMD) -c $< -o $@ $(CPPFLAGS) $(DEFS) $(INCS)

# Generated C++ objects.
$(OBJDIR)/%.o:		$(GENDIR)/%.cpp
	@$(CHKDIR)
	$(CPPCMD) -c $< -o $@ $(CPPFLAGS) $(DEFS) $(INCS)

$(OBJDIR)/%.d:		%.cpp
	@$(CHKDIR)
	@echo "Updating dependencies for $<"
	@set -e; $(CPPCMD) -MM $< $(CPPFLAGS) $(DEFS) $(INCS) | \
	egrep -v $(DEPFLT) | \
	perl -p -e 's#(\S+.o)\s*:#$(@D)/$$1 $@: #g' > $@; \
	[ -s $@ ] || rm -f $@

$(OBJDIR)/%.d:		$(GENDIR)/%.cpp
	@$(CHKDIR)
	@echo "Updating dependencies for $<"
	@set -e; $(CPPCMD) -MM $< $(CPPFLAGS) $(DEFS) $(INCS) | \
	egrep -v $(DEPFLT) | \
	perl -p -e 's#(\S+.o)\s*:#$(@D)/$$1 $@: #g' > $@; \
	[ -s $@ ] || rm -f $@

$(BLTDEP):	$(BLTGENSRC)

.PRECIOUS:		$(BLTGENSRC)
