ROOTDIR = .

include $(ROOTDIR)/config/define.mk

SUBDIRS =	\
			parquetformat \
			parquetfile \
			proto2parq \
			$(NULL)

include $(ROOTDIR)/config/depend.mk
