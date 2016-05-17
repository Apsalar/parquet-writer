ROOTDIR = .

include $(ROOTDIR)/config/define.mk

SUBDIRS =	\
			parquetformat \
			parquetfile \
			proto2parq \
			sample \
			$(NULL)

include $(ROOTDIR)/config/depend.mk
