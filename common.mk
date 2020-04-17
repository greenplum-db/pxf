SHELL := /bin/bash

# PXF_HOME is only needed for install targets
ifeq "$(PXF_HOME)" ""
	ifneq "$(GPHOME)" ""
		PXF_HOME := "$(GPHOME)/pxf"
	endif
endif
export PXF_HOME
