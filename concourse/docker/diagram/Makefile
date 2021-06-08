ifeq (, $(shell type dot))
$(error "No dot in $(PATH), install graphviz")
endif

images.svg: images.dot
	dot  -Tsvg images.dot -o images.svg
