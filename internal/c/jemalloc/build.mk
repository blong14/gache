.PHONY: clean-jemalloc

VERSION := 5.3.0
DIR 	:= $(PWD)/internal/c/jemalloc
SRC		:= ${DIR}/jemalloc-${VERSION}

${DIR}/.deps/setup:
	@echo ${DIR}
	@mkdir ${DIR}/.deps
	@touch $@

${DIR}/.deps: ${DIR}/.deps/setup
	@wget -P ${DIR} https://github.com/jemalloc/jemalloc/releases/download/${VERSION}/jemalloc-${VERSION}.tar.bz2
	@tar -xf ${DIR}/jemalloc-${VERSION}.tar.bz2 -C ${DIR}
	@cd ${SRC} && ./configure --with-jemalloc-prefix='je_' --with-malloc-conf='background_thread:true,metadata_thp:auto'
	@touch $@

clean-jemalloc:
	@rm -rf ${SRC}
	@rm -rf ${DIR}/.deps
	@rm ${DIR}/jemalloc-${VERSION}.tar.bz2 || true
	@rm build-jemalloc

build-jemalloc: ${DIR}/.deps
	@make -C ${SRC}
	@sudo make -C ${SRC} install
	@touch build-jemalloc
