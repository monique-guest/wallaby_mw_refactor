.PHONY: help all casda subfits hi4pi miriad

REGISTRY ?= images.canfar.net/srcnet
DEV_TAG ?= dev
PUSH_TAG ?= latest

casda: IMAGE_NAME=wallaby-mw-casda
casda: DOCKERFILE=containers/casda_download/Dockerfile

subfits: IMAGE_NAME=wallaby-mw-subfits
subfits: DOCKERFILE=containers/subfits/Dockerfile

hi4pi: IMAGE_NAME=wallaby-mw-hi4pi
hi4pi: DOCKERFILE=containers/hi4pi_download/Dockerfile

miriad: IMAGE_NAME=wallaby-mw-miriad-script
miriad: DOCKERFILE=containers/miriad_script/Dockerfile

help:
	@echo "Usage:"
	@echo "  make casda|subfits|hi4pi|miriad      Build, tag, push one image"
	@echo "  make casda hi4pi                      Build, tag, push selection"
	@echo "  make all                              Build, tag, push all images"
	@echo ""
	@echo "Variables:"
	@echo "  REGISTRY=images.canfar.net/srcnet"
	@echo "  DEV_TAG=dev"
	@echo "  PUSH_TAG=latest"

all: casda subfits hi4pi miriad

casda subfits hi4pi miriad:
	docker build -f $(DOCKERFILE) -t $(IMAGE_NAME):$(DEV_TAG) .
	docker tag $(IMAGE_NAME):$(DEV_TAG) $(REGISTRY)/$(IMAGE_NAME):$(PUSH_TAG)
	docker push $(REGISTRY)/$(IMAGE_NAME):$(PUSH_TAG)
