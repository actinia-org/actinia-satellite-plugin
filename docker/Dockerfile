FROM alpine:edge as build

RUN apk add git python3
RUN pip3 install --upgrade pip pep517 wheel

# Build
COPY . /src/actinia_satellite_plugin
WORKDIR /src/actinia_satellite_plugin
# TODO: include tests. Currently only running whith actinia_core installed
# RUN pip3 install -r requirements.txt && python3 setup.py test
RUN python3 -m pep517.build --out-dir /build . && \
    pip3 wheel -r requirements.txt -w /build


FROM alpine:edge

COPY --from=build /build/actinia*.whl /build/
COPY docker/create_release_with_asset.sh .

ARG release_url=https://api.github.com/repos/mundialis/actinia_satellite_plugin/releases
ARG tag=0.0
ARG credentials=dummy:dummy
ARG file=/build/actinia*.whl

ENV env_release_url=$release_url
ENV env_tag=$tag
ENV env_credentials=$credentials
ENV env_file=$file

RUN apk add curl jq

ENTRYPOINT ["/bin/sh"]
CMD ["./create_release_with_asset.sh"]
