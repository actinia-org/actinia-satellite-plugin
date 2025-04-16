FROM mundialis/actinia:latest as actinia_test

LABEL authors="Carmen Tawalika,Anika Weinmann"
LABEL maintainer="tawalika@mundialis.de,weinmann@mundialis.de"

ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
ENV ACTINIA_CUSTOM_TEST_CFG /etc/default/actinia_test
# TODO do not set DEFAULT_CONFIG_PATH if this is fixed
ENV DEFAULT_CONFIG_PATH /etc/default/actinia_test

# install things only for tests
RUN apk add valkey valkey-cli
RUN pip3 install iniconfig colorlog pytest pytest-cov

# # add data for tests
# RUN wget --quiet https://grass.osgeo.org/sampledata/north_carolina/nc_spm_08_micro.zip && \
#   unzip nc_spm_08_micro.zip && \
#   rm -f nc_spm_08_micro.zip && \
#   mv nc_spm_08_micro /actinia_core/grassdb/nc_spm_08
# RUN grass -e -c 'EPSG:4326' /actinia_core/grassdb/latlong_wgs84
# RUN wget --quiet https://grass.osgeo.org/sampledata/north_carolina/nc_spm_mapset_modis2015_2016_lst_grass8.zip && \
#   unzip nc_spm_mapset_modis2015_2016_lst_grass8.zip && \
#   rm -f nc_spm_mapset_modis2015_2016_lst_grass8.zip && \
#   mv modis_lst /actinia_core/grassdb/nc_spm_08/modis_lst
# RUN chown -R 1001:1001 /actinia_core/grassdb/nc_spm_08/modis_lst && chmod -R g+w /actinia_core/grassdb/nc_spm_08/modis_lst

# copy needed files and configs for test
COPY docker/actinia-plugin-tests/actinia-test.cfg /etc/default/actinia
COPY docker/actinia-plugin-tests/actinia-test.cfg /etc/default/actinia_test

# TODO: Postgres for tests
# using tests/data/poly.gpkg

RUN pip3 uninstall actinia_satellite_plugin -y
COPY . /src/actinia_satellite_plugin
WORKDIR /src/actinia_satellite_plugin
RUN chmod a+x tests_with_kvdb.sh

RUN make install

# RUN make test
