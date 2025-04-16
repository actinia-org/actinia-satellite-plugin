# Actinia Satellite Plugin

This actinia plugin is designed for efficient satellite data handling, especially Landsat and Sentinel-2 scenes.
It implements endpoints for on-the-fly NDVI computation and time series import for Landsat and Sentinel-2
scenes in the actinia database.

Note:

    Actinia[1] is an open source REST API for scalable, distributed, high performance
    processing of geographical data that uses GRASS GIS for computational tasks.

    The Actinia service consists of the *Actinia Core* that provides the basic but sophisticated processing service
    and *Actinia plugins* that provide problem specific services like Sentinel-2 and Landsat NDVI computation,
    spatio-temporal statistical analysis and many more.

    [1] https://github.com/mundialis/actinia_core


## Installation

The actinia plugin must be installed in the same environment as actinia core.
Actinia core must be configured to load the installed plugin. When the plugin is
loaded and all plugin endpoints are available in actinia, the interface description
of actinia will be extended with the endpoints of the plugins.

    .. code-block:: bash

        git clone https://github.com/mundialis/actinia_satellite_plugin.git

        cd actinia_satellite_plugin
        pip3 install .

    ..

After installation set the plugin name in the actinia core configuration
and restart the actinia core server.


## Testing locally

```
docker build -f docker/actinia-plugin-tests/Dockerfile -t actinia-plugin-test .
docker run -v </path/to/actinia_satellite_plugin/tests>:/src/actinia_satellite_plugin/tests -it actinia-plugin-test:latest -i

# run tests with
make test
```
