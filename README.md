# Split Vectortiles by Region

This Python script allows you to split a set of vectortiles (or any other type of tiles, but not metatiles) into
multiple smaller packages definedb by polygons. A GeoJSON file of (multi)polygon features decides which tile should go into which
package.

GeoJSON features may overlap.

Supported formats (must be the same for input and output):

* from a dirctory of z/x/y.suffix files to multiple GZIP compressed tarballs containing z/x/y.suffix files
* from MBTiles to multiple MBTiles

## Usage

```
split-vectortiles-by-region.py [-h] -c CONFIG -f FORMAT -g GEOJSON -i INPUT [-l LOG_LEVEL] -o OUTPUT [-p PROCESSES] [-s SUFFIX] [-S] -t TILELIST [-z MINZOOM] [-Z MAXZOOM]

Split a vector tile set (in z/x/y.pbf structure) into multiple tile sets.

Required arguments:

  -c CONFIG, --config CONFIG
                        Configuration file in YAML format (required)
  -f FORMAT, --format FORMAT
                        Input and output format ('mbtiles' for MBTiles input and output, 'tar.gz' for input from directory and output to .tar.gz)
  -g GEOJSON, --geojson GEOJSON
                        GeoJSON index file containing clipping polygons for all regions.
  -i INPUT, --input INPUT
                        Directory to load shape files from
  -o OUTPUT, --output OUTPUT
                        Output base directory
  -t TILELIST, --tilelist TILELIST
                        Path to program to generate a list of tiles from polygon

Optional arguments:
  -h, --help            show this help message and exit
  -l LOG_LEVEL, --log-level LOG_LEVEL
                        log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  -p PROCESSES, --processes PROCESSES
                        Number of parallel processes
  -s SUFFIX, --suffix SUFFIX
                        File name suffix, should start with a dot. Ignore if output is written in MBTiles format.
  -S, --strict          Fail if a requested region is not found
  -z MINZOOM, --minzoom MINZOOM
                        Minimum zoom level
  -Z MAXZOOM, --maxzoom MAXZOOM
                        Maximium zoom level
```

## Configuration

split-vectortiles-by-region is configured using a YAML and a GeoJSON file.

### YAML file

Example:

```yaml
path_template: "{region_path}-shortbread-1.0"

polygons:
  - id: "afghanistan"
    region_path: "asia/afghanistan"
  - id: "albania"
    region_path: "europe/albania"
  - id: "alberta"
    region_path: "canada/alberta"
  - id: "algeria"
    region_path: "africa/algeria"
```

The YAML file must be a mapping with the following properties:

* `path_template`: A Template (as Python string template) to create the path where a tarball/MBTiles file is to be written within the selected target base directory. A file name suffix is added automatically depending on the output format.
* `polygons`: This list of mappings defines which tile packages should be created. Each entry must have the following properties:
  * `id`: unique ID of the clipping (multi)polygon; there must be a (multi)polygon feature in the GeoJSON file with its property `id` set to the same string.
  * `region_path`: path (can include subdirectories and directory separators like slashes) to be used in `path_template`. This key has to be used in the `path_template` to generate unique paths per tile set.

### GeoJSON file

The GeoJSON file holds the geometries for all clipping (multi)polygons. There must be a feature in the GeoJSON file for each tile set defined in the YAML file. GeoJSON features and tile sets are matched by the `id` property in the YAML file and the property `id` in the GeoJSON file.

Features in the GeoJSON file not used by any tile set will be ignored. Missing features in the GeoJSON file will raise an error.


### Example configuration

This repository contains an example configuration to split a world-wide set of vector tiles into regional packages.

## Requirements

* PyYAML (Debian package `python3-yaml`)
* SQLite3 bindings for Python
* [Polygon-To-Tile-List](https://github.com/Geofabrik/polygon-to-tile-list)
* Unix utilites `tar` and `gzip`
