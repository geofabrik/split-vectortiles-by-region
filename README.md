# Split Vectortiles by Region

This Python script allows you to split a set of vectortiles (or any other type of tiles, but not metatiles) into
multiple GZIP-compressed tarballs. A GeoJSON file of (multi)polygon features decides which tile should go into which
tarball.

GeoJSON features may overlap.

See the help feature of the script for details about its usage.

## Requirements

* PyYAML (Debian package `python3-yaml`)
* [Polygon-To-Tile-List](https://gitea.geofabrik.de/Geofabrik/polygon-to-tile-list)
