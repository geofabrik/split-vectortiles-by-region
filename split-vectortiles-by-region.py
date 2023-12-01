#! /usr/bin/env python3

import argparse
import json
import logging
import multiprocessing
import os
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import yaml


logger = None


def run_cmd(args, i, shell=False, cwd=None, env=None, check_output=False):
    args_for_print = ""
    if type(args) is list:
        args_for_print = " ".join(args)
    else:
        args_for_print = args
    logger.debug("{}: {}".format(i, args_for_print))
    if check_output:
        #TODO handle return code != 0
        return subprocess.check_output(args, shell=shell, cwd=cwd, env=env)
    r = subprocess.run(args, shell=shell, cwd=cwd, env=env)
    if r.returncode != 0:
        logger.error("{}: Command returned with code {}. Command was:\n{}".format(i, r.returncode, args_for_print))
        r.check_returncode()


def merge_bounding_boxes(bbox1, bbox2):
    return [
        min(bbox1[0], bbox2[0]),
        min(bbox1[1], bbox2[1]),
        max(bbox1[2], bbox2[2]),
        max(bbox1[3], bbox2[3]),
    ]


def add_to_bbox(bbox, x, y):
    return [
        min(bbox[0], x),
        min(bbox[1], y),
        max(bbox[2], x),
        max(bbox[3], y),
    ]


def bbox_of_ring(ring):
    bbox = [181.0, 91.0, -181.0, -91.0]
    for elem in ring:
        if type(elem) is list and type(elem[0]) is float:
            bbox = add_to_bbox(bbox, elem[0], elem[1])
        elif type(elem) is list:
            bbox = merge_bounding_boxes(bbox, bbox_of_ring(elem))
    return bbox


def get_bbox(geometry):
    """Get bounding box from geometry.
    """
    return bbox_of_ring(geometry["coordinates"])


def write_metadata_json(input_dir, geometry):
    """Write updated metadata.json file. This function alters the bounding box and adds data source and attribution.
    """
    metadata = {}
    with open(os.path.join(input_dir, "metadata.json"), "r") as infile:
        metadata = json.load(infile)
    metadata["name"] = "Shortbread"
    metadata["attribution"] = "OpenStreetMap contributors (ODbL)"
    metadata["bounds"] = get_bbox(geometry)
    path = None
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmpfile:
        json.dump(metadata, tmpfile)
        path = tmpfile.name
    return path


def write_geojson_feature(feature):
    """Write GeoJSON feature to a named temporary file and return its path.
    """
    geojson_feature_collection = {
        "type": "FeatureCollection",
        "features": [feature],
    }
    path = None
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmpfile:
        json.dump(geojson_feature_collection, tmpfile)
        path = tmpfile.name
    return path


def remove_leading_slash(path):
    if path and path[0] == "/":
        return path[1:]
    return path


def delete_if_exists(path):
    if not path:
        return
    try:
        os.remove(path)
    except OSError:
        pass


def convert_tile_list(tile_list):
    """Convert a list of tile IDs in z/x/y format into a list of tuples of ints with the following strcuture: (zoom_level, tile_column, tile_row)

    The returned list uses the tiling schema of MBTiles (Y axis pointing to the north).
    """
    result = []
    #TODO handle encoding
    for t in tile_list.decode("utf-8").split("\n"):
        if t == "":
            continue
        parts = [int(p) for p in t.split("/")]
        zoom = parts[0]
        y_corrected = (2 ** zoom - 1) - parts[2]
        result.append((zoom, parts[1], y_corrected,))
    return result


def create_mbtiles(input_path, output_path, tile_list):
    """Create an MBTiles by copying tiles and metadata from an existing database.
    """
    if os.path.isfile(output_path):
        logger.warning("Deleting and rewriting {}".format(output_path))
        os.remove(output_path)
    exit_prog = False
    conn = None
    cur = None
    try:
        conn = sqlite3.connect(output_path)
        cur = conn.cursor()
        #TODO escape filename
        logger.debug("setting pragmas")
        cur.execute("PRAGMA auto_vacuum = 0;")
        cur.execute("PRAGMA cache_size = -1048576;") # 2 GiB cache
        cur.execute("PRAGMA journal_mode = OFF;")
        cur.execute("PRAGMA synchronous = OFF;")
        conn.commit()
        cur.execute("ATTACH DATABASE '{}' AS source;".format(input_path))
        logger.debug("filling tiles table")
        cur.execute("CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")
        for tile in tile_list:
            cur.execute("INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) SELECT zoom_level, tile_column, tile_row, tile_data FROM source.tiles WHERE zoom_level = %s AND tile_column = %s AND tile_row = %s" % tile)
        conn.commit()
        logger.debug("building index")
        cur.execute("CREATE UNIQUE INDEX tile_index on tiles (zoom_level, tile_column, tile_row);")
        conn.commit()
        logger.debug("creating metadata table")
        cur.execute("CREATE TABLE metadata AS SELECT name, value FROM source.metadata;")
        #TODO update bounds
        conn.commit()
        logger.debug("detach source db")
        cur.execute("DETACH DATABASE source;")
        conn.commit()
    except Exception as e:
        logger.exception(e)
        exit_prog = True
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    if exit_prog:
        sys.exit(1)


def create_tileset_mbtiles(i, polygon_to_tile_list, input_path, output_base_dir, output_path, geojson_feature, minzoom, maxzoom, suffix, shortbread_version):
    # Write GeoJSON feature to temporary file
    error = False
    geojson_path = None
    try:
        geojson_path = write_geojson_feature(geojson_feature)
        output_filename = "{}-shortbread-{}.mbtiles".format(os.path.join(output_base_dir, output_path), shortbread_version)
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        logger.info("{}: Creating tile set {}".format(i, output_path))
        args = [polygon_to_tile_list, "-g", geojson_path, "-z", str(minzoom), "-Z", str(maxzoom)]
        cwd = os.path.dirname(input_path)
        if cwd == "":
            cwd = "."
        tile_list = run_cmd(args, i, False, cwd, {"OGR_ENABLE_PARTIAL_REPROJECTION": "TRUE"}, True)
        tile_list = convert_tile_list(tile_list)
        create_mbtiles(input_path, output_filename, tile_list)
    except subprocess.CalledProcessError:
        error = True
    finally:
        delete_if_exists(geojson_path)
    if error:
        exit(1)
    logger.info("{}: Completed tile set {}".format(i, output_path))
    return True


def create_tileset_targz(i, polygon_to_tile_list, input_dir, output_base_dir, output_path, geojson_feature, minzoom, maxzoom, suffix, shortbread_version):
    # Write GeoJSON feature to temporary file
    error = False
    geojson_path = None
    try:
        geojson_path = write_geojson_feature(geojson_feature)
        metadata_path = write_metadata_json(input_dir, geojson_feature["geometry"])
        output_filename = "{}-shortbread-{}.tar.gz".format(os.path.join(output_base_dir, output_path), shortbread_version)
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        opts = {
            "polygon_to_tile_list": shlex.quote(os.path.abspath(polygon_to_tile_list)),
            "geojson_path": shlex.quote(geojson_path),
            "input_dir": shlex.quote(input_dir),
            "metadata_path": shlex.quote(metadata_path),
            "metadata_path_for_regex": shlex.quote(remove_leading_slash(metadata_path)),
            "output_filename": shlex.quote(output_filename),
            "minzoom": shlex.quote(str(minzoom)),
            "maxzoom": shlex.quote(str(maxzoom)),
            "suffix": shlex.quote(suffix),
        }
        logger.info("{}: Creating tile set {}".format(i, output_path))
        args = "{polygon_to_tile_list} -c -n -a {metadata_path} -g {geojson_path} -z {minzoom} -Z {maxzoom} -s {suffix} | tar --null -c --owner=0 --group=0 --transform='flags=r;s|{metadata_path_for_regex}|metadata.json|' --files-from=- | gzip -1 > {output_filename}".format(**opts)
        run_cmd(args, i, True, input_dir, {"OGR_ENABLE_PARTIAL_REPROJECTION": "TRUE"})
    except subprocess.CalledProcessError:
        error = True
    finally:
        delete_if_exists(geojson_path)
        delete_if_exists(metadata_path)
    if error:
        exit(1)
    logger.info("{}: Completed tile set {}".format(i, output_path))
    return True


def find_region(geojson_data, region_id):
    """Return GeoJSON feature matching the provided region_id or None if not found."""
    for feature in geojson_data["features"]:
        if feature.get("properties", {}).get("id") == region_id:
            return feature
    return None


parser = argparse.ArgumentParser(description="Split a vector tile set (in z/x/y.pbf structure) into multiple tile sets.")
parser.add_argument("-c", "--config", type=argparse.FileType("r"), required=True, help="Configuration file in YAML format")
parser.add_argument("-f", "--output-format", type=str, required=True, help="Output format ('mbtiles' or 'tar.gz')")
parser.add_argument("-g", "--geojson", type=argparse.FileType("r"), required=True, help="GeoJSON index file containing clipping polygons for all regions.")
parser.add_argument("-i", "--input", type=str, required=True, help="Directory to load shape files from")
parser.add_argument("-l", "--log-level", help="log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)", default="INFO", type=str)
parser.add_argument("-o", "--output", type=str, required=True, help="Output base directory")
parser.add_argument("-p", "--processes", type=int, default=8, help="Number of parallel processes")
parser.add_argument("-s", "--suffix", type=str, default=".pbf", help="File name suffix, should start with a dot. Ignore if output is written in MBTiles format.")
parser.add_argument("-S", "--strict", action="store_true", help="Fail if a requested region is not found")
parser.add_argument("-t", "--tilelist", type=str, required=True, help="Path to program to generate a list of tiles from polygon")
parser.add_argument("-z", "--minzoom", type=int, help="Minimum zoom level", default=0)
parser.add_argument("-Z", "--maxzoom", type=int, help="Maximium zoom level", default=14)
args = parser.parse_args()

numeric_log_level = getattr(logging, args.log_level.upper())
if not isinstance(numeric_log_level, int):
    raise ValueError("Invalid log level {}".format(args.log_level.upper()))
logging.basicConfig(level=numeric_log_level, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger()

if not os.path.isfile(args.tilelist):
    logging.error("{} is not a file.".format(args.tilelist))
    exit(1)

if args.minzoom < 0 or args.maxzoom > 20 or args.minzoom > args.maxzoom:
    logging.error("Zoom levels out of range or swapped")
    exit(1)

output_format = args.output_format.lower()
if output_format not in ["mbtiles", "tar.gz"]:
    logging.error("Output format must be either 'mbtiles' or 'tar.gz'")
    exit(1)

for d in [args.input, args.output]:
    if not os.path.isdir(d) and (d != args.input or args.output_format == "tar.gz"):
        logging.error("{} is not a directory.".format(d))
        exit(1)

if args.suffix is not None and not args.suffix.startswith("."):
    logging.warning("File name suffix does not start with a dot.")

logging.debug("Loading configuration")
config = yaml.safe_load(args.config)
shortbread_version = config["shortbread_version"]
requested_regions = config["polygons"]
if type(requested_regions) is not list or len(requested_regions) == 0:
    logging.error("Configuration is invalid, 'polyogns' not found or an empty list.")
    exit(1)

logging.debug("Loading clipping polygons")
geojson_data = json.load(args.geojson)
if len(geojson_data.get("features", [])) == 0:
    logging.error("GeoJSON is empty or invalid")
    exit(1)

tasks = []
i = 1
for polygon in requested_regions:
    polygon_id = polygon["id"]
    geojson_feature = find_region(geojson_data, polygon_id)
    if not geojson_feature:
        msg = "Region {} not found among GeoJSON features."
        if args.strict:
            logger.error(msg)
            exit(1)
        else:
            logger.warning(msg)
    polygon_path = polygon["path"]
    tasks.append((i, args.tilelist, args.input, args.output, polygon_path, geojson_feature, args.minzoom, args.maxzoom, args.suffix, shortbread_version))
    i += 1

logger.info("Processing {} regions".format(len(tasks)))
pool = multiprocessing.Pool(processes=args.processes)
if output_format == "tar.gz":
    result = [ pool.apply_async(create_tileset_targz, args=t) for t in tasks ]
elif output_format == "mbtiles":
    result = [ pool.apply_async(create_tileset_mbtiles, args=t) for t in tasks ]
output = [p.get() for p in result]
