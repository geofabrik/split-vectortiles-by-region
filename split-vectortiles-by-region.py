#! /usr/bin/env python3

import argparse
import json
import logging
import multiprocessing
import os
import shlex
import subprocess
import tempfile
import yaml


logger = None


def run_cmd(args, i, shell=False):
    args_for_print = ""
    if type(args) is list:
        args_for_print = " ".join(args)
    else:
        args_for_print = args
    logger.debug("{}: {}".format(i, args_for_print))
    r = subprocess.run(args, shell=shell)
    if r.returncode != 0:
        logger.error("{}: Command returned with code {}. Command was:\n{}".format(i, r.returncode, args_for_print))
        r.check_returncode()


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


def create_tileset(i, polygon_to_tile_list, input_dir, output_base_dir, output_path, geojson_feature, minzoom, maxzoom, suffix):
    # Write GeoJSON feature to temporary file
    error = False
    geojson_path = None
    try:
        geojson_path = write_geojson_feature(geojson_feature)
        output_filename = "{}-shortbread.tar.gz".format(os.path.join(output_base_dir, output_path))
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        opts = {
            "polygon_to_tile_list": shlex.quote(polygon_to_tile_list),
            "geojson_path": shlex.quote(geojson_path),
            "input_dir": shlex.quote(input_dir),
            "output_filename": shlex.quote(output_filename),
            "minzoom": shlex.quote(str(minzoom)),
            "maxzoom": shlex.quote(str(maxzoom)),
            "suffix": shlex.quote(suffix),
        }
        logger.info("{}: Creating tile set {}".format(i, output_path))
        args = "{polygon_to_tile_list} -n -a metadata.json -g {geojson_path} -z {minzoom} -Z {maxzoom} -s {suffix} | tar --null -C {input_dir} -c --files-from=- | gzip -1 > {output_filename}".format(**opts)
        run_cmd(args, i, True)
    except subprocess.CalledProcessError:
        error = True
    finally:
        if geojson_path:
            os.remove(geojson_path)
            pass
    if error:
        exit(1)
    return True


def find_region(geojson_data, region_id):
    """Return GeoJSON feature matching the provided region_id or None if not found."""
    for feature in geojson_data["features"]:
        if feature.get("properties", {}).get("id") == region_id:
            return feature
    return None


parser = argparse.ArgumentParser(description="Split a vector tile set (in z/x/y.pbf structure) into multiple tile sets.")
parser.add_argument("-c", "--config", type=argparse.FileType("r"), required=True, help="Configuration file in YAML format")
parser.add_argument("-g", "--geojson", type=argparse.FileType("r"), required=True, help="GeoJSON index file containing clipping polygons for all regions.")
parser.add_argument("-i", "--input", type=str, required=True, help="Directory to load shape files from")
parser.add_argument("-l", "--log-level", help="log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)", default="INFO", type=str)
parser.add_argument("-o", "--output", type=str, required=True, help="Output base directory")
parser.add_argument("-p", "--processes", type=int, default=8, help="Number of parallel processes")
parser.add_argument("-s", "--suffix", type=str, default=".pbf", help="File name suffix, should start with a dot.")
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

for d in [args.input, args.output]:
    if not os.path.isdir(d):
        logging.error("{} is not a directory.".format(d))
        exit(1)

if args.suffix is not None and not args.suffix.startswith("."):
    logging.warning("File name suffix does not start with a dot.")

logging.debug("Loading configuration")
config = yaml.safe_load(args.config)
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
    tasks.append((i, args.tilelist, args.input, args.output, polygon_path, geojson_feature, args.minzoom, args.maxzoom, args.suffix))
    i += 1

logger.info("Processing {} regions".format(len(tasks)))
pool = multiprocessing.Pool(processes=args.processes)
result = [ pool.apply_async(create_tileset, args=t) for t in tasks ]
output = [p.get() for p in result]