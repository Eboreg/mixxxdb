from pathlib import Path
import argparse
from .mixxxdb import move_files, list_orphan_files, list_orphan_locations, delete_orphan_locations


def get_parser():
    default_db_path = str(Path().home().joinpath(".mixxx/mixxxdb.sqlite"))
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db",
        help="path to Mixxx database (default: %s)" % (default_db_path,),
        default=default_db_path
    )
    return parser


def mv():
    parser = get_parser()
    parser.add_argument("source", help="source of song(s) to move; wildcards are allowed", nargs="*")
    parser.add_argument("dest", help="destination; must be an existing directory")
    args = parser.parse_args()
    move_files(args.db, args.source, args.dest)


def orphanfiles():
    parser = get_parser()
    parser.add_argument("source", help="root path to check for orphan files")
    parser.add_argument("--recursive", "-r", action="store_true", help="do recursive check")
    args = parser.parse_args()
    list_orphan_files(args.db, args.source, args.recursive)


def orphanlocations():
    parser = get_parser()
    parser.add_argument("--delete", "-d", help="also delete the orphan locations from the database")
    args = parser.parse_args()
    if args.delete:
        delete_orphan_locations(args.db)
    else:
        list_orphan_locations(args.db)
