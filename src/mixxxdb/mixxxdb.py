from datetime import datetime
import shutil
import sys
import mimetypes
from pathlib import Path
from typing import List
from .schemas import TrackLocation


def backup_db(db_filename: str) -> Path:
    db_path = Path(db_filename)
    timestr = datetime.now().strftime("%Y%m%d-%H%M%S")
    destpath = db_path.with_stem(db_path.stem + "-" + timestr)
    if destpath.exists():
        destpath.unlink()
    shutil.copyfile(db_path, destpath)
    return destpath


def move_files(db_filename: str, sources: List[str], destination: str):
    locations = TrackLocation.list(db_filename)
    destdir = Path(destination).resolve()

    def update_db(srcpath: Path, destpath: Path):
        db_updated = False
        # TODO: Undo any previous changes if exception comes later in loop
        for location in [l for l in locations if l.location == str(srcpath.resolve())]:
            location.location = str(destpath)
            location.directory = str(destpath.parent)
            try:
                location.update(db_filename)
            except Exception as e:
                raise e
            db_updated = True
        return db_updated

    if not destdir.is_dir():
        raise ValueError("`destination` must be an existing directory.")

    for source in sources:
        srcpath = Path(source)
        if srcpath.is_file():
            destpath = destdir.joinpath(srcpath.name)
            try:
                srcpath.rename(destpath)
            except Exception as e:
                sys.stderr.write("Error when renaming %s to %s: %s\n" % (str(srcpath), str(destpath), str(e)))
                continue
            try:
                db_updated = update_db(srcpath, destpath)
            except Exception as e:
                sys.stderr.write("Error when updating DB for %s: %s. Undoing file move.\n" % (str(srcpath), str(e)))
                destpath.rename(srcpath)
                continue
            if db_updated:
                sys.stdout.write("%s moved to %s; DB updated.\n" % (str(srcpath), str(destpath)))
            else:
                sys.stdout.write("%s moved to %s; no DB entry found.\n" % (str(srcpath), str(destpath)))


def list_orphan_files(db_filename: str, source: str, recursive: bool):
    if recursive:
        paths = Path(source).glob("**/*")
    else:
        paths = Path(source).glob("*")

    locations = [l.location for l in TrackLocation.list(db_filename)]

    for path in paths:
        if path.is_file():
            mimetype = mimetypes.guess_type(path)
            if mimetype[0] and mimetype[0].startswith("audio/"):
                if str(path.resolve()) not in locations:
                    sys.stdout.write(str(path) + "\n")


def list_orphan_locations(db_filename: str):
    for location in TrackLocation.list_orphans(db_filename):
        sys.stdout.write(location.location + "\n")


def delete_orphan_locations(db_filename: str):
    backup_path = backup_db(db_filename)
    for location in TrackLocation.list_orphans(db_filename):
        location.delete(db_filename)
        sys.stdout.write(location.location + " -- DELETED\n")
    sys.stdout.write("\nDB backed up to: %s\n" % (str(backup_path),))
