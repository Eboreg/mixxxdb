from contextlib import contextmanager
import os
import sqlite3
from typing import Any, Dict, List, Literal, Optional, Sequence, TypeVar, Type

_Self = TypeVar("_Self", bound="BaseSchema")


@contextmanager
def db_connection(db_filename: str):
    try:
        conn = sqlite3.connect(db_filename)
        yield conn
    finally:
        conn.close()


def db_fetchall(db_filename: str, table: str, columns: Sequence[str]) -> List[Dict[str, Any]]:
    with db_connection(db_filename) as conn:
        try:
            sql = "SELECT %s FROM %s" % (", ".join(columns), table)
            cur = conn.execute(sql)
            lst = cur.fetchall()
            return [{columns[idx]: row[idx] for idx in range(len(columns))} for row in lst]
        finally:
            cur.close()


class BaseSchema:
    columns: Sequence[str]
    id_column: Optional[str] = None
    table: str

    def __init__(self, **values):
        for key, value in values.items():
            setattr(self, key, value)

    def __repr__(self):
        return "<%s [%s]>" % (
            self.__class__.__name__,
            ", ".join(["%s=%s" % (column, getattr(self, column)) for column in self.columns])
        )

    def update(self, db_filename: str):
        if self.id_column is None:
            raise ValueError("Cannot do update() on %s; id_column is None" % (self.__class__.__name__,))
        updates = ["%s='%s'" % (column, getattr(self, column)) for column in self.columns if column != self.id_column]
        sql = "UPDATE %s SET %s WHERE %s='%s'" % (
            self.table,
            ", ".join(updates),
            self.id_column,
            getattr(self, self.id_column)
        )
        with db_connection(db_filename) as conn:
            conn.execute(sql)

    def delete(self, db_filename: str):
        if self.id_column is None:
            raise ValueError("Cannot do delete() on %s; id_column is None" % (self.__class__.__name__,))
        sql = "DELETE FROM %s WHERE %s='%s'" % (self.table, self.id_column, getattr(self, self.id_column))
        with db_connection(db_filename) as conn:
            conn.execute(sql)

    @classmethod
    def list(cls: Type[_Self], db_filename: str) -> List[_Self]:
        lst = db_fetchall(db_filename, cls.table, cls.columns)
        return [cls(**row) for row in lst]

    @classmethod
    def bulk_delete(cls: Type[_Self], db_filename: str, *objs: _Self):
        if cls.id_column is None:
            raise ValueError("Cannot do delete() on %s; id_column is None" % (cls.__name__,))
        with db_connection(db_filename) as conn:
            conn.execute(
                "DELETE FROM %s WHERE %s IN (%s)" % (
                    cls.table,
                    cls.id_column,
                    ", ".join([str(getattr(obj, cls.id_column)) for obj in objs])
                )
            )


class HasTrackForeignKey(BaseSchema):
    @classmethod
    def delete_by_track_id(cls, db_filename: str, *track_ids: int):
        with db_connection(db_filename) as conn:
            conn.execute(
                "DELETE FROM %s WHERE track_id IN (%s)" % (cls.table, ", ".join([str(t) for t in track_ids]))
            )


class TrackLocation(BaseSchema):
    columns = ("id", "location", "filename", "directory", "filesize", "fs_deleted", "needs_verification")
    table = "track_locations"
    id_column = "id"

    id: int
    location: str
    filename: str
    directory: str
    filesize: int
    fs_deleted: Literal[0, 1]
    needs_verification: Literal[0, 1]

    @classmethod
    def list_orphans(cls, db_filename: str) -> "List[TrackLocation]":
        orphans = []
        locations = cls.list(db_filename)

        # Probably faster to do one directory at a time:
        dirs = {}
        for dirname in set([os.path.dirname(l.location) for l in locations]):
            try:
                dirs[dirname] = os.listdir(dirname)
            except FileNotFoundError:
                dirs[dirname] = []

        for location in locations:
            dirname = os.path.dirname(location.location)
            if location.filename not in dirs[dirname]:
                orphans.append(location)

        return orphans

    @classmethod
    def bulk_delete(cls, db_filename: str, *objs: "TrackLocation"):
        tracks = Track.list_by_locations(db_filename, *objs)
        Track.bulk_delete(db_filename, *tracks)
        return super().bulk_delete(db_filename, *objs)


class PlaylistTrack(HasTrackForeignKey):
    columns = ("id", "playlist_id", "track_id", "position", "pl_datetime_added")
    id_column = "id"
    table = "PlaylistTracks"


class CrateTrack(HasTrackForeignKey):
    columns = ("crate_id", "track_id")
    table = "crate_tracks"


class Cue(HasTrackForeignKey):
    columns = ("id", "track_id", "type", "position", "length", "hotcue", "label", "color")
    id_column = "id"
    table = "cues"


class TrackAnalysis(HasTrackForeignKey):
    columns = (
        "id",
        "track_id",
        "type",
        "description",
        "version",
        "created",
        "data_checksum",
    )
    id_column = "id"
    table = "track_analysis"


class Track(BaseSchema):
    columns = (
        "id",
        "artist",
        "title",
        "album",
        "year",
        "genre",
        "tracknumber",
        "location",
        "comment",
        "url",
        "duration",
        "bitrate",
        "samplerate",
        "cuepoint",
        "bpm",
        "wavesummaryhex",
        "channels",
        "datetime_added",
        "mixxx_deleted",
        "played",
        "header_parsed",
        "filetype",
        "replaygain",
        "timesplayed",
        "rating",
        "key",
        "beats",
        "beats_version",
        "composer",
        "bpm_lock",
        "beats_sub_version",
        "keys",
        "keys_version",
        "keys_sub_version",
        "key_id",
        "grouping",
        "album_artist",
        "coverart_source",
        "coverart_type",
        "coverart_location",
        "coverart_hash",
        "replaygain_peak",
        "tracktotal",
        "color",
    )
    table = "library"
    id_column = "id"

    id: int

    @classmethod
    def list_by_locations(cls, db_filename: str, *locations: TrackLocation):
        with db_connection(db_filename) as conn:
            try:
                sql = "SELECT %s FROM %s WHERE location IN (%s)" % (
                    ", ".join(cls.columns),
                    cls.table,
                    ", ".join([str(l.id) for l in locations])
                )
                cur = conn.execute(sql)
                lst = cur.fetchall()
                return [cls(**row) for row in lst]
            finally:
                cur.close()

    @classmethod
    def bulk_delete(cls, db_filename: str, *objs: "Track"):
        track_ids = [o.id for o in objs]
        PlaylistTrack.delete_by_track_id(db_filename, *track_ids)
        CrateTrack.delete_by_track_id(db_filename, *track_ids)
        Cue.delete_by_track_id(db_filename, *track_ids)
        TrackAnalysis.delete_by_track_id(db_filename, *track_ids)
        return super().bulk_delete(db_filename, *objs)
