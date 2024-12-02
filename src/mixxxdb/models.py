import os
from typing import List
from mixxxdb.db import BinaryField, BooleanField, DatetimeField, FloatField, ForeignKeyField, Model, IntegerField, CharField, TextField


class TrackLocation(Model):
    class Meta:
        table = "track_locations"
        id_column = "id"

    id = IntegerField(nullable=False)
    location = CharField(max_length=512)
    directory = CharField(max_length=512)
    filename = CharField(max_length=512)
    filesize = IntegerField()
    fs_deleted = BooleanField()
    needs_verification = BooleanField()

    @classmethod
    def bulk_delete(cls, *objs):
        ids = [obj.id for obj in objs]
        super().bulk_delete(*objs)
        TrackAnalysis.bulk_delete(*TrackAnalysis.list(track_id=ids))
        Track.bulk_delete(*Track.list(location=ids))

    @classmethod
    def list_orphans(cls) -> "List[TrackLocation]":
        orphans = []
        locations = [l for l in cls.list() if l.location is not None]

        # Probably faster to do one directory at a time:
        dirs = {}
        for dirname in set([os.path.dirname(l.location) for l in locations]):
            try:
                dirs[dirname] = os.listdir(dirname)
            except FileNotFoundError:
                dirs[dirname] = []

        for l in locations:
            dirname = os.path.dirname(l.location)
            if l.filename not in dirs[dirname]:
                orphans.append(l)

        return orphans

    def delete(self):
        super().delete()
        tracks = Track.list(location=self.id)
        Track.bulk_delete(*tracks)


class Track(Model):
    class Meta:
        table = "library"
        id_column = "id"

    id = IntegerField(nullable=False)
    artist = CharField(max_length=64)
    title = CharField(max_length=64)
    album = CharField(max_length=64)
    year = CharField(max_length=16)
    genre = CharField(max_length=64)
    tracknumber = CharField(max_length=3)
    location = ForeignKeyField(model="TrackLocation")
    comment = CharField(max_length=256)
    url = CharField(max_length=256)
    duration = IntegerField()
    bitrate = IntegerField()
    samplerate = IntegerField()
    cuepoint = IntegerField()
    bpm = FloatField()
    wavesummaryhex = BinaryField()
    channels = IntegerField()
    datetime_added = DatetimeField()
    mixxx_deleted = BooleanField()
    played = BooleanField()
    header_parsed = BooleanField(default=False)
    filetype = CharField(max_length=8, default="?")
    replaygain = FloatField(default=0.0)
    timesplayed = IntegerField(default=0)
    rating = IntegerField(default=0)
    key = CharField(max_length=8, default="")
    beats = BinaryField()
    beats_version = TextField()
    composer = CharField(max_length=64, default="")
    bpm_lock = BooleanField(default=False)
    beats_sub_version = TextField(default="")
    keys = BinaryField()
    keys_version = TextField()
    keys_sub_version = TextField()
    key_id = IntegerField(default=0)
    grouping = TextField(default="")
    album_artist = TextField(default="")
    coverart_source = IntegerField(default=0)
    coverart_type = IntegerField(default=0)
    coverart_location = TextField(default="")
    coverart_hash = IntegerField(default=0)
    replaygain_peak = FloatField(default=-1.0)
    tracktotal = TextField(default="//")
    color = IntegerField()

    @classmethod
    def bulk_delete(cls, *objs):
        ids = [obj.id for obj in objs]
        super().bulk_delete(*objs)
        Cue.bulk_delete(*Cue.list(track_id=ids))
        # TODO: How to handle model with composite key?
        # CrateTrack.bulk_delete(*CrateTrack.list(track_id=ids))
        PlaylistTrack.bulk_delete(*PlaylistTrack.list(track_id=ids))


class TrackAnalysis(Model):
    class Meta:
        table = "track_analysis"
        id_column = "id"

    id = IntegerField(nullable=False)
    track_id = ForeignKeyField(model="TrackLocation", nullable=False)
    type = CharField(max_length=512)
    description = CharField(max_length=1024)
    version = CharField(max_length=512)
    created = DatetimeField()
    data_checksum = CharField(max_length=512)


class Cue(Model):
    class Meta:
        table = "cues"
        id_column = "id"

    id = IntegerField(nullable=False)
    track_id = ForeignKeyField(model="Track", nullable=False)
    type = IntegerField(nullable=False, default=0)
    position = IntegerField(nullable=False, default=-1)
    length = IntegerField(nullable=False, default=0)
    hotcue = IntegerField(nullable=False, default=-1)
    label = TextField(default="", nullable=False)
    color = IntegerField(nullable=False, default=4294901760)


class CrateTrack(Model):
    class Meta:
        table = "crate_tracks"

    crate_id = IntegerField(nullable=False)
    track_id = ForeignKeyField(model="Track", nullable=False)


class PlaylistTrack(Model):
    class Meta:
        table = "PlaylistTracks"
        id_column = "id"

    id = IntegerField(nullable=False)
    playlist_id = IntegerField()
    track_id = ForeignKeyField(model="Track")
    position = IntegerField()
    pl_datetime_added = DatetimeField()
