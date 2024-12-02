from importlib import import_module
from copy import copy
from contextlib import contextmanager
from datetime import datetime
from dateutil.parser import isoparse
from pathlib import Path
import sqlite3
from typing import Dict, List, Optional, Sequence, Type, TypeVar, Generic, Union


_T = TypeVar("_T")
_M = TypeVar("_M", bound="Model")

DB_FILENAME = str(Path().home().joinpath(".mixxx/mixxxdb.sqlite"))

EMPTY = object()


@contextmanager
def db_connection(db_filename: str):
    try:
        conn = sqlite3.connect(db_filename)
        yield conn
    finally:
        conn.close()


class Field(Generic[_T]):
    nullable: bool
    type: Type[_T]
    value: Optional[_T]
    default: Optional[_T]

    def __init__(self, nullable=True, value=None, default=None, **kwargs):
        self.nullable = nullable
        self.value = value
        self.default = default

    def __copy__(self):
        return self.__class__(**{k: v for k, v in self.__dict__.items() if not k.startswith("_")})

    def check(self):
        if not isinstance(self.value, self.type):
            if self.value is not None or not self.nullable:
                raise ValueError

    def get_equals_expr(self, value=EMPTY):
        real_value = self.value if value is EMPTY else value
        if real_value is None:
            return " IS NULL"
        return "=%s" % (self.to_db(real_value),)

    def to_python(self, value):
        return value

    def to_db(self, value=EMPTY):
        real_value = self.value if value is EMPTY else value
        if real_value is None:
            return "NULL"
        return self.to_db_not_null(real_value)

    def to_db_not_null(self, value: _T):
        return value


class TextField(Field[str]):
    type = str

    def to_db_not_null(self, value):
        return "'%s'" % (str(value).replace("'", "''"),)


class CharField(TextField):
    max_length: int

    def __init__(self, max_length: int, **kwargs):
        super().__init__(**kwargs)
        self.max_length = max_length


class IntegerField(Field[int]):
    type = int


class FloatField(Field[float]):
    type = float


class BinaryField(Field[bytes]):
    type = bytes


class DatetimeField(Field[datetime]):
    type = datetime

    def to_python(self, value):
        return isoparse(value)

    def to_db_not_null(self, value):
        return "'%s'" % (value.isoformat(),)


class BooleanField(Field[bool]):
    type = bool

    def to_python(self, value):
        return bool(value)

    def to_db_not_null(self, value):
        return 1 if value else 0


class ForeignKeyField(Field[int]):
    # TODO: How to access object?
    model: "Union[str, Type[Model]]"

    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        self.model = model

    def get_object(self):
        return self.model.get(self.value)


class ModelMeta:
    base_fields: Dict[str, Field]
    id_column: str
    id_field: Field
    table: str

    @property
    def db_filename(self) -> str:
        return DB_FILENAME

    def __init__(
        self,
        table: Optional[str],
        id_column: Optional[str],
        base_fields: Dict[str, Field]
    ):
        self.base_fields = base_fields
        if table:
            self.table = table
        if id_column:
            self.id_column = id_column
            self.id_field = base_fields[id_column]


class ModelBase(type):
    def __new__(cls, name, bases, attrs, **kwargs):
        new_class = super().__new__(cls, name, bases, attrs)
        fields = {k: v for k, v in attrs.items() if isinstance(v, Field)}
        meta = ModelMeta(
            table=getattr(attrs["Meta"], "table", None),
            id_column=getattr(attrs["Meta"], "id_column", None),
            base_fields=fields,
        )
        setattr(new_class, "_meta", meta)
        return new_class


class Model(metaclass=ModelBase):
    _fields: Dict[str, Field]
    _meta: ModelMeta

    class Meta:
        table: str
        id_column: str

    def __init__(self, **kwargs):
        # setattr(self, "_fields", {k: copy(v) for k, v in self._meta.fields.items()})
        self._fields = {k: copy(v) for k, v in self._meta.base_fields.items()}
        self._meta.id_field = self._fields[self._meta.id_column]
        for field_name, field in self._fields.items():
            if isinstance(field, ForeignKeyField):
                if isinstance(field.model, str):
                    if "." in field.model:
                        classname, modname = [r[::-1] for r in field.model[::-1].split(".", 1)]
                    else:
                        classname, modname = (field.model, self.__module__)
                    module = import_module(modname)
                    field.model = getattr(module, classname)
            if field_name in kwargs:
                field.value = field.to_python(kwargs[field_name])
            else:
                field.value = None

    def __getattribute__(self, name: str):
        if not name.startswith("_") and hasattr(self, "_fields") and name in self._fields:
            return self._fields[name].value
        return super().__getattribute__(name)

    def __repr__(self):
        return '<%s [%s]>' % (self.__class__.__name__, self)

    def __str__(self):
        return ", ".join(["%s=%s" % (k, v.value) for k, v in self._fields.items()])

    def __setattr__(self, name: str, value):
        if hasattr(self, "_fields") and name in self._fields:
            self._fields[name].value = value
        else:
            super().__setattr__(name, value)

    def _check(self):
        for field_name, field in self._fields.items():
            try:
                field.check()
            except ValueError:
                raise ValueError("Illegal value for %s.%s: %s" % (self.__class__.__name__, field_name, field.value))

    @classmethod
    def bulk_delete(cls: Type[_M], *objs: _M):
        with db_connection(cls._meta.db_filename) as conn:
            conn.execute(
                "DELETE FROM %s WHERE %s IN (%s)" % (
                    cls._meta.table,
                    cls._meta.id_column,
                    ", ".join([str(obj.get_id()) for obj in objs])
                )
            )

    @classmethod
    def get(cls: Type[_M], id) -> _M:
        with db_connection(cls._meta.db_filename) as conn:
            field_names = list(cls._meta.base_fields.keys())
            sql = "SELECT %s FROM %s WHERE %s%s" % (
                ", ".join(field_names),
                cls._meta.table,
                cls._meta.id_column,
                cls._meta.id_field.get_equals_expr(id),
            )
            cur = conn.execute(sql)

            try:
                row = cur.fetchone()
                if row is None:
                    raise ValueError("%s with %s=%s does not exist" % (
                        cls.__name__,
                        cls._meta.id_column,
                        id,
                    ))
                return cls(**{field_names[idx]: row[idx] for idx in range(len(field_names))})
            finally:
                cur.close()

    @classmethod
    def list(cls: Type[_M], **conds) -> List[_M]:
        field_names = list(cls._meta.base_fields.keys())
        db_conds = []

        for k, vseq in conds.items():
            if k in field_names:
                if isinstance(vseq, str) or not isinstance(vseq, Sequence):
                    vseq = [vseq]
                db_conds.append(
                    "(%s)" % (
                        " OR ".join([k + cls._meta.base_fields[k].get_equals_expr(v) for v in vseq])
                    )
                )

        with db_connection(cls._meta.db_filename) as conn:
            sql = "SELECT %s FROM %s" % (", ".join(field_names), cls._meta.table)
            if db_conds:
                sql += " WHERE "
                sql += " AND ".join(db_conds)
            cur = conn.execute(sql)

            try:
                lst = cur.fetchall()
                ret = []
                for row in lst:
                    ret.append(cls(**{field_names[idx]: row[idx] for idx in range(len(field_names))}))
                return ret
            finally:
                cur.close()

    def get_equals_expr(self, field_name: str, value=EMPTY):
        return "%s%s" % (field_name, self._fields[field_name].get_equals_expr(value))

    def get_id(self):
        return self._meta.id_field.value

    def delete(self):
        if self.get_id() is None:
            raise ValueError("Cannot do delete() on %s; id is None" % (self,))
        sql = "DELETE FROM %s WHERE %s" % (self._meta.table, self.get_equals_expr(self._meta.id_column))
        with db_connection(self._meta.db_filename) as conn:
            conn.execute(sql)

    def update(self):
        if self.get_id() is None:
            raise ValueError("Cannot do update() on %s; id is None" % (self,))
        self._check()
        updates = [
            self.get_equals_expr(field_name)
            for field_name in self._fields.keys()
            if field_name != self._meta.id_column
        ]
        sql = "UPDATE %s SET %s WHERE %s" % (
            self._meta.table,
            ", ".join(updates),
            self._meta.id_column,
            self.get_equals_expr(self._meta.id_column)
        )
        with db_connection(self._meta.db_filename) as conn:
            conn.execute(sql)
