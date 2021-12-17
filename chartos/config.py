import yaml
import string
import typing
from abc import ABC, abstractmethod
from typing import Optional, List, Iterator, TypeVar, Dict, Type, Literal, Union, ClassVar, Generator, Tuple
from enum import IntEnum, auto
from dataclasses import dataclass, field
from chartos.utils import PeekableIterator, ValueDependable
from chartos.serialized_config import SerializedConfig, SerializedLayer, SerializedView, SerializedField
from collections import defaultdict
from shapely.geometry import shape


get_config = ValueDependable("get_config")


class FieldType(ABC):
    @property
    @abstractmethod
    def pg_type(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True, eq=True)
class Field:
    name: str
    description: str
    type: FieldType

    @staticmethod
    def parse(raw_config: SerializedField) -> "Field":
        field_type = TypeParser.parse_str(raw_config.type)
        assert isinstance(field_type, FieldType)
        return Field(
            raw_config.name,
            raw_config.description,
            field_type,
        )

    def pg_view_select(self) -> str:
        if not isinstance(self.type, GeomField):
            return self.pg_name()
        return f"ST_Transform({self.pg_name()}, 4326) AS {self.pg_name()}"

    def pg_name(self) -> str:
        return f'"{self.name}"'

    def pg_type(self) -> str:
        return self.type.pg_type

    def from_json(self, json_data):
        if isinstance(self.type, GeomField):
            return shape(json_data)
        return json_data


@dataclass
class View:
    name: str
    on_field: Field
    fields: List[Field]
    cache_duration: int

    @staticmethod
    def parse(layer_fields: Dict[str, Field], raw_config: SerializedView) -> "View":
        resolved_on_field = layer_fields[raw_config.on_field]
        view_fields = (
            raw_config.fields
            if raw_config.fields is not None
            else list(layer_fields.keys())
        )
        if raw_config.exclude_fields is not None:
            for excluded_field in raw_config.exclude_fields:
                view_fields.remove(excluded_field)

        resolved_fields = [layer_fields[name] for name in view_fields]
        cache_duration = raw_config.cache_duration
        if cache_duration is None:
            cache_duration = 3600
        return View(
            raw_config.name,
            resolved_on_field,
            resolved_fields,
            cache_duration,
        )


@dataclass
class Layer:
    name: str
    id_field: Field
    fields: Dict[str, Field]
    views: Dict[str, View]
    description: Optional[str] = None
    attribution: Optional[str] = None

    def pg_table_name(self):
        return self.name

    @staticmethod
    def parse(raw_config: SerializedLayer) -> "Layer":
        parsed_fields = map(Field.parse, raw_config.fields)
        fields = {field.name: field for field in parsed_fields}
        parsed_views = (View.parse(fields, view) for view in raw_config.views)
        views = {view.name: view for view in parsed_views}
        id_field = fields[raw_config.id_field_name]
        return Layer(
            raw_config.name,
            id_field,
            fields,
            views,
            description=raw_config.description,
            attribution=raw_config.attribution,
        )

    def pg_schema(self):
        yield None, "version", "varchar"
        for layer_field in self.fields.values():
            yield layer_field, layer_field.pg_name(), layer_field.pg_type()

    def get_pg_field_index(self, field):
        for i, layer_field in enumerate(self.fields.values()):
            if layer_field is field:
                return i + 1
        raise RuntimeError("field not found")

    def pg_field_names(self):
        for _, field_name, _ in self.pg_schema():
            yield field_name

    def pg_table_sig(self) -> str:
        fields_sig = ', '.join(
            f"{pg_name} {pg_type}"
            for _, pg_name, pg_type
            in self.pg_schema()
        )
        return f"({fields_sig})"

    def get_viewed_fields(self) -> Dict[Field, View]:
        indexed_geo_fields: Dict[Field, View] = defaultdict(list)
        for view in self.views.values():
            indexed_geo_fields[view.on_field].append(view)
        return indexed_geo_fields



@dataclass
class Config:
    name: str
    description: str
    layers: Dict[str, Layer]

    @staticmethod
    def parse(raw_config: SerializedConfig) -> "Config":
        parsed_layers = (Layer.parse(layer) for layer in raw_config.layers)
        return Config(
            raw_config.name,
            raw_config.description,
            {layer.name: layer for layer in parsed_layers}
        )


@dataclass(frozen=True, eq=True)
class TextField(FieldType):
    pg_type = "varchar"


@dataclass(frozen=True, eq=True)
class JsonField(FieldType):
    pg_type = "jsonb"


@dataclass(frozen=True, eq=True)
class IntField(FieldType):
    pg_type = "integer"


@dataclass(frozen=True, eq=True)
class BoolField(FieldType):
    pg_type = "boolean"


@dataclass(frozen=True, eq=True)
class BigIntField(FieldType):
    pg_type = "bigint"


@dataclass(frozen=True, eq=True)
class DoubleField(FieldType):
    pg_type = "double precision"


@dataclass(frozen=True, eq=True)
class StringField(FieldType):
    max_len: Optional[int] = None

    @property
    def pg_type(self) -> str:
        if self.max_len is None:
            return "varchar"
        return f"varchar({self.max_len})"


@dataclass(frozen=True, eq=True)
class CharField(FieldType):
    max_len: Optional[int] = None

    def __post_init__(self) -> None:
        if self.max_len is None:
            raise TypeError("missing max_len")

    @property
    def pg_type(self) -> str:
        assert self.max_len is not None
        return f"char({self.max_len})"


@dataclass(frozen=True, eq=True)
class ArrayField(FieldType):
    of: Optional[FieldType] = None

    def __post_init__(self) -> None:
        if self.of is None:
            raise TypeError("missing Array(of=)")

    @property
    def pg_type(self) -> str:
        assert self.of is not None
        return f"{self.of.pg_type}[]"

@dataclass(frozen=True, eq=True)
class GeomField(FieldType):
    pg_type = "geometry(Geometry, 3857)"


@dataclass(frozen=True, eq=True)
class TimestampField(FieldType):
    pg_type = 'timestamp with time zone'



class TokType(IntEnum):
    NAME = auto()
    INT = auto()
    CALL_START = auto()
    EQUAL = auto()
    PARAM_SEP = auto()
    CALL_END = auto()


class AbstractTok(ABC):
    @classmethod
    @abstractmethod
    def parse(cls, stream: PeekableIterator[str]) -> Optional["Tok"]:
        raise NotImplementedError


class ConstantTok(AbstractTok):
    constant_value: ClassVar[str]

    @classmethod
    def parse(cls, stream: PeekableIterator[str]) -> Optional["Tok"]:
        peek_stream = iter(stream.peek_iterator())
        try:
            constant = cls.constant_value
            for c in constant:
                if c != next(peek_stream):
                    return None
            stream.consume(len(constant))
            return cls()  # type: ignore
        except StopIteration:
            # if the stream end before the constant token, the constant isn't there
            return None


class CharClassTok(AbstractTok):
    char_class: ClassVar[str]

    @classmethod
    @abstractmethod
    def parse_from_str(cls, tok_str: str) -> Optional["Tok"]:
        raise NotImplementedError

    @classmethod
    def parse(cls, stream: PeekableIterator[str]) -> Optional["Tok"]:
        char_class = cls.char_class
        first_char = stream.peek()
        if first_char not in char_class:
            return None

        tok_str = [first_char]
        stream.consume()

        while True:
            next_char = stream.try_peek()
            if next_char is None or next_char not in char_class:
                break
            tok_str.append(next_char)
            stream.consume()
        return cls.parse_from_str("".join(tok_str))


@dataclass
class TokName(CharClassTok):
    name: str
    tag: Literal[TokType.NAME] = field(default=TokType.NAME, repr=False)

    char_class = string.ascii_lowercase + "_"

    @classmethod
    def parse_from_str(cls, tok_str: str) -> Optional["Tok"]:
        return TokName(tok_str)


@dataclass
class TokInt(CharClassTok):
    value: int
    tag: Literal[TokType.INT] = field(default=TokType.INT, repr=False)

    char_class = string.digits

    @classmethod
    def parse_from_str(cls, tok_str: str) -> Optional["Tok"]:
        return TokInt(int(tok_str, base=10))


@dataclass
class TokCallStart(ConstantTok):
    tag: Literal[TokType.CALL_START] = field(default=TokType.CALL_START, repr=False)
    constant_value = "("


@dataclass
class TokEqual(ConstantTok):
    tag: Literal[TokType.EQUAL] = field(default=TokType.EQUAL, repr=False)
    constant_value = "="


@dataclass
class TokParamSep(ConstantTok):
    tag: Literal[TokType.PARAM_SEP] = field(default=TokType.PARAM_SEP, repr=False)
    constant_value = ","


@dataclass
class TokCallEnd(ConstantTok):
    tag: Literal[TokType.CALL_END] = field(default=TokType.CALL_END, repr=False)
    constant_value = ")"


Tok = Union[
    TokCallStart,
    TokEqual,
    TokParamSep,
    TokCallEnd,
    TokName,
    TokInt,
]


def lex(stream: PeekableIterator[str]) -> Iterator[Tok]:
    tok_type: Type[Tok]
    while not stream.is_empty():
        if stream.peek() in string.whitespace:
            stream.consume()
            continue
        for tok_type in typing.get_args(Tok):
            if (tok := tok_type.parse(stream)) is not None:
                break
        else:
            raise SyntaxError(f"invalid char: {stream.peek()}")
        yield tok


FIELD_TYPES: Dict[str, Type[FieldType]] = {
    "text": TextField,
    "char": CharField,
    "string": StringField,
    "int": IntField,
    "bigint": BigIntField,
    "bool": BoolField,
    "double": DoubleField,
    "json": JsonField,
    "array": ArrayField,
    "geom": GeomField,
    "timestamp": TimestampField,
}


T = TypeVar("T")


class TypeParser:
    __slots__ = ("tok_stream",)

    tok_stream: PeekableIterator[Tok]

    def __init__(self, tok_stream: PeekableIterator[Tok]):
        self.tok_stream = tok_stream

    @staticmethod
    def parse_str(expr_str: str) -> FieldType:
        parser = TypeParser(PeekableIterator(lex(PeekableIterator(iter(expr_str)))))
        return parser.parse()

    def expect(self, tok_type: Type[T]) -> T:
        tok = self.tok_stream.try_peek()
        if tok is None:
            raise SyntaxError(f"expected token with type {tok_type}")
        if not isinstance(tok, tok_type):
            raise SyntaxError(f"unexpected token: {tok}")
        self.tok_stream.consume()
        return tok

    def parse(self) -> FieldType:
        res = self.parse_field_type()
        remainder = self.tok_stream.try_peek()
        if remainder is None:
            return res
        raise SyntaxError(f"unexpected token {remainder}")

    def parse_field_type(self) -> FieldType:
        # parse the field type name
        field_type_name = self.expect(TokName)

        # find the associated field type
        field_type = FIELD_TYPES.get(field_type_name.name)
        if field_type is None:
            raise SyntaxError("unknown field type {}, expected {}".format(
                field_type_name, ", ".join(FIELD_TYPES)
            ))

        # if there's no (, evaluate the field type straight away
        next_tok = self.tok_stream.try_peek()
        if next_tok is None or next_tok.tag != TokType.CALL_START:
            return field_type()
        self.tok_stream.consume()  # pop the (

        args: Dict[str, Union[str, int, FieldType]] = {}
        while True:
            next_tok = self.tok_stream.try_peek()
            # stop when the ) is reached
            if next_tok is None:
                raise SyntaxError("unexpected end of argument list")
            if next_tok.tag == TokType.CALL_END:
                self.tok_stream.consume()
                break

            arg_name = self.expect(TokName)
            self.expect(TokEqual)
            arg_value = self.tok_stream.try_peek()
            if arg_value is None:
                raise SyntaxError("unexpected end of argument value")

            if arg_value.tag == TokType.INT:
                self.tok_stream.consume()
                args[arg_name.name] = arg_value.value
            elif arg_value.tag == TokType.NAME:
                args[arg_name.name] = self.parse_field_type()
            else:
                raise SyntaxError("unexpected argument value token")
        return field_type(**args)  # type: ignore
