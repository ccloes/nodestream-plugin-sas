from io import IOBase
from logging import getLogger
from typing import Iterable

import pandas as pd
from nodestream.model import JsonLikeDocument
from nodestream.pipeline.extractors.files import FileCodec

LOGGER = getLogger(__name__)


def _sas_text_encoding(reader, source: str) -> str:
    """Return the codec for SAS text cells.

    The file is read as raw bytes, so a bad byte is handled per cell instead
    of aborting the reader. pandas records the declared encoding on
    `inferred_encoding`. An unknown code is not a Python codec, and those
    files are decoded as latin-1.
    """
    encoding = getattr(reader, "inferred_encoding", None)
    if isinstance(encoding, str) and not encoding.startswith("unknown"):
        return encoding
    LOGGER.warning(
        "Could not infer SAS text encoding; decoding text as latin-1 in %s.",
        source,
    )
    return "latin-1"


def _count_undecodable_cells(series: pd.Series, encoding: str) -> int:
    count = 0
    for value in series:
        if not isinstance(value, (bytes, bytearray)):
            continue
        try:
            value.decode(encoding)
        except UnicodeDecodeError:
            count += 1
    return count


def _log_replaced_sas_bytes(
    replaced: dict[str, int],
    source: str,
    replaced_headers: dict[str, str] | None = None,
) -> None:
    """Log one warning for undecodable SAS cells and column names."""
    total = sum(replaced.values())
    header_names = list((replaced_headers or {}).values())
    if not total and not header_names:
        return
    if total and header_names:
        LOGGER.warning(
            "replaced undecodable bytes in %s cells (columns: %s) "
            "and %s column names (%s) in %s",
            total,
            ", ".join(replaced),
            len(header_names),
            ", ".join(header_names),
            source,
        )
        return
    if header_names:
        LOGGER.warning(
            "replaced undecodable bytes in %s column names (%s) in %s",
            len(header_names),
            ", ".join(header_names),
            source,
        )
        return
    LOGGER.warning(
        "replaced undecodable bytes in %s cells (columns: %s) in %s",
        total,
        ", ".join(replaced),
        source,
    )


def _normalize_frame(
    frame: pd.DataFrame,
    encoding: str | None = None,
    replaced: dict[str, int] | None = None,
    replaced_headers: dict[str, str] | None = None,
) -> list[JsonLikeDocument]:
    """Turn one SAS chunk into plain records.

    SAS stores every number as a float, so numeric columns stay float even
    when a value is a whole number. Missing values are None. Timestamps
    become datetime objects. Text cells arrive as bytes; they are decoded
    with `encoding`, and a byte that does not fit is replaced. Column names
    arrive as latin-1 and are re-decoded with the file encoding before
    surrounding spaces are stripped. A column name that does not fit is
    replaced and recorded once in `replaced_headers` (later chunks repeat
    the same header).
    """
    normalized = frame.copy()
    names = []
    for column in normalized.columns:
        name, header_replaced = _sas_column_name(column, encoding)
        names.append(name)
        if header_replaced and replaced_headers is not None:
            replaced_headers.setdefault(str(column), name)
    normalized.columns = names
    for column in list(normalized.columns):
        series = normalized[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            normalized[column] = pd.Series(
                [
                    None if pd.isna(value) else pd.Timestamp(value).to_pydatetime()
                    for value in series
                ],
                index=normalized.index,
                dtype=object,
            )
            continue
        if encoding and _series_holds_bytes(series):
            if replaced is not None:
                count = _count_undecodable_cells(series, encoding)
                if count:
                    replaced[column] = replaced.get(column, 0) + count
            normalized[column] = series.str.decode(encoding, errors="replace")
    normalized = normalized.astype(object).where(normalized.notna(), None)
    return normalized.to_dict(orient="records")


def _sas_column_name(column, encoding: str | None) -> tuple[str, bool]:
    """Return the stripped name and whether decoding replaced bytes."""
    name = str(column)
    replaced = False
    if encoding:
        raw = name.encode("latin-1")
        try:
            name = raw.decode(encoding)
        except UnicodeDecodeError:
            name = raw.decode(encoding, errors="replace")
            replaced = True
    return name.strip(), replaced


def _series_holds_bytes(series: pd.Series) -> bool:
    if series.dtype != object:
        return False
    present = series.dropna()
    return not present.empty and isinstance(present.iloc[0], (bytes, bytearray))


def _iter_sas7bdat_records(
    reader: IOBase, chunksize: int, source: str
) -> Iterable[JsonLikeDocument]:
    """Yield one dictionary per row from an open sas7bdat file."""
    replaced: dict[str, int] = {}
    replaced_headers: dict[str, str] = {}
    try:
        with pd.read_sas(
            reader,
            format="sas7bdat",
            iterator=True,
            chunksize=chunksize,
            compression=None,
            encoding=None,
        ) as sas_reader:
            encoding = _sas_text_encoding(sas_reader, source)
            for frame in sas_reader:
                yield from _normalize_frame(frame, encoding, replaced, replaced_headers)
    finally:
        _log_replaced_sas_bytes(replaced, source, replaced_headers)


class Sas7bdatFileFormat(FileCodec, alias=".sas7bdat"):
    """File format codec for SAS sas7bdat files.

    Each row is a dictionary keyed by column name, like csv.DictReader.
    Missing values are None and SAS dates are Python datetime objects.
    SAS stores every number as a float, including whole numbers, so numeric
    properties stay one type across rows. Cast them later with a normalizer
    if a pipeline wants integers. Text is decoded with the encoding declared
    in the file. A byte that does not fit that encoding is replaced, and the
    rest of the file is still read. An unknown encoding is decoded as latin-1.
    Rows are read in chunks of `chunksize` (10,000 by default).

    Suffix lookup in nodestream is case-insensitive, so `.SAS7BDAT` uses this
    codec too.
    """

    chunksize = 10_000

    def read_file_from_handle(
        self, reader: IOBase, source: str | None = None
    ) -> Iterable[JsonLikeDocument]:
        if not source:
            name = getattr(reader, "name", None)
            source = str(name) if name else "unknown file"
        return _iter_sas7bdat_records(reader, self.chunksize, source)
