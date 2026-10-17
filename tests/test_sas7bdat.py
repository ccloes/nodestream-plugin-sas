import bz2
import gzip
import logging
import zipfile
from datetime import datetime
from importlib.metadata import entry_points
from io import BytesIO
from pathlib import Path

import pandas as pd
import pytest
from nodestream.pipeline.extractors.files import FileExtractor, LocalFileSource

from nodestream_plugin_sas.sas7bdat import (
    Sas7bdatFileFormat,
    _iter_sas7bdat_records,
    _normalize_frame,
)

SIMPLE_RECORD = {"record": "value"}
PLUGIN_LOGGER = "nodestream_plugin_sas.sas7bdat"
CORE_LOGGER = "nodestream.pipeline.extractors.files"

# sample.sas7bdat is pandas' test1.sas7bdat; see tests/data/LICENSE-pandas.
SAS7BDAT_FIXTURE = Path(__file__).resolve().parent / "data" / "sample.sas7bdat"
SAS_PREVIEW_COLUMNS = ("Column2", "Column3", "Column4", "Column8")
SAS_PREVIEW = [
    {
        "Column2": "pear",
        "Column3": 84.0,
        "Column4": datetime(1965, 12, 10),
        "Column8": None,
    },
    {
        "Column2": "dog",
        "Column3": 49.0,
        "Column4": datetime(1977, 3, 7),
        "Column8": 339.0,
    },
]

# Byte 70 of a sas7bdat header is the encoding code. 62 is cp1252, 20 is UTF-8.
SAS_ENCODING_OFFSET = 70
SAS_CP1252_ENCODING = 62
SAS_UTF8_ENCODING = 20


def _warning_message(logger) -> str:
    logger.warning.assert_called_once()
    args = logger.warning.call_args.args
    return args[0] % args[1:]


def _sas_preview(records):
    return [
        {column: record[column] for column in SAS_PREVIEW_COLUMNS}
        for record in records[:2]
    ]


async def _records_from(path: Path):
    subject = FileExtractor([LocalFileSource([path])])
    return [record async for record in subject.extract_records()]


def _assert_sas_records(records):
    assert len(records) == 10
    assert all(len(record) == 100 for record in records)
    assert _sas_preview(records) == SAS_PREVIEW
    assert all(
        record["Column1"] is None or isinstance(record["Column1"], float)
        for record in records
    )
    assert records[0]["Column1"] == pytest.approx(0.636)
    assert all(
        record["Column3"] is None or isinstance(record["Column3"], float)
        for record in records
    )


@pytest.fixture
def sas7bdat_file(tmp_path: Path) -> Path:
    path = tmp_path / "sample.sas7bdat"
    path.write_bytes(SAS7BDAT_FIXTURE.read_bytes())
    return path


def test_file_formats_entry_point_is_published():
    found = [
        entry
        for entry in entry_points(group="nodestream.plugins", name="file_formats")
        if entry.value == "nodestream_plugin_sas.sas7bdat"
    ]
    assert found


@pytest.mark.asyncio
async def test_sas7bdat_file(sas7bdat_file: Path):
    _assert_sas_records(await _records_from(sas7bdat_file))


@pytest.mark.asyncio
async def test_uppercase_sas7bdat_suffix(tmp_path: Path, sas7bdat_file: Path):
    path = tmp_path / "PEOPLE.SAS7BDAT"
    path.write_bytes(sas7bdat_file.read_bytes())
    _assert_sas_records(await _records_from(path))


@pytest.mark.asyncio
async def test_gzip_compressed_sas7bdat(tmp_path: Path, sas7bdat_file: Path):
    path = tmp_path / "people.sas7bdat.gz"
    with gzip.open(path, "wb") as compressed:
        compressed.write(sas7bdat_file.read_bytes())
    _assert_sas_records(await _records_from(path))


@pytest.mark.asyncio
async def test_bz2_compressed_sas7bdat(tmp_path: Path, sas7bdat_file: Path):
    path = tmp_path / "people.sas7bdat.bz2"
    with bz2.open(path, "wb") as compressed:
        compressed.write(sas7bdat_file.read_bytes())
    _assert_sas_records(await _records_from(path))


@pytest.mark.asyncio
async def test_zip_archive_dispatches_each_member(tmp_path: Path, sas7bdat_file: Path):
    archive_path = tmp_path / "bundle.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(sas7bdat_file, "tables/one.sas7bdat")
        archive.writestr("notes/readme.txt", "ignore me")
        archive.writestr("notes/people.csv", "record\nvalue\n")
        archive.writestr("__MACOSX/._one.sas7bdat", b"not a sas file")
        archive.write(sas7bdat_file, "tables/two.SAS7BDAT")
    records = await _records_from(archive_path)
    _assert_sas_records(records[:10])
    assert records[10] == {"line": "ignore me"}
    assert records[11] == SIMPLE_RECORD
    _assert_sas_records(records[12:])


@pytest.mark.asyncio
async def test_zip_bad_member_does_not_drop_later_members(
    tmp_path: Path, sas7bdat_file: Path, mocker, caplog
):
    archive_path = tmp_path / "bundle.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("a_corrupt.csv", "record\nvalue\n")
        archive.writestr("a_bad.sas7bdat", b"this is not a sas7bdat file")
        archive.write(sas7bdat_file, "b_good.sas7bdat")
    _corrupt_zip_member_crc(archive_path, "a_corrupt.csv")
    subject = FileExtractor([LocalFileSource([archive_path])])
    subject.logger = mocker.Mock()
    with caplog.at_level(logging.WARNING, logger=CORE_LOGGER):
        records = [record async for record in subject.extract_records()]
    _assert_sas_records(records)
    warning = " ".join(
        str(arg) for call in subject.logger.warning.call_args_list for arg in call.args
    )
    assert "a_bad.sas7bdat" in warning
    assert "a_corrupt.csv" in caplog.text
    assert "b_good.sas7bdat" not in warning
    assert "b_good.sas7bdat" not in caplog.text


def _corrupt_zip_member_crc(path: Path, member: str) -> None:
    """Flip one stored payload byte so the member's CRC no longer matches."""
    with zipfile.ZipFile(path) as archive:
        offset = archive.getinfo(member).header_offset
    raw = bytearray(path.read_bytes())
    name_len = int.from_bytes(raw[offset + 26 : offset + 28], "little")
    extra_len = int.from_bytes(raw[offset + 28 : offset + 30], "little")
    data_start = offset + 30 + name_len + extra_len
    raw[data_start] ^= 0xFF
    path.write_bytes(raw)


@pytest.mark.asyncio
async def test_invalid_sas7bdat_is_skipped(tmp_path: Path, mocker):
    path = tmp_path / "bad.sas7bdat"
    path.write_bytes(b"this is not a sas7bdat file")
    subject = FileExtractor([LocalFileSource([path])])
    subject.logger = mocker.Mock()
    assert [record async for record in subject.extract_records()] == []
    message = _warning_message(subject.logger)
    reason = subject.logger.warning.call_args.kwargs["extra"]["exception"]
    assert message.startswith(f"Failed to parse {path} file (at path {path}).")
    assert reason


@pytest.mark.asyncio
async def test_sas7bdat_chunksize_keeps_column_types(sas7bdat_file: Path, monkeypatch):
    monkeypatch.setattr(Sas7bdatFileFormat, "chunksize", 2)
    records = await _records_from(sas7bdat_file)
    _assert_sas_records(records)


@pytest.mark.asyncio
async def test_sas7bdat_bad_text_byte_is_replaced(
    tmp_path: Path, sas7bdat_file: Path, caplog
):
    path = tmp_path / "bad-byte.sas7bdat"
    data = bytearray(sas7bdat_file.read_bytes())
    assert data[SAS_ENCODING_OFFSET] == SAS_CP1252_ENCODING
    data[SAS_ENCODING_OFFSET] = SAS_UTF8_ENCODING
    pear = data.find(b"pear")
    assert pear != -1
    data[pear + 1] = 0xFF
    path.write_bytes(data)
    with caplog.at_level(logging.WARNING, logger=PLUGIN_LOGGER):
        records = await _records_from(path)
    assert len(records) == 10
    assert records[0]["Column2"] == "p\ufffdar"
    assert records[1]["Column2"] == "dog"
    assert records[0]["Column3"] == 84.0
    assert isinstance(records[0]["Column3"], float)
    assert records[0]["Column4"] == datetime(1965, 12, 10)
    assert records[0]["Column8"] is None
    assert caplog.messages == [
        f"replaced undecodable bytes in 1 cells (columns: Column2) in {path}"
    ]


@pytest.mark.asyncio
async def test_replaced_sas_bytes_are_logged_once_per_file(
    tmp_path: Path, sas7bdat_file: Path, monkeypatch, caplog
):
    monkeypatch.setattr(Sas7bdatFileFormat, "chunksize", 1)
    path = tmp_path / "bad-bytes.sas7bdat"
    data = bytearray(sas7bdat_file.read_bytes())
    data[SAS_ENCODING_OFFSET] = SAS_UTF8_ENCODING
    # Row 0 and row 1 of Column2, so the bad cells fall in different chunks.
    row0 = data.find(b"pear     ")
    row1 = data.find(b"dog      pear     apple    pear ")
    assert row0 != -1 and row1 != -1 and row0 != row1
    data[row0 + 1] = 0xFF
    data[row1 + 1] = 0xFF
    path.write_bytes(data)
    with caplog.at_level(logging.WARNING, logger=PLUGIN_LOGGER):
        records = await _records_from(path)
    assert records[0]["Column2"] == "p\ufffdar"
    assert records[1]["Column2"] == "d\ufffdg"
    assert caplog.messages == [
        f"replaced undecodable bytes in 2 cells (columns: Column2) in {path}"
    ]


@pytest.mark.asyncio
async def test_unknown_sas_encoding_names_the_file(
    tmp_path: Path, sas7bdat_file: Path, caplog
):
    path = tmp_path / "unknown-encoding.sas7bdat"
    data = bytearray(sas7bdat_file.read_bytes())
    data[SAS_ENCODING_OFFSET] = 0
    path.write_bytes(data)
    with caplog.at_level(logging.WARNING, logger=PLUGIN_LOGGER):
        records = await _records_from(path)
    assert len(records) == 10
    assert records[0]["Column2"] == "pear"
    assert caplog.messages == [
        f"Could not infer SAS text encoding; decoding text as latin-1 in {path}."
    ]


def test_normalize_frame_keeps_numbers_as_floats():
    frame = pd.DataFrame(
        {
            "whole": [2.0, None, 4.0],
            "mixed": [2.0, 2.5, None],
            "when": [pd.Timestamp("2020-01-15"), pd.NaT, pd.Timestamp("2020-02-01")],
        }
    )
    records = _normalize_frame(frame)
    assert isinstance(records[0]["whole"], float)
    assert records[0]["whole"] == 2.0
    assert records[1]["whole"] is None
    assert records[2]["whole"] == 4.0
    assert isinstance(records[0]["mixed"], float)
    assert records[0]["mixed"] == 2.0
    assert records[1]["mixed"] == 2.5
    assert records[2]["mixed"] is None
    assert records[0]["when"] == datetime(2020, 1, 15)
    assert records[1]["when"] is None


def test_normalize_frame_replaces_undecodable_text():
    frame = pd.DataFrame({"name": [b"p\xffar", None, b"dog"]})
    records = _normalize_frame(frame, encoding="utf-8")
    assert records[0]["name"] == "p\ufffdar"
    assert records[1]["name"] is None
    assert records[2]["name"] == "dog"


def test_normalize_frame_decodes_non_ascii_column_names():
    def as_pandas_header(name: str) -> str:
        return name.encode("utf-8").decode("latin-1")

    groesse = as_pandas_header("Größe")
    citta = as_pandas_header("Città")
    angstrom = as_pandas_header("Å")
    padded = as_pandas_header("Grösse ")
    frame = pd.DataFrame(
        {
            groesse: [2.0],
            citta: ["rome"],
            angstrom: [1.0],
            padded: [3.0],
            "id": [1.0],
        }
    )
    replaced_headers: dict[str, str] = {}
    records = _normalize_frame(
        frame, encoding="utf-8", replaced_headers=replaced_headers
    )
    assert replaced_headers == {}
    assert list(records[0]) == ["Größe", "Città", "Å", "Grösse", "id"]
    assert records[0]["Größe"] == 2.0
    assert isinstance(records[0]["Größe"], float)
    assert records[0]["Città"] == "rome"
    assert records[0]["Å"] == 1.0
    assert isinstance(records[0]["Å"], float)
    assert records[0]["Grösse"] == 3.0
    assert records[0]["id"] == 1.0
    assert isinstance(records[0]["id"], float)


def test_replaced_sas_column_names_are_logged_once(monkeypatch, caplog):
    class _Reader:
        inferred_encoding = "utf-8"

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def __iter__(self):
            # Two chunks share one header, so the column name is counted once.
            yield pd.DataFrame({"c\xffol ": [b"ok"]})
            yield pd.DataFrame({"c\xffol ": [b"ok"]})

    monkeypatch.setattr(
        "nodestream_plugin_sas.sas7bdat.pd.read_sas",
        lambda *args, **kwargs: _Reader(),
    )
    with caplog.at_level(logging.WARNING, logger=PLUGIN_LOGGER):
        records = list(_iter_sas7bdat_records(BytesIO(), 1, "sample.sas7bdat"))
    assert records == [{"c\ufffdol": "ok"}, {"c\ufffdol": "ok"}]
    assert caplog.messages == [
        "replaced undecodable bytes in 1 column names (c\ufffdol) in sample.sas7bdat"
    ]


def test_replaced_sas_cells_and_column_names_share_one_warning(monkeypatch, caplog):
    class _Reader:
        inferred_encoding = "utf-8"

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def __iter__(self):
            yield pd.DataFrame({"c\xffol": [b"p\xff"]})
            yield pd.DataFrame({"c\xffol": [b"ok"]})

    monkeypatch.setattr(
        "nodestream_plugin_sas.sas7bdat.pd.read_sas",
        lambda *args, **kwargs: _Reader(),
    )
    with caplog.at_level(logging.WARNING, logger=PLUGIN_LOGGER):
        records = list(_iter_sas7bdat_records(BytesIO(), 1, "sample.sas7bdat"))
    assert records == [{"c\ufffdol": "p\ufffd"}, {"c\ufffdol": "ok"}]
    assert caplog.messages == [
        "replaced undecodable bytes in 1 cells (columns: c\ufffdol) "
        "and 1 column names (c\ufffdol) in sample.sas7bdat"
    ]
