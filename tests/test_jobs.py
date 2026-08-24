"""
The load identifier: derived from the source, not generated per run.

Every property asserted here is one the pipeline's idempotency rests on. A
random id would satisfy none of them and would still look fine in a demo --
the rows would land, the counts would match, and the provenance would quietly
record one file as two loads.
"""

import re
import uuid

import pytest

from src import jobs

# The shape sql/schema.sql's CHECK constraint enforces. Asserted here as well
# as there because a mint that produced the wrong shape would fail at the
# write, ten minutes into a run, with a constraint name and no explanation.
UUID_TEXT = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


@pytest.fixture
def extract(tmp_path):
    """:returns: A small file standing in for a source extract."""
    path = tmp_path / "extract.csv"
    path.write_text("TXN_SEQ,ACCOUNT_ID\n1,ACC-1\n2,ACC-2\n", encoding="utf-8")
    return path


def test_the_id_matches_the_shape_the_column_requires(extract):
    identifier = jobs.job_id_for(extract)

    assert UUID_TEXT.match(identifier), identifier
    assert uuid.UUID(identifier).version == 5


def test_the_same_file_mints_the_same_id(extract):
    """
    The property everything else rests on. Re-running a load has to produce
    the same id, or the upsert writes the same rows under a second identity
    and the audit trail reports one delivery as two.
    """
    assert jobs.job_id_for(extract) == jobs.job_id_for(extract)


def test_the_id_follows_the_contents_not_the_name(tmp_path, extract):
    """
    A scheduler that stages each delivery under a timestamped filename would
    otherwise mint a new id for byte-identical data.
    """
    copy = tmp_path / "extract-2026-08-24T09-00-00.csv"
    copy.write_bytes(extract.read_bytes())

    assert jobs.job_id_for(copy) == jobs.job_id_for(extract)


def test_a_changed_byte_changes_the_id(extract):
    """
    The other direction, and the one that makes the id worth having: a
    corrected extract is a different load and has to be identifiable as one.
    """
    before = jobs.job_id_for(extract)
    extract.write_text(
        "TXN_SEQ,ACCOUNT_ID\n1,ACC-1\n2,ACC-3\n", encoding="utf-8"
    )

    assert jobs.job_id_for(extract) != before


def test_the_id_does_not_depend_on_the_process(extract):
    """
    Guards the reason ``hash()`` is not used. It is salted per process, so an
    id built on it would differ between two runs of the same file on the same
    machine -- and would pass a test that only ever ran in one process.
    """
    import subprocess
    import sys

    script = (
        "import sys; sys.path.insert(0, '.');"
        "from src import jobs;"
        f"print(jobs.job_id_for({str(extract)!r}))"
    )
    out = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )

    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == jobs.job_id_for(extract)


def test_a_missing_source_is_reported_as_such(tmp_path):
    """
    Rather than an id derived from nothing, which would be a valid-looking
    UUID for a file that is not there.
    """
    with pytest.raises(FileNotFoundError):
        jobs.job_id_for(tmp_path / "absent.csv")


def test_a_large_file_is_not_read_into_memory(tmp_path, monkeypatch):
    """
    The digest reads in chunks. Asserted by shrinking the chunk to something
    smaller than the file and checking the answer is unchanged -- a whole-file
    read would give the same answer too, so what this really pins is that the
    chunked path is exercised and correct.
    """
    path = tmp_path / "big.csv"
    path.write_bytes(b"x" * 5000)
    whole = jobs.job_id_for(path)

    monkeypatch.setattr(jobs, "CHUNK", 64)

    assert jobs.job_id_for(path) == whole


def test_a_digest_can_be_supplied_directly():
    """
    The Kafka path will identify a load by whatever the producer considered
    one, which need not be a file on this machine.
    """
    identifier = jobs.job_id_from_digest("batch-2026-08-24-001")

    assert UUID_TEXT.match(identifier)
    assert identifier == jobs.job_id_from_digest("batch-2026-08-24-001")
