"""
The identity of one load, derived rather than generated.

Every row the pipeline writes carries a ``sync_job_id`` naming the load it came
from, and the completion event carries the same id, which is what lets a reader
go from "job X cleaned 265,195 rows" back to the rows themselves.

The id is a UUIDv5 over the source's *contents*, not a ``uuid4()``. That choice
is the difference between a pipeline that is idempotent and one that merely
does not crash when re-run:

* With a random id, re-running the same file writes the same rows tagged as a
  different load. The upsert overwrites them, so the table is right, but the
  provenance is now a lie -- one file appears in the audit trail as two loads
  and there is no way to tell a replay from a genuine second delivery.
* With a derived id, re-running the same file produces the same id, the same
  rows and the same values. The second run changes nothing a reader can
  observe, which is the actual definition of idempotent and the property a
  re-delivering consumer needs.

Contents rather than the path, because a file copied to a new name is the same
load; a scheduler that stages each delivery under a timestamped filename would
otherwise mint a new id for identical data. And contents rather than modified
time, because a checkout, a copy or a restore from backup all move mtime
without changing a byte.

The same reasoning ``tests/harness/sample.py`` gives for using SHA-256 over
``hash()`` applies here and matters more: ``hash()`` is salted per process, so
an id built on it would differ between two runs of the same file on the same
machine.
"""

import hashlib
import uuid
from pathlib import Path

# The namespace every job id is minted under. A fixed UUID, so the derivation
# is reproducible across machines and versions; a project-specific one, so an
# id from this pipeline cannot collide with an id some other system derived
# from the same bytes under the standard namespaces.
#
# Never change this. It is not a version number -- editing it re-mints every
# id the pipeline would produce, which means a file already loaded would come
# back as a new job and be written a second time under a new identity.
NAMESPACE = uuid.UUID("6f5b3d2a-9c14-5e77-b0a8-1d4e2f7c8a30")

# Read in chunks rather than whole. The forecast extract is 68 MB today and
# there is no reason the next one is not 10 GB; a derivation that needs the
# file in memory would be a size limit hidden inside an id.
CHUNK = 1024 * 1024


def content_digest(path: str | Path) -> str:
    """
    :param path: File to digest.
    :returns: SHA-256 of the file's bytes, hex.
    :raises FileNotFoundError: If the path does not exist, which is a clearer
        failure here than a job id derived from nothing.
    """
    path = Path(path)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def job_id_for(path: str | Path) -> str:
    """
    Mints the load identifier for a source file.

    :param path: The file being loaded.
    :returns: A UUIDv5 as the lowercase hyphenated string the
        ``sync_job_id`` column's CHECK constraint requires.
    """
    return str(uuid.uuid5(NAMESPACE, content_digest(path)))


def job_id_from_digest(digest: str) -> str:
    """
    The same derivation, for a caller that already has the digest or that is
    identifying something other than a file -- a Kafka batch, say, keyed by
    whatever the producer considered one load.

    :param digest: Any stable string identifying the load.
    :returns: The corresponding UUIDv5, as a string.
    """
    return str(uuid.uuid5(NAMESPACE, digest))
