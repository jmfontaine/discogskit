"""discogskit: convert and load Discogs XML data dumps.

The names in ``__all__`` are the supported Python API; everything else may change without notice.
"""

from __future__ import annotations

from discogskit.decompress import DecompressError, ForeignXmlError
from discogskit.entities import ENTITIES, EntityDef, detect_entity
from discogskit.pipeline import (
    PipelineConfig,
    PipelineResult,
    ProfileData,
    UnhandledElementsError,
    run,
)
from discogskit.writers import OutputExistsError, Writer, get_file_writer, get_writer

__all__ = [
    "ENTITIES",
    "DecompressError",
    "EntityDef",
    "ForeignXmlError",
    "OutputExistsError",
    "PipelineConfig",
    "PipelineResult",
    "ProfileData",
    "UnhandledElementsError",
    "Writer",
    "detect_entity",
    "get_file_writer",
    "get_writer",
    "run",
]
