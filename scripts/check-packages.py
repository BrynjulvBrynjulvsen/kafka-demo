#!/usr/bin/env python3
"""Check built demo ownership and shared resource packaging without running either app."""
import io
import sys
from pathlib import Path
from zipfile import ZipFile


def verify(path, owner, forbidden):
    with ZipFile(path) as archive:
        names = set(archive.namelist())
        assert f"BOOT-INF/classes/io/bekk/{owner}/Application.class" in names, path
        assert not any(f"io/bekk/{forbidden}/" in name for name in names), path
        entry = "index.html" if owner == "kafkalessons" else "migration.html"
        other = "migration.html" if owner == "kafkalessons" else "index.html"
        assert f"BOOT-INF/classes/static/{entry}" in names, path
        assert f"BOOT-INF/classes/static/{other}" not in names, path
        libraries = [name for name in names if name.startswith("BOOT-INF/lib/presentation-")]
        assert len(libraries) == 1, libraries
        with ZipFile(io.BytesIO(archive.read(libraries[0]))) as presentation:
            assets = set(presentation.namelist())
            prefix = "META-INF/resources/kafka-demo/"
            for asset in ["css/theme.css", "js/deck.js", "js/live-client.js", "js/kafka-client.js", "vendor/reveal/reveal.esm.js", "vendor/reveal/LICENSE"]:
                assert prefix + asset in assets, asset
            assert not any(name.endswith(".html") for name in assets), assets
            assert not any("migration" in name or "experiment" in name for name in assets), assets
        backends = [name for name in names if name.startswith("BOOT-INF/lib/backend-")]
        assert len(backends) == 1, backends
        with ZipFile(io.BytesIO(archive.read(backends[0]))) as backend:
            assert not any("Migration" in name or "Experiment" in name or name.endswith("application.yml") for name in backend.namelist())
        print(f"PASS {Path(path).name}: own application/deck, shared libraries, no other demo")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: check-packages.py LESSONS_BOOT_JAR MIGRATION_BOOT_JAR")
    verify(sys.argv[1], "kafkalessons", "kafkamigration")
    verify(sys.argv[2], "kafkamigration", "kafkalessons")
