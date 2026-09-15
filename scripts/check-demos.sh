#!/usr/bin/env bash
set -euo pipefail
core=$(cd "$(dirname "$0")/.." && pwd)
lessons=${1:-"$core/../kafka-lessons"}
migration=${2:-"$core/../kafka-migration-demo"}
for demo in "$lessons" "$migration"; do
  [[ -f "$demo/settings.gradle.kts" ]] || { echo "Missing demo build: $demo" >&2; exit 1; }
done
(cd "$core" && ./gradlew test assemble)
# Shared composite outputs must not be built concurrently by separate Gradle invocations.
for demo in "$lessons" "$migration"; do
  (cd "$demo" && ./gradlew -PkafkaDemoCore="$core" test bootJar)
done
python3 "$core/scripts/check-packages.py" \
  "$lessons/build/libs/kafka-lessons-0.1.0-SNAPSHOT.jar" \
  "$migration/build/libs/kafka-migration-demo-0.1.0-SNAPSHOT.jar"
