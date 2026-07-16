#!/usr/bin/env bash
#
# Renders every C4 diagram in this directory to SVG.
#
# The SVGs are committed, so nobody needs this script to *read* the docs -- it
# exists so that editing a .puml and regenerating is one command rather than a
# hunt for the toolchain. Run it after changing any .puml, and commit both the
# source and the regenerated SVG.
#
#   ./docs/diagrams/render.sh
#
# Requires Java. PlantUML itself is downloaded once into .cache/ (gitignored)
# and pinned to the version below, so a rerun on another machine produces the
# same output rather than whatever the latest release happens to draw.
#
# Graphviz is deliberately NOT required: -Playout=smetana selects PlantUML's
# own pure-Java layout engine. A dependency on a system `dot` binary is the
# usual reason diagram tooling stops working on a colleague's machine.
set -euo pipefail

PLANTUML_VERSION="1.2026.6"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cache="$here/.cache"
jar="$cache/plantuml-$PLANTUML_VERSION.jar"

command -v java >/dev/null || {
  echo "error: java is required to render the diagrams (the committed SVGs need no toolchain)" >&2
  exit 1
}

if [[ ! -f "$jar" ]]; then
  mkdir -p "$cache"
  url="https://github.com/plantuml/plantuml/releases/download/v$PLANTUML_VERSION/plantuml-$PLANTUML_VERSION.jar"
  echo "downloading plantuml $PLANTUML_VERSION -> $jar"
  curl -sSfL --retry 3 -o "$jar.tmp" "$url"
  mv "$jar.tmp" "$jar"
fi

# lib/ holds the vendored C4-PlantUML macros and is included by the diagrams,
# never rendered on its own.
cd "$here"
shopt -s nullglob
sources=(*.puml)
(( ${#sources[@]} )) || { echo "no .puml sources found in $here" >&2; exit 1; }

echo "rendering ${#sources[@]} diagram(s) with smetana layout"
java -jar "$jar" -tsvg -Playout=smetana -nometadata "${sources[@]}"

for src in "${sources[@]}"; do
  out="${src%.puml}.svg"
  [[ -f "$out" ]] || { echo "error: $src produced no $out" >&2; exit 1; }
  printf '  %-34s -> %s\n' "$src" "$out"
done
