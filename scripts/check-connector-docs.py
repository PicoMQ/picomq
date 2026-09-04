#!/usr/bin/env python3
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
CONNECTORS = ROOT / "connectors"
DOCS = ROOT / "website" / "pages" / "docs" / "connectors"
SIDEBAR = ROOT / "website" / ".vitepress" / "config.mts"

CONFIG_STRUCT = re.compile(r"pub struct \w*Config\b[^{]*\{(.*?)\n\}", re.S)
FIELD = re.compile(r"^\s*pub\s+(\w+)\s*:", re.M)
SERDE_RENAME = re.compile(r'#\[serde\([^)]*rename\s*=\s*"([^"]+)"')
FLATTEN = re.compile(r"#\[serde\([^)]*flatten")


def plugin_dirs(kind):
    for cargo in sorted((CONNECTORS / kind).glob("*/Cargo.toml")):
        yield cargo.parent


def config_fields(src_dir):
    fields = set()
    for rs in src_dir.rglob("*.rs"):
        text = rs.read_text()
        for match in CONFIG_STRUCT.finditer(text):
            body = match.group(1)
            if "#[cfg(test)]" in text[: match.start()] and "mod tests" in text[: match.start()]:
                continue
            for line_match in FIELD.finditer(body):
                name = line_match.group(1)
                preceding = body[: line_match.start()].rsplit("\n\n", 1)[-1]
                if FLATTEN.search(preceding):
                    continue
                renamed = SERDE_RENAME.search(preceding)
                fields.add(renamed.group(1) if renamed else name)
    return fields


def documented_keys(page):
    keys = set()
    for line in page.read_text().splitlines():
        if line.startswith("| `"):
            cell = line.split("|")[1].strip()
            for key in re.findall(r"`([^`]+)`", cell):
                keys.update(key.split("."))
    return keys


def sidebar_links():
    return set(re.findall(r"link: '(/docs/connectors/(?:sinks|sources)/[^']+)'", SIDEBAR.read_text()))


def main():
    failures = []
    links = sidebar_links()
    for kind in ("sinks", "sources"):
        for plugin in plugin_dirs(kind):
            name = plugin.name
            page = DOCS / kind / f"{name}.md"
            link = f"/docs/connectors/{kind}/{name}"
            if not page.exists():
                failures.append(f"{kind}/{name}: missing catalog page {page.relative_to(ROOT)}")
                continue
            if link not in links:
                failures.append(f"{kind}/{name}: not in sidebar ({SIDEBAR.relative_to(ROOT)})")
            fields = config_fields(plugin / "src")
            keys = documented_keys(page)
            for field in sorted(fields - keys):
                failures.append(f"{kind}/{name}: config field `{field}` not in {page.relative_to(ROOT)}")
            text = page.read_text()
            required = ("## Quick start", "## Configuration", "## Replay" if kind == "sinks" else "## State")
            for heading in required:
                if heading not in text:
                    failures.append(f"{kind}/{name}: missing section '{heading}'")
            if "pico-diagram" not in text:
                failures.append(f"{kind}/{name}: no diagram")
    for link in sorted(links):
        kind, name = link.rsplit("/", 2)[1:]
        if not (CONNECTORS / kind / name).exists():
            failures.append(f"sidebar link {link} has no plugin crate")
    for failure in failures:
        print(failure)
    print(f"{len(failures)} problem(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
