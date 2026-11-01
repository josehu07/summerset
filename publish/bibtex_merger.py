import os

import bibtexparser
from bibtexparser.middlewares import NormalizeFieldKeys
from bibtexparser.model import Entry

DROPBOX_DIR = os.path.expanduser("~/Dropbox/Apps/Overleaf")
PROJECTS = [
    "Crossword-eurosys26",
    "Bodega-sosp25",
    "Consistency-Model-socc25",
]
BIB_FILENAME = "references.bib"


def load_bib_files() -> dict[str, dict[str, Entry]]:
    bib_maps = {}

    for project in PROJECTS:
        bib_file = f"{DROPBOX_DIR}/{project}/{BIB_FILENAME}"
        library = bibtexparser.parse_file(bib_file, append_middleware=[NormalizeFieldKeys()])
        bib_maps[project] = {entry.key: entry for entry in library.entries}

    return bib_maps


def titles_match(title1: str, title2: str) -> bool:
    return title1.strip().lower() == title2.strip().lower()


def prompt_manual_check(entry: Entry, existing: Entry) -> bool:
    print(f"\n--- Manual check required: {entry.key}")
    print("\nOld:")
    print(existing)
    print("\nNew:")
    print(entry)

    decision = input("\nUse new entry? (y/n): ").strip().lower()
    if decision == "y":
        return False
    elif decision == "n":
        return True
    else:
        raise ValueError("Invalid input, must enter 'y' or 'n'!")


def process_one_entry(entry_id: str, entry: Entry, bib_merged: dict[str, Entry]) -> None:
    if entry_id in bib_merged:
        # double check that titles match, otherwise prompt a manual check
        if not titles_match(entry["title"], bib_merged[entry_id]["title"]):
            keep_right = prompt_manual_check(entry, bib_merged[entry_id])
            if not keep_right:
                bib_merged[entry_id] = entry
    else:
        bib_merged[entry_id] = entry


def merge_bib_maps(bib_maps: dict[str, dict[str, Entry]]) -> dict[str, Entry]:
    bib_merged = {}

    for bib_map in bib_maps.values():
        for entry_id, entry in bib_map.items():
            process_one_entry(entry_id, entry, bib_merged)

    return bib_merged


def main() -> None:
    bib_maps = load_bib_files()
    bib_cnts = [len(m) for m in bib_maps.values()]
    print(f"\nLoaded: {bib_cnts} ∑={sum(bib_cnts)} entries")

    print("\nMerging bibtex entries...")
    bib_merged = merge_bib_maps(bib_maps)
    print(f"\nMerged: final={len(bib_merged)} entries, writing to file...")

    library = bibtexparser.Library(sorted(bib_merged.values(), key=lambda e: e.key))
    bib_format = bibtexparser.BibtexFormat()
    bib_format.indent = " "
    bibtexparser.write_file("references.bib", library, bibtex_format=bib_format)
    print("See merged bibtex file: references.bib")


if __name__ == "__main__":
    main()
