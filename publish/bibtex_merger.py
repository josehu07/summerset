import sys
import os
import bibtexparser
import pprint

from typing import Dict

DROPBOX_DIR = os.path.expanduser("~/Dropbox/Apps/Overleaf")
PROJECTS = [
    "Crossword-eurosys26",
    "Bodega-sosp25",
    "Consistency-Model-socc25",
]
BIB_FILENAME = "references.bib"


def load_bib_files():
    bib_maps = {}

    for project in PROJECTS:
        bib_file = f"{DROPBOX_DIR}/{project}/{BIB_FILENAME}"
        with open(bib_file, "r") as f:
            bib_database = bibtexparser.load(f)
            bib_maps[project] = {entry["ID"]: entry for entry in bib_database.entries}

    return bib_maps


def titles_match(title1: str, title2: str) -> bool:
    return title1.strip().lower() == title2.strip().lower()


def prompt_manual_check(entry: Dict[str, str], existing: Dict[str, str]) -> bool:
    print(f"\n--- Manual check required: {entry['ID']}")
    print("\nOld:")
    pprint.pprint(existing)
    print("\nNew:")
    pprint.pprint(entry)

    decision = input("\nUse new entry? (y/n): ").strip().lower()
    if decision == "y":
        return False
    elif decision == "n":
        return True
    else:
        raise ValueError("Invalid input, must enter 'y' or 'n'!")


def process_one_entry(entry_id: str, entry: Dict[str, str], bib_merged: Dict[str, str]):
    if entry_id in bib_merged:
        # double check that titles match, otherwise prompt a manual check
        if not titles_match(entry["title"], bib_merged[entry_id]["title"]):
            keep_right = prompt_manual_check(entry, bib_merged[entry_id])
            if not keep_right:
                bib_merged[entry_id] = entry
    else:
        bib_merged[entry_id] = entry


def merge_bib_maps(bib_maps: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    bib_merged = {}

    for bib_map in bib_maps.values():
        for entry_id, entry in bib_map.items():
            process_one_entry(entry_id, entry, bib_merged)

    return bib_merged


if __name__ == "__main__":
    bib_maps = load_bib_files()
    bib_cnts = [len(m) for m in bib_maps.values()]
    print(f"\nLoaded: {bib_cnts} ∑={sum(bib_cnts)} entries")

    print("\nMerging bibtex entries...")
    bib_merged = merge_bib_maps(bib_maps)
    print(f"\nMerged: final={len(bib_merged)} entries, writing to file...")

    with open("references.bib", "w") as f:
        bib_database = bibtexparser.bibdatabase.BibDatabase()
        bib_database.entries = list(bib_merged.values())
        writer = bibtexparser.bwriter.BibTexWriter()
        f.write(writer.write(bib_database))
    print("See merged bibtex file: references.bib")
