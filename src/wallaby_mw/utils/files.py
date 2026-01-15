import os
import logging
from pathlib import Path
from typing import List, Dict, Any
from urllib.parse import urlparse

def nonempty_file_exists(filepath):
    return os.path.exists(filepath) and os.path.getsize(filepath) > 0

def filename_from_url(url):
    return os.path.basename(urlparse(url).path)

def create_symlinks_from_patterns(
    base_dir: str | Path,
    filenames: List[str],
    patterns: List[Dict[str, Any]],
) -> Dict[str, str]:
    """
    Create canonical symlinks in `base_dir` based on pattern dictionaries.

    Each pattern dict can contain:
      - 'startswith': optional str that the filename must start with
      - 'endswith'  : optional str that the filename must end with
      - 'contains'  : optional str that must appear in the filename
      - 'link'      : REQUIRED str, the symlink name to create in base_dir
      - 'key'       : OPTIONAL str, the key name to use in the returned dict.
                      If not provided, 'link' will be used.
      - 'required'  : OPTIONAL bool, if True and no match is found, logs at
                      ERROR level instead of WARNING.

    Example pattern:
        {
            "startswith": "image.restored.i.",
            "endswith": ".cube.MilkyWay.contsub.fits",
            "link": "cube.fits",
            "key": "cube_fits",
            "required": True,
        }

    Returns
    -------
    dict
        Mapping from pattern['key'] (or 'link' if no key) to the absolute
        symlink path as a string.
    """

    base_path = Path(base_dir)
    outputs: Dict[str, str] = {}

    logging.debug(f"[create_symlinks_from_patterns] Base dir: {base_path}")
    logging.debug(f"[create_symlinks_from_patterns] Filenames: {filenames}")

    for pattern in patterns:
        logging.debug(f"\n[Pattern] Evaluating pattern: {pattern}")

        startswith = pattern.get("startswith")
        endswith = pattern.get("endswith")
        contains = pattern.get("contains")
        link_name = pattern.get("link")
        if not link_name:
            raise ValueError(f"Pattern missing required 'link' field: {pattern}")

        key = pattern.get("key", link_name)
        required = bool(pattern.get("required", False))

        matches: List[str] = []

        # --- Debug: describe what this pattern expects
        logging.debug(
            f"[Pattern] Conditions for {link_name}: "
            f"startswith={startswith!r}, endswith={endswith!r}, contains={contains!r}"
        )

        # --- Try matching each filename
        for f in filenames:
            logging.debug(f"  [Test] Checking filename: {f!r}")
            ok = True

            if startswith:
                if f.startswith(startswith):
                    logging.debug(f"    ✓ startswith({startswith!r}) passed")
                else:
                    logging.debug(f"    ✗ startswith({startswith!r}) FAILED")
                    ok = False

            if ok and endswith:
                if f.endswith(endswith):
                    logging.debug(f"    ✓ endswith({endswith!r}) passed")
                else:
                    logging.debug(f"    ✗ endswith({endswith!r}) FAILED")
                    ok = False

            if ok and contains:
                if contains in f:
                    logging.debug(f"    ✓ contains({contains!r}) passed")
                else:
                    logging.debug(f"    ✗ contains({contains!r}) FAILED")
                    ok = False

            if ok:
                logging.debug(f"    → MATCHED filename: {f!r}")
                matches.append(f)
            else:
                logging.debug(f"    → NOT matched")

        # --- If no matches for this pattern
        if not matches:
            msg = f"No match found for link '{link_name}' with pattern {pattern}"
            if required:
                logging.error(msg)
            else:
                logging.warning(msg)
            continue
        
        # --- Use the first match (stable behavior)
        src_name = matches[0]
        logging.debug(f"[Pattern] Using first matched file: {src_name}")

        src = base_path / src_name
        link_path = base_path / link_name

        if not src.exists():
            logging.warning(f"Matched file {src} does not exist on disk.")
            continue

        if not link_path.exists():
            try:
                link_path.symlink_to(src.name)
                logging.info(f"Created symlink {link_path} → {src.name}")
            except Exception as e:
                logging.warning(f"Failed to create symlink {link_path}: {e}")
        else:
            logging.info(f"Symlink or file already exists: {link_path}")

        outputs[key] = str(link_path.resolve())

    logging.debug(f"\n[create_symlinks_from_patterns] Outputs: {outputs}\n")
    return outputs
