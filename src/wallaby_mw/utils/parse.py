import re
from typing import List


def parse_sbid_groups(text: str) -> List[List[int]]:
    """
    Parse SBID groups from a string.

    Accepts either:
        - "[66866 67022] [68759 64378 60000]"
        - "[[66866,67022],[68759,64378]]"

    Returns: [[66866, 67022], [68759, 64378, 60000]]
    """
    s = text.strip()

    # Case 1: JSON-ish nested brackets [[..],[..]]
    if s.startswith("[[") and s.endswith("]]"):
        # Extract all numbers, then split by inner bracket groups.
        groups = []
        inner_groups = re.findall(r"\[([^\[\]]+)\]", s)  # contents of each [...]
        for g in inner_groups:
            nums = re.findall(r"\d+", g)
            if nums:
                groups.append([int(n) for n in nums])
        if not groups:
            raise ValueError(f"Could not parse any SBID groups from: {text}")
        return groups

    # Case 2: "[..] [..]" style (multiple bracketed groups)
    inner_groups = re.findall(r"\[([^\[\]]+)\]", s)
    if not inner_groups:
        raise ValueError(
            f"Expected bracketed groups like '[66866 67022] [68759 64378]'. Got: {text}"
        )

    groups: List[List[int]] = []
    for g in inner_groups:
        nums = re.findall(r"\d+", g)
        if not nums:
            continue
        groups.append([int(n) for n in nums])

    if not groups:
        raise ValueError(f"Could not parse any SBID groups from: {text}")

    return groups
