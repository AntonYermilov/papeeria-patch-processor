from typing import List
from diff_match_patch import patch_obj, diff_match_patch


def invert(patch: patch_obj) -> patch_obj:
    new_patch = patch_obj()
    new_patch.start1, new_patch.start2 = patch.start2, patch.start1
    new_patch.length1, new_patch.length2 = patch.length2, patch.length1

    for op, data in patch.diffs:
        if op == diff_match_patch.DIFF_INSERT:
            new_patch.diffs.append((diff_match_patch.DIFF_DELETE, data))
        elif op == diff_match_patch.DIFF_DELETE:
            new_patch.diffs.append((diff_match_patch.DIFF_INSERT, data))
        else:
            new_patch.diffs.append((op, data))

    return new_patch


def merge_patches(patches1: List[patch_obj], patches2: List[patch_obj]) -> List[patch_obj]:
    return patches1 + patches2


def invert_patches(patches: List[patch_obj]) -> List[patch_obj]:
    patches = [invert(patch) for patch in reversed(patches)]
    return patches
