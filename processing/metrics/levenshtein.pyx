import numpy as np
cimport numpy as np
cimport cython

from typing import Tuple


@cython.boundscheck(False)
@cython.wraparound(False)
cdef compute(int[:] s1, int[:] s2):
    cdef Py_ssize_t sz1 = s1.shape[0]
    cdef Py_ssize_t sz2 = s2.shape[0]

    cdef int S = 0
    cdef int I = 0
    cdef int D = 0

    dist_np = np.zeros((sz1 + 1, sz2 + 1), dtype=np.intc)
    cdef int[:, :] dist = dist_np
    cdef Py_ssize_t i, j

    for i in range(sz1 + 1):
        dist[i, 0] = i
    for j in range(sz2 + 1):
        dist[0, j] = j

    for i in range(1, sz1 + 1):
        for j in range(1, sz2 + 1):
            dist[i, j] = min(dist[i, j - 1] + 1, dist[i - 1, j] + 1, dist[i - 1, j - 1] + (s1[i - 1] != s2[j - 1]))

    i = sz1
    j = sz2

    while i > 0 and j > 0:
        if dist[i, j] == dist[i - 1, j - 1]:
            i -= 1
            j -= 1
            continue
        if dist[i, j] == dist[i - 1, j - 1] + 1:
            i -= 1
            j -= 1
            S += 1
            continue
        if dist[i, j] == dist[i - 1, j] + 1:
            i -= 1
            D += 1
        else:
            j -= 1
            I += 1

    D += i
    I += j

    assert S + I + D == dist[sz1, sz2]
    return S, I, D


# Returns levenshtein distance as a tuple of 3 values: #substitutions, #insertions, #deletions
def levenshtein(np.ndarray s1, np.ndarray s2) -> Tuple[int, int, int]:
    return compute(s1.astype(np.intc), s2.astype(np.intc))
