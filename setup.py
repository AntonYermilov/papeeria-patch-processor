import numpy
from pathlib import Path
from distutils.core import setup
from Cython.Build import cythonize


setup(
    ext_modules=cythonize([
        str(Path('processing', 'metrics', 'levenshtein.pyx'))
    ], annotate=True),
    include_dirs=[numpy.get_include()]
)
