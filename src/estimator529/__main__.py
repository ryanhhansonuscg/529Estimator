"""Module executed when running ``python -m 529Estimator``."""
from __future__ import annotations

import multiprocessing

from . import main

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
