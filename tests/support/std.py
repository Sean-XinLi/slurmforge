from __future__ import annotations

import io
import json
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import yaml

__all__ = [
    "Namespace",
    "Path",
    "io",
    "json",
    "patch",
    "redirect_stderr",
    "redirect_stdout",
    "replace",
    "tempfile",
    "unittest",
    "yaml",
]
