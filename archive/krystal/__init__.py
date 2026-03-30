"""
Krystal - End-to-End Testing Agent

A powerful testing framework built on CrewAI for automating end-to-end testing workflows.
"""

__version__ = "1.0.0"
__author__ = "Krystal Team"

from krystal.config import ConfigManager
from krystal.runner import TestRunner
from krystal.report import ReportGenerator

__all__ = ["ConfigManager", "TestRunner", "ReportGenerator"]
