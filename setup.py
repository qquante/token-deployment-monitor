"""
Bankr Monitor - Setup configuration for pip installation.

Usage:
    pip install -e .  # Development install
    pip install .     # Regular install
"""

from setuptools import setup, find_packages

setup(
    name="bankr-monitor",
    version="1.0.0",
    description="Forensic analysis tool for Bankr token deployments on Base",
    author="Bankr Monitor Team",
    python_requires=">=3.10",
    packages=find_packages(exclude=["tests", "data"]),
    install_requires=[
        "web3>=6.0.0",
        "eth-abi>=4.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "bankr-monitor=scripts.run_analysis:main",
        ],
    },
)
