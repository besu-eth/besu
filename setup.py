#!/usr/bin/env python3
"""
ARKHE OS — Pacote final incluindo Merces e todos os Substratos 0-121.
"""

import os
from setuptools import setup, find_packages

# Lê o README para a descrição longa
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Define as dependências base
install_requires = [
    "numpy>=1.24.0",
    "scipy>=1.11.0",
    "torch>=2.1.0",
    "qutip>=4.7.0",
    "web3>=6.0.0",
    "eth-account>=0.8.0",
    "cryptography>=41.0.0",
    "pyyaml>=6.0",
    "requests>=2.31.0",
    "fastapi>=0.104.0",
    "uvicorn>=0.24.0",
    "pydantic>=2.4.0",
    "plotly>=5.17.0",
    "pandas>=2.1.0",
]

# Extras opcionais
extras_require = {
    "fhe": ["tenseal>=0.3.0", "concrete-ml>=1.0.0"],
    "bci": ["brainflow>=5.0.0"],
    "blockchain": ["web3>=6.0.0", "eth-account", "substrate-interface"],
    "mpc": ["co-snarks>=0.1.0", "pymerces>=0.1.0"],  # dependências do Merces
    "all": [
        "tenseal>=0.3.0",
        "brainflow>=5.0.0",
        "web3>=6.0.0",
        "concrete-ml>=1.0.0",
        "co-snarks>=0.1.0",
    ],
}

setup(
    name="arkhe-os",
    version="∞.Ω.∇.121",
    author="ARKHE Sovereign Collective",
    author_email="sovereign@arkhe.os",
    description="ARKHE OS — World Model Operating System com Consenso Federado, Cofre Silencioso (Merces) e Renda Neural",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/arkhe-os/world-model",
    packages=find_packages(include=["arkhe_os", "arkhe_os.*"]),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "License :: Other/Proprietary License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: System :: Operating System",
        "Topic :: Security :: Cryptography",
    ],
    python_requires=">=3.10",
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points={
        "console_scripts": [
            "arkhe-render = arkhe_os.rendering.coherence_monitor:main",
            "arkhe-monitor = arkhe_os.monitoring.metacognitive_skyrmion_monitor:main",
            "arkhe-bci = arkhe_os.neural.brainflow_bci_driver:main",
            "arkhe-audit = arkhe_os.audit.compliance_dashboard:main",
            "arkhe-merces = arkhe_os.finance.merces.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
