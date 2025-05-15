import os
import re

from setuptools import find_packages, setup

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))


def get_version():
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    with open(
        os.path.join(SCRIPT_DIR, "oasis_data_manager", "__init__.py"), encoding="utf-8"
    ) as init_py:
        return re.search("__version__ = ['\"]([^'\"]+)['\"]", init_py.read()).group(1)


def get_install_requirements():
    with open(
        os.path.join(SCRIPT_DIR, "requirements-package.in"), encoding="utf-8"
    ) as reqs:
        return reqs.readlines()


def get_optional_requirements():
    with open(
        os.path.join(SCRIPT_DIR, "optional-package.in"), encoding="utf-8"
    ) as reqs:
        return {"extra": reqs.readlines()}


version = get_version()


setup(
    name="oasis-data-manager",
    version=version,
    packages=find_packages(exclude=("tests", "tests.*", "tests.*.*")),
    include_package_data=True,
    package_data={
        "": [],
    },
    exclude_package_data={
        "": ["__pycache__", "*.py[co]"],
    },
    description="",
    long_description="",
    long_description_content_type="text/markdown",
    url="https://github.com/OasisLMF/OasisDataManager",
    author="Oasis LMF",
    author_email="support@oasislmf.org",
    keywords="",
    python_requires=">=3.6",
    install_requires=get_install_requirements(),
    extras_require=get_optional_requirements(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
    ],
)
