from pathlib import Path
from setuptools import setup, find_packages

long_description = Path("README.md").read_text()
reqs = Path("requirements.txt").read_text().strip().splitlines()

pkg = "sqlite_backup"
setup(
    name=pkg,
    version="0.1.6",
    url="https://github.com/seanbreckenridge/sqlite_backup",
    author="Sean Breckenridge",
    author_email="seanbrecke@gmail.com",
    description=("""A tool to copy sqlite databases you don't own"""),
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(include=[pkg]),
    install_requires=reqs,
    package_data={pkg: ["py.typed"]},
    zip_safe=False,
    keywords="database sqlite",
    python_requires=">=3.7",
    entry_points={"console_scripts": ["sqlite_backup = sqlite_backup.__main__:main"]},
    extras_require={
        "testing": [
            "pytest",
            "mypy",
            "flake8",
            "pytest-reraise",
        ]
    },
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
