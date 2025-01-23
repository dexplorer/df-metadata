import setuptools

setuptools.setup(
    name="metadata",
    version="1.0.0",
    # setup will install the app as command line module
    scripts=["./scripts/metadata"],
    author="Rajakumaran Arivumani",
    description="metadata python package install",
    url="https://github.com/dexplorer/df-metadata",
    # packages=setuptools.find_packages(),
    packages=[
        "metadata",
    ],
    # packages = find_packages(),
    install_requires=[
        "setuptools",
        "utils@git+https://github.com/dexplorer/utils#egg=utils-1.0.0",
    ],
    python_requires=">=3.12",
)
