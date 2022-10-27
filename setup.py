import setuptools

setuptools.setup(
    name="axolotl",
    version="0.2.0",
    scripts=[
    ],
    author="Joint Genome Institute - Genome Analysis R&D",
    author_email="zhongwang@lbl.gov",
    description=("A Python PGenomics Library based on pySpark"),
    long_description=(
        "Please see our GitHub page: "
        "https://github.com/zhongwang/axolotl"
    ),
    url="https://github.com/zhongwang/axolotl",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.6",
    test_suite="tests",
    install_requires=[
        "pyspark"
    ]
)