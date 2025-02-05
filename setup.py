from setuptools import setup, find_packages

setup(
    name="dq_tool",
    version="0.1.0",
    author="Salim Sheikh",
    author_email="your.email@example.com",
    description="A data quality tool for managing and validating data.",
    #long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/salimhussain/dq_tool_latest",
    packages=find_packages(include=['dq_tool', 'dq_tool.*']),
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9,<3.10",
    install_requires=[
        "pyspark>=3.0.0,<4.0.0",
        "great_expectations==1.3.3",
        "pandas>=1.3.0,<2.0.0",
    ],
    entry_points={
        "console_scripts": [
            "main=dq_tool.main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "dq_tool": ["config/*.yml", "templates/*.html"],
    },
    zip_safe=False,
)
