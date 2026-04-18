from setuptools import find_packages, setup

setup(
    name="saga_common",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pydantic>=2.0",
        "aiokafka>=0.10.0",
        "httpx>=0.26.0",
    ],
    python_requires=">=3.11",
)
