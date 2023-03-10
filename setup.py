from setuptools import find_packages, setup

setup(
    name="test_proj",
    packages=find_packages(exclude=["test_proj_tests"]),
    install_requires=["dagster-cloud"],
    extras_require={"dev": ["dagit", "pytest"]},
)
