from setuptools import find_packages, setup

setup(
    name="test_proj",
    packages=find_packages(exclude=["test_proj_tests"]),
    install_requires=["git+https://github.com/dagster-io/dagster@master", "dagster-cloud"],
    extras_require={"dev": ["dagit", "pytest"]},
)
