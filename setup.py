from setuptools import find_packages, setup

setup(
    name="dream_weaver",
    packages=find_packages(exclude=["dream_weaver_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
