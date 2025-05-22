from setuptools import setup, find_packages

# setup.py is now optional and kept for legacy support. Use pyproject.toml for modern builds.

setup(
    name="viirs_snpp_daily_gridding",
    version="0.1.0",
    description="VIIRS SNPP daily gridding tools",
    author="Your Name",
    packages=find_packages(),
    install_requires=[
        "joblib",
        # Add other dependencies here
    ],
    python_requires=">=3.7",
)
