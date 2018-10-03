
from setuptools import setup, find_packages

DEPENDENCIES = [
    "anthill-common"
]

setup(
    name='anthill-leaderboard',
    package_data={
      "anthill.leaderboard": ["anthill/leaderboard/sql", "anthill/leaderboard/static"]
    },
    setup_requires=["pypigit-version"],
    git_version="0.1.0",
    description='User ranking service for Anthill Platform',
    author='desertkun',
    license='MIT',
    author_email='desertkun@gmail.com',
    url='https://github.com/anthill-platform/anthill-leaderboard',
    namespace_packages=["anthill"],
    packages=find_packages(),
    zip_safe=False,
    install_requires=DEPENDENCIES
)
