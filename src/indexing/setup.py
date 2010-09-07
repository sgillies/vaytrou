from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='indexing',
      version=version,
      description="Spatial indexing in atomic batches",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='spatial index batch',
      author='Sean Gillies',
      author_email='sean.gillies@gmail.com',
      url='',
      license='BSD',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
