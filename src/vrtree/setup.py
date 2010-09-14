from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(name='vrtree',
      version=version,
      description="Rtree backend for Vaytrou",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='spatial indexing rtree',
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
