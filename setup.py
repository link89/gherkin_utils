# coding: utf-8
from distutils.core import setup
setup(name='gherkin_utils',
      packages=['gherkin_utils'],
      version='0.0.1-snapshot',
      description='Gherkin Utils',
      author='Henry Xu',
      url='https://github.com/link89/gherkin_utils',
      license='MIT',
      keywords=['gherkin', 'bdd'],
      classifiers=['Programming Language :: Python',
                   'Programming Language :: Python :: 2',
                   'Programming Language :: Python :: 3',
                   ],
      install_requires=['GitPython >= 2.1.8',
                        'base32-crockford',
                        'pycrypto',
                        ],
      dependency_links=['https://github.com/link89/gherkin-python/archive/hb_parser.zip#egg=package-1.0'],
      )
