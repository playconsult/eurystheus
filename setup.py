from distutils.core import setup

setup(
    name='QueueProcessor',
    version='0.1.1',
    author='Alastair McFarlane',
    author_email='alastair@play-consult.co.uk',
    packages=['queue_processor'],
    scripts=[],
    url='http://pypi.python.org/pypi/TowelStuff/',
    license='LICENSE.txt',
    description='Queue processor',
    long_description=open('README.txt').read(),
    install_requires=[
        "boto3 >= 1.3.1"
    ],
)
