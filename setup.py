from setuptools import setup, find_packages

setup(
    name='concept_helper',
    version='0.1.0',
    author='汤中天',
    author_email='799138793@qq.com',
    packages=find_packages(),
    description='A script to handle stock concept factors.',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url='https://github.com/Zhongtian-Tang/Concept-Factor',
    install_requires=[
        'logging',
        'requests==<version>',
        'pandas==<version>',
        'sqlalchemy==<version>',
        'iFinDPy==<version>',
    ],
)
