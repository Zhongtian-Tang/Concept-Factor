from setuptools import setup, find_packages

setup(
    name='concept_factor',
    version='0.1.0',
    author='汤中天',
    author_email='799138793@qq.com',
    packages=find_packages("concept_factor", "concept_factor.*"),
    description='处理股票概念因子的脚本',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url='https://github.com/Zhongtian-Tang/Concept-Factor',
    install_requires=[
        'loguru',
        'requests==2.28.1',
        'pandas==1.5.3',
        'sqlalchemy==1.4.39',
        'iFinDPy',
    ],
)
