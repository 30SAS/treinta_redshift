from setuptools import setup, find_packages

setup(
    name='treinta_redshift',
    version='0.1.1',
    author='cristian.pinela',
    author_email='cristian.pinela@treinta.co',
    packages=find_packages(),
    license='LICENSE',
    description='Libreria para data engineering',
    long_description='Librería que permite programar sobre redshift utilizando python para tareas de data engineering',
    long_description_content_type='text/markdown',  # Specify Markdown format
    install_requires=[
        'boto3==1.34.23',
        'botocore==1.34.23',
        'numpy==1.25.0',
        'pandas==2.0.2',
        'pyarrow==13.0.0',
    ]
)