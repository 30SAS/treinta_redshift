from setuptools import setup, find_packages

setup(
    name='treinta_redshift',
    version='1.0.4',  # Incremento de la versión
    author='cristian.pinela',
    author_email='cristian.pinela@treinta.co',
    packages=find_packages(),
    license='LICENSE',
    description='paquete que permite interactuar pandas con redshift',
    long_description=open('README.md').read(),  # Asegurarse de que README.md no tiene errores de sintaxis
    long_description_content_type='text/markdown',  # Especifica que la descripción larga está en Markdown
    install_requires=[
        'boto3',
        'botocore',
        'numpy',
        'pandas',
        'pyarrow'
    ]
)
