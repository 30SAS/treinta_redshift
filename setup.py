from setuptools import setup, find_packages

setup(
    name='treinta_redshift',
    version='1.71',  # Incremento de la versión
    author='cristian.pinela',
    author_email='cristian.pinela@treinta.co',
    packages=find_packages(),
    license='LICENSE',
    description='paquete que permite interactuar pandas con redshift',
    # Asegurarse de que README.md no tiene errores de sintaxis
    long_description=open('README.md').read(),
    # Especifica que la descripción larga está en Markdown
    long_description_content_type='text/markdown',
    install_requires=[
        'boto3',
        'botocore',
        'numpy',
        'pandas',
        'pyarrow'
    ]
)
