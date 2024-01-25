from setuptools import setup, find_packages

setup(
    name='treinta_redshift',
    version='0.1.4',  # Incremento de la versión
    author='cristian.pinela',
    author_email='cristian.pinela@treinta.co',
    packages=find_packages(),
    license='LICENSE',
    description='Libreria para data engineering',
    long_description=open('README.md').read(),  # Asegurarse de que README.md no tiene errores de sintaxis
    long_description_content_type='text/markdown',  # Especifica que la descripción larga está en Markdown
    install_requires=[
        'boto3==1.34.23',
        'botocore==1.34.23',
        'numpy==1.25.0',
        'pandas==2.0.2',
        'pyarrow==13.0.0',
    ]
)
