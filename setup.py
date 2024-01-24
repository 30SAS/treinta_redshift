from setuptools import setup, find_packages

# Función para leer el contenido de requirements.txt
def read_requirements():
    with open('requirements.txt') as req:
        return req.read().splitlines()

setup(
    name='treinta_redshift',
    version='0.1.0',
    author='cristian.pinela',
    author_email='cristian.pinela@treinta.co',
    packages=find_packages(),
    license='LICENSE',
    description='Librería que permite programar sobre redshift utilizando python para tareas de data engineering',
    long_description=open('README.md').read(),
    install_requires=read_requirements(),  # Usar la función aquí
)
