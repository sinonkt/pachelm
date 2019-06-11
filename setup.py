from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()
with open("requirements.txt") as f:
    install_requires = f.read().splitlines()

setup(
     name='pachelm',  
     version='0.0.4',
     scripts=['bin/pachelm'] ,
     author="Krittin Phornsiricharoenphant",
     author_email="oatkrittin@gmail.com",
     description="Pachyderm pipelines migrations & seeds tools, to ease pipelines deployment.",
     long_description=long_description,
     long_description_content_type="text/markdown",
     url="https://github.com/sinonkt/pachelm",
     packages=find_packages(),
     install_requires=install_requires,
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
 )