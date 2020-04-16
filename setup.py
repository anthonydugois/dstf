import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(name="dstf",
                 version="0.0.4",
                 packages=setuptools.find_packages(),
                 url="https://github.com/anthonydugois/dstf",
                 author="Anthony Dugois",
                 author_email="hello@anthonydugois.com",
                 description="A framework for Deterministic Scheduling Theory",
                 keywords="deterministic scheduling theory",
                 long_description=long_description,
                 long_description_content_type="text/markdown",
                 classifiers=["License :: OSI Approved :: MIT License",
                              "Operating System :: OS Independent",
                              "Programming Language :: Python :: 3",
                              "Topic :: Scientific/Engineering"])
