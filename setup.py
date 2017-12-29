from setuptools import setup, find_packages


with open('./magne/__init__.py', 'r') as f:
    version_marker = '__version__ = '
    for line in f:
        if line.startswith(version_marker):
            _, version = line.split(version_marker)
            version = version.strip().strip('"')
            break
    else:
        raise RuntimeError("Version marker not found.")

setup(
    name="magne",
    version=version,
    author="allenling",
    author_email="allenling3@gmail.com",
    description="A distributed task queue with curio, support rabbitmq only.",
    url="https://github.com/allenling/magne",
    packages=find_packages(),
    include_package_data=True,
    install_requires=['curio >= 0.8.0',
                      'pika  >= 0.11,<0.12',
                      ],
    python_requires=">=3.6",
    license='MIT',
    entry_points={"console_scripts": ["magne = magne.run:main"]},
    classifiers=["Programming Language :: Python :: 3.6",
                 "Programming Language :: Python :: 3.7",
                 "Programming Language :: Python :: 3 :: Only",
                 "Topic :: System :: Distributed Computing",
                 'License :: OSI Approved :: MIT License',
                 ],
)
