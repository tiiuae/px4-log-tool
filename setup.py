from setuptools import setup, find_packages

with open('requirements.txt', 'r') as f:
    install_requires = f.read().splitlines()

setup(
    name="px4-log-tool",
    version="0.2.0",
    description="A brief description of your project",
    author="Junior Sundar",
    author_email="junior.sundar@tii.ae",
    url="https://github.com/tiiuae/px4-log-tool",
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        "console_scripts": [
            # "px4-log-tool=px4_log_tool.cli:main",
            "px4-log-tool=px4_log_tool.cli:cli",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=install_requires,
    extras_require={
        "dev": [
            "pytest>=6.2",
            "black>=20.8b1",
        ],
    },
)
