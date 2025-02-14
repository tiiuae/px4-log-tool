from setuptools import setup, find_packages

with open('requirements.txt', 'r') as f:
    install_requires = f.read().splitlines()

setup(
    name="px4-log-tool",
    version="0.5.0",
    description="The All-in-One tool to work with PX4 log files.",
    author="Junior Sundar",
    author_email="junior.sundar@tii.ae",
    url="https://github.com/tiiuae/px4-log-tool",
    packages=find_packages(),
    package_data= {
        "px4-log-tool": ["msg_reference.csv"],
    },
    include_package_data=True,
    entry_points={
        "console_scripts": [
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
