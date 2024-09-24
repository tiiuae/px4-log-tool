import argparse
from srta_drone_dataset.ulog_converter import ulog_converter

class NorgCLI():
    def __init__(self, parser):
        self.parser = parser
        self.subparsers = {}

def main():
    ulog_converter()
