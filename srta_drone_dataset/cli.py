import click
from srta_drone_dataset.ulog_converter import ulog_converter

@click.group()
def cli():
    """
    srta-drone-dataset CLI Tool
    """

@click.command()
@click.argument('directory_address', type=click.Path(exists=True))
@click.option('-i', '--input-format', required=True, type=click.Choice(['ulog', 'db3'], case_sensitive=False), help='Input file format.')
@click.option('-o', '--output-format', required=True, type=click.Choice(['csv', 'db3'], case_sensitive=False), help='Output file format.')
@click.option('-f', '--filter', type=click.Path(exists=True), required=True, help='Path to the filter YAML file.')
def convert(directory_address, input_format, output_format, filter):
    """
    Convert drone dataset in the DIRECTORY_ADDRESS from INPUT format to OUTPUT format using FILTER file.
    """
    click.echo(f"Converting dataset in {directory_address}")
    click.echo(f"Input format: {input_format}")
    click.echo(f"Output format: {output_format}")
    click.echo(f"Using filter: {filter}")

def main():
    # ulog_converter()
    return

cli.add_command(convert)
if __name__ == '__main__':
    cli()
