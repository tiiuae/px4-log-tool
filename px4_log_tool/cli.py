from tabnanny import verbose
import click
from px4_log_tool.processing_modules.converter import convert_ros2bag2csv
from px4_log_tool.runners import (
    db3_csv,
    ulog_csv,
    csv_db3,
    generate_ulog_metadata,
    ulog_db3
)


# Context object to store verbose flag
class CLIContext:
    def __init__(self):
        self.verbose: bool = False


@click.group()
@click.option("--verbose", is_flag=True, help="Enable verbose output.")
@click.pass_context
def cli(ctx, verbose):
    """
    px4-log-tool CLI Tool
    """
    ctx.ensure_object(CLIContext)
    ctx.obj.verbose = verbose


@click.command()
@click.argument("directory_address", type=click.Path(exists=True))
@click.option(
    "-r",
    "--resample",
    is_flag=True,
    help="Resample data to a given frequency (filter.yaml is mandatory)",
)
@click.option(
    "-c",
    "--clean",
    is_flag=True,
    help="Cleans directory and breadcrumbs leaving only unified.csv",
)
@click.option(
    "-m",
    "--merge",
    is_flag=True,
    help="Merge .csv files per .ulog files into merged.csv [leaves breadcrumbs].",
)
@click.option(
    "-f", "--filter", type=click.Path(exists=True), help="Path to the filter YAML file."
)
@click.option(
    "-o",
    "--output_dir",
    type=click.Path(exists=False),
    help="Module creates mirror directory tree of one with ULOGs with the CSV files in corresponding locations",
)
@click.pass_context
def ulog2csv(ctx, directory_address, resample, clean, merge, filter, output_dir):
    """
    Convert ulog files to CSV in DIRECTORY_ADDRESS using FILTER.
    """
    if ctx.obj.verbose:
        click.echo("Verbose mode enabled.")
    ulog_csv(verbose=ctx.obj.verbose, ulog_dir=directory_address, filter=filter, output_dir=output_dir, merge=merge, clean=clean, resample=resample)


@click.command()
@click.argument("directory_address", type=click.Path(exists=True))
@click.option(
    "-f", "--filter", type=click.Path(exists=True), help="Path to the filter YAML file."
)
@click.pass_context
@click.option(
    "-o",
    "--output_dir",
    type=click.Path(exists=False),
    help="Create mirror directory tree of CSVs directory and populate with DB3 bags. Operation in-place if none provided.",
)
def ulog2db3(ctx, directory_address, filter, output_dir):
    """
    Convert ulog files to DB3 in DIRECTORY_ADDRESS using FILTER.
    """
    if ctx.obj.verbose:
        click.echo("Verbose mode enabled.")
    ulog_db3(verbose=ctx.obj.verbose, directory_address=directory_address, filter=filter, output_dir=output_dir)

@click.command()
@click.argument("directory_address", type=click.Path(exists=True))
@click.option(
    "-f", "--filter", type=click.Path(exists=True), help="Path to the filter YAML file."
)
@click.pass_context
def db32csv(ctx, directory_address, filter):
    """
    Convert DB3 files to CSV in DIRECTORY_ADDRESS using FILTER.
    """
    if ctx.obj.verbose:
        click.echo("Verbose mode enabled.")
    db3_csv(directory_address=directory_address, filter=filter, verbose=ctx.obj.verbose)


@click.command()
@click.argument("directory_address", type=click.Path(exists=True))
@click.option(
    "-f", "--filter", type=click.Path(exists=True), help="Path to the filter YAML file."
)
@click.pass_context
def generate_metadata(ctx, directory_address, filter):
    """
    Generate metadata.json for ulog files in DIRECTORY_ADDRESS with metadata fields in FILTER. This operation is in place, so the .json files will be added into the provided directory.
    """
    if ctx.obj.verbose:
        click.echo("Verbose mode enabled.")
    generate_ulog_metadata(verbose=ctx.obj.verbose, directory_address=directory_address, filter=filter)


@click.command()
@click.argument("directory_address", type=click.Path(exists=True))
@click.option(
    "-f", "--filter", type=click.Path(exists=True), help="Path to the filter YAML file."
)
@click.option(
    "-o",
    "--output_dir",
    type=click.Path(exists=False),
    help="Create mirror directory tree of CSVs directory and populate with DB3 bags. Operation in-place if none provided.",
)
@click.pass_context
def csv2db3(ctx, directory_address, filter, output_dir):
    """
    Convert and merge CSV files in a directory into ROS 2 bag DB3 files in DIRECTORY_ADDRESS.
    """
    if ctx.obj.verbose:
        click.echo("Verbose mode enabled.")
    csv_db3(verbose=ctx.obj.verbose, directory_address=directory_address, filter=filter, output_dir=output_dir)


# Adding commands to the CLI
cli.add_command(ulog2csv)
cli.add_command(csv2db3)
cli.add_command(ulog2db3)
cli.add_command(db32csv)
cli.add_command(generate_metadata)

if __name__ == "__main__":
    cli()
