import sys
import os
print_level = int(os.getenv("PRINT_LEVEL", "0"))  # Default to 0 (basic info)

def progress_bar(progress: float, verbose: bool = False) -> None:
    """
    Displays a simple progress bar in the console.

    Args:
        progress: A float between 0.0 and 1.0 representing the progress percentage.
    """

    bar_length = 50
    filled_length = int(bar_length * progress)
    bar = f"[{'=' * filled_length}{' ' * (bar_length - filled_length)}]"
    if verbose and print_level == 2:
        sys.stdout.write(f"\r{bar} {progress * 100:.1f}%")
        sys.stdout.flush()

