import os
from datetime import datetime

# Environment configuration
print_level = int(os.getenv("PRINT_LEVEL", "0"))
enable_colors = os.getenv("LOG_COLORS", "true").lower() == "true"

class ColorCodes:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'

LOG_LEVELS = {
    0: ("INFO", ColorCodes.GREEN),
    1: ("WARN", ColorCodes.YELLOW),
    2: ("ERROR", ColorCodes.RED)
}

def get_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(
    msg: str,
    log_level: int = 0,  # 0=INFO, 1=WARN, 2=ERROR
    verbosity: bool = True,
    bold: bool = False,
    underline: bool = False
):
    # Original abs() logic
    if not verbosity or abs(log_level - 2) > print_level:
        return

    # Get level name and color
    level_info = LOG_LEVELS.get(log_level, LOG_LEVELS[0])
    level_name, level_color = level_info

    # Create timestamp
    timestamp = f"{ColorCodes.CYAN}{get_timestamp()}{ColorCodes.RESET}" if enable_colors else get_timestamp()
    
    # Format level prefix
    colored_level = f"{level_color}{level_name}{ColorCodes.RESET}" if enable_colors else level_name
    
    # Apply text formatting
    formatted_msg = msg
    if bold:
        formatted_msg = f"{ColorCodes.BOLD}{formatted_msg}{ColorCodes.RESET}"
    if underline:
        formatted_msg = f"{ColorCodes.UNDERLINE}{formatted_msg}{ColorCodes.RESET}"
    
    # Construct final message
    full_message = f"{timestamp} {colored_level} - {formatted_msg}"
    print(full_message)
