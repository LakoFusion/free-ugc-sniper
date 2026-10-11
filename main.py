import asyncio
import sys
import logging
import os
import subprocess
import importlib.util

def check_and_install_modules():
    req_file = "requirements.txt"
    if not os.path.exists(req_file):
        return

    mapping = {
        "discord.py": "discord"
    }
    stdlib = {"datetime", "socket", "uuid", "typing", "json", "os", "sys", "asyncio", "logging", "time", "timeit"}

    with open(req_file, "r") as f:
        packages = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    for pkg in packages:
        pkg_name = pkg.split('==')[0].split('>=')[0].split('<=')[0].split('<')[0].split('>')[0].strip()

        if pkg_name.lower() in stdlib:
            continue

        module_name = mapping.get(pkg_name, pkg_name)

        if importlib.util.find_spec(module_name) is None:
            print(f"[*] Thấy module '{pkg_name}' chưa được cài đặt. Đang tự động cài đặt...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
                print(f"[+] Đã cài đặt xong '{pkg_name}'.")
            except Exception as e:
                print(f"[!] Lỗi khi cài đặt '{pkg}': {e}")
                logging.getLogger(__name__).error(
                    f"Lỗi khi cài đặt '{pkg}': {e}"
                )

from logging.handlers import RotatingFileHandler

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class NoisyRuntimeFileFilter(logging.Filter):
    """Drop common non-actionable runtime messages from the file log."""

    _SUPPRESSED_PATTERNS = (
        "devapi/bad location",
        "invalidsalelocation",
        "invalid location",
        "bad sale location",
        "bad location",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage().lower()
        return not any(pattern in message for pattern in self._SUPPRESSED_PATTERNS)


def setup_logging():
    os.makedirs("logs", exist_ok=True)
    fmt = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] %(name)s – %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = RotatingFileHandler(
        "logs/sniper_logs.txt", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    fh.addFilter(NoisyRuntimeFileFilter())

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)
    ch.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(fh)
    root.addHandler(ch)


logger = logging.getLogger(__name__)


async def main():
    from src.sniper import Sniper

    sniper = Sniper()
    try:
        await sniper.main()
    except KeyboardInterrupt:
        print("\n[!] Dừng chương trình...")
    except Exception as e:
        logger.error(f"Lỗi chương trình chính: {e}", exc_info=True)


if __name__ == "__main__":
    os.system("")
    setup_logging()
    check_and_install_modules()
    asyncio.run(main())
