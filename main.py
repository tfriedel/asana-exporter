from loguru import logger

from asana_exporter.client import main as asana_main


def main() -> None:
    logger.info("Application started")
    asana_main()
    logger.info("Application finished")


if __name__ == "__main__":
    main()
