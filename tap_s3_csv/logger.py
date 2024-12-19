import logging
import os
from pathlib import Path

import yaml


def _load_yaml_logging_config(path):
    with path.open() as f:
        return yaml.safe_load(f)


if "SINGER_SDK_LOG_CONFIG" in os.environ:
    log_config_path = Path(os.environ["SINGER_SDK_LOG_CONFIG"])
    logging.config.dictConfig(_load_yaml_logging_config(log_config_path))

internal_logger = logging.getLogger("internal")
user_logger = logging.getLogger("user")
