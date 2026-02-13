import logging


def _parse_log_level(log_level):
    try:
        return getattr(logging, log_level.upper())
    except AttributeError:
        return logging.WARNING


def set_aws_log_level(log_level):
    level = _parse_log_level(log_level)
    logging.getLogger("boto3").setLevel(level)
    logging.getLogger("botocore").setLevel(level)
    logging.getLogger("nose").setLevel(level)
    logging.getLogger("s3transfer").setLevel(level)
    logging.getLogger("urllib3").setLevel(level)


def set_azure_log_level(log_level):
    level = _parse_log_level(log_level)
    logging.getLogger("azure").setLevel(level)
