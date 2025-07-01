import hashlib
from pathlib import Path

import structlog
from structlog import BoundLogger

from schemachange.jinja.JinjaTemplateProcessor import JinjaTemplateProcessor
from schemachange.config.render_config import RenderConfig
from schemachange.config.get_merged_config import get_merged_config
from schemachange.config.redact_config_secrets import redact_config_secrets
from schemachange.action.deploy import deploy
from schemachange.action.render import render
from schemachange.session.session_factory import get_db_session

module_logger = structlog.getLogger(__name__)
SCHEMACHANGE_VERSION = "4.0.1"

def main():
    config = get_merged_config(logger=module_logger)
    redact_config_secrets(config_secrets=config.secrets)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(config.log_level),
    )
    logger = structlog.getLogger()
    logger = logger.bind(schemachange_version=SCHEMACHANGE_VERSION)
    config.log_details()

    if config.subcommand == "render":
        render(
            config=config,
            script_path=config.script_path,
            logger=logger,
        )
    else:
        db_session = get_db_session(
            db_type=config.db_type,
            logger=logger,
            session_kwargs=config.get_session_kwargs(),
        )
        deploy(config=config, session=db_session)

if __name__ == "__main__":
    main()