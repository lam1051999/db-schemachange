def render(config: RenderConfig, script_path: Path, logger: BoundLogger) -> None:
    """
    Renders the provided script.

    Note: does not apply secrets filtering.
    """
    # Always process with jinja engine
    jinja_processor = JinjaTemplateProcessor(
        project_root=config.root_folder, modules_folder=config.modules_folder
    )
    content = jinja_processor.render(
        jinja_processor.relpath(script_path), config.config_vars
    )

    checksum = hashlib.sha224(content.encode("utf-8")).hexdigest()
    logger.info("Success", checksum=checksum, content=content)
