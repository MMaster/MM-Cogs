

import logging

from mmaiuser.functions.scrape.scrape import scrape_page
from mmaiuser.functions.tool_call import ToolCall
from mmaiuser.functions.types import Function, Parameters, ToolCallSchema

logger = logging.getLogger("red.mmaster.mmaiuser")


class ScrapeToolCall(ToolCall):
    schema = ToolCallSchema(function=Function(
        name="open_url",
        description="Opens a URL or link and returns the content of it, does not support non-text content types",
        parameters=Parameters(
            properties={
                    "url": {
                        "type": "string",
                        "description": "The URL or link to open",
                    }
            },
            required=["query"]
        )))
    function_name = schema.function.name

    async def _handle(self, arguments):
        logger.info(f'Attempting scrape of {arguments["url"]} in {self.ctx.guild}')
        try:
            return await scrape_page(arguments["url"])
        except Exception:
            logger.debug(f"Failed to scrape {arguments['url']}")
            return None
