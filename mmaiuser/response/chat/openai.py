import json
import logging
from dataclasses import asdict
from typing import Any, Dict, List, Optional

import httpx
import openai
from redbot.core import commands

from mmaiuser.abc import MixinMeta
from mmaiuser.common.constants import FUNCTION_CALLING_SUPPORTED_MODELS, VISION_SUPPORTED_MODELS
from mmaiuser.common.utilities import get_enabled_tools, is_koboldcpp_model
from mmaiuser.functions.tool_call import ToolCall
from mmaiuser.functions.types import ToolCallSchema
from mmaiuser.messages_list.messages import MessagesList
from mmaiuser.response.chat.generator import ChatGenerator

logger = logging.getLogger("red.mmaster.mmaiuser")


class OpenAIAPIGenerator(ChatGenerator):
    def __init__(self, cog: MixinMeta, ctx: commands.Context, messages: MessagesList):
        super().__init__(cog, ctx, messages)
        self.openai_client = cog.openai_client
        self.enabled_tools: List[ToolCall] = []
        self.available_tools_schemas: List[ToolCallSchema] = []
        self.completion: Optional[str] = None

    async def get_custom_parameters(self) -> Dict[str, Any]:
        custom_parameters = await self.config.guild(self.ctx.guild).parameters()
        kwargs = json.loads(custom_parameters) if custom_parameters else {}

        if "logit_bias" not in kwargs:
            weights = await self.config.guild(self.ctx.guild).weights()
            kwargs["logit_bias"] = json.loads(weights or "{}")

        if kwargs.get("logit_bias") and self.model in VISION_SUPPORTED_MODELS:
            logger.warning(f"logit_bias is not supported for model {self.model}, removing...")
            del kwargs["logit_bias"]

        return kwargs

    async def setup_tools(self):
        if not (self.model in FUNCTION_CALLING_SUPPORTED_MODELS and await self.config.guild(self.ctx.guild).function_calling()):
            return
        self.enabled_tools = await get_enabled_tools(self.config, self.ctx)
        self.available_tools_schemas = [tool.schema for tool in self.enabled_tools]

    async def create_completion(self, kwargs: Dict[str, Any]) -> str:
        if "gpt-3.5-turbo-instruct" in self.model:
            prompt = "\n".join(message["content"] for message in self.messages)
            response = await self.openai_client.completions.create(
                model=self.model, prompt=prompt, **kwargs
            )
            return response.choices[0].message.content, None
        elif is_koboldcpp_model(self.model):
##
## <|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\nYou are a helpful AI assistant for travel tips and recommendations<|eot_id|><|start_header_id|>user<|end_header_id|>\n\nWhat can you help me with?<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n
##
            # Local LLM template tags
            tag_start = "<|begin_of_text|>"
            tag_msg_start = ""
            tag_msg_end = "<|eot_id|>"
            tag_role_start = "<|start_header_id|>"
            tag_role_end = "<|end_header_id|>\n\n"

            # stripped
            tag_start_s = tag_start.strip()
            tag_msg_start_s = tag_msg_start.strip()
            tag_msg_end_s = tag_msg_end.strip()
            tag_role_start_s = tag_role_start.strip()
            tag_role_end_s = tag_role_end.strip()

            prompt = tag_start
            for message in self.messages:
                prompt += f'{tag_role_start}{message["role"]}{tag_role_end}{message["content"]}{tag_msg_end}'
            response = await self.openai_client.completions.create(
                model=self.model, prompt=prompt + f'{tag_role_start}assistant{tag_role_end}', stop=[tag_msg_end_s, tag_role_start_s, tag_role_end_s], **kwargs
            )
            output = response.choices[0].text
            output = output.replace(tag_start_s, "")
            output = output.replace(tag_msg_start_s, "")
            output = output.replace(tag_role_start_s + 'user', "")
            output = output.replace(tag_role_start_s + 'assistant', "")
            output = output.replace(tag_role_start_s, "")
            output = output.replace(tag_msg_end_s, "")
            return output, None
        else:
            response = await self.openai_client.chat.completions.create(
                model=self.model, messages=self.msg_list.get_json(), **kwargs
            )
            return response.choices[0].message.content, response.choices[0].message.tool_calls

    async def request_openai(self) -> Optional[str]:
        kwargs = await self.get_custom_parameters()
        await self.setup_tools()

        while not self.completion:
            if self.available_tools_schemas:
                kwargs["tools"] = [asdict(schema) for schema in self.available_tools_schemas]

            self.completion, tool_calls = await self.create_completion(kwargs)

            if tool_calls and not self.completion:
                await self.handle_tool_calls(tool_calls)
            else:
                break

        logger.debug(f'Generated response in {self.ctx.guild.name}: "{self.completion}"')
        return self.completion

    async def handle_tool_calls(self, tool_calls: List[Any]):
        for tool_call in tool_calls:
            function = tool_call.function
            arguments = json.loads(function.arguments)
            result = await self.run_tool(function.name, arguments)
            if result:
                await self.msg_list.add_system(result, index=len(self.msg_list) + 1)

    async def run_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Optional[str]:
        for tool in self.enabled_tools:
            if tool.function_name == tool_name:
                logger.info(f'Handling tool call in {self.ctx.guild.name}: "{tool_name}" with arguments: "{arguments}"')
                arguments["request"] = self
                return await tool.run(arguments, self.available_tools_schemas)

        self.available_tools_schemas = []
        logger.warning(f'Could not find tool "{tool_name}" in {self.ctx.guild.name}')
        return None

    async def generate_message(self) -> Optional[str]:
        try:
            return await self.request_openai()
        except httpx.ReadTimeout:
            logger.error("Failed request to LLM endpoint. Timed out.")
            await self.ctx.react_quietly("💤", message="`mmaiuser` request timed out")
        except openai.RateLimitError:
            await self.ctx.react_quietly("💤", message="`mmaiuser` request ratelimited")
        except Exception:
            logger.exception("Failed API request(s) to LLM endpoint")
            await self.ctx.react_quietly("⚠️", message="`mmaiuser` request failed")
        return None
