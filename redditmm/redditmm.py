import os
import asyncio
import logging
import re
from datetime import datetime, timedelta
from html import unescape
from typing import Optional, Union

import aiohttp
import asyncpraw
import asyncprawcore
import discord
import tabulate
import validators
from discord.http import Route
from redbot.core import Config, data_manager, app_commands, commands
from redbot.core.commands.converter import TimedeltaConverter
from redbot.core.utils.chat_formatting import box, humanize_timedelta, pagify, spoiler
from .redditmmdb import RedditMMDB

log = logging.getLogger("red.mmaster.redditmm")

REDDIT_LOGO = "https://www.redditinc.com/assets/images/site/reddit-logo.png"
REDDIT_REGEX = re.compile(
    r"(?i)\A(((https?://)?(www\.)?reddit\.com/)?r/)?([A-Za-z0-9][A-Za-z0-9_]{2,20})/?\Z"
)

class PostMenuView(discord.ui.View):
    def __init__(self, author: str = None, source: str = None):
        super().__init__(timeout=None)

        author_lbl = "❌"
        author_url = "https://www.reddit.com"
        if author is not None:
            author_lbl = f"u/{author}"
            author_url = f"https://www.reddit.com/user/{author}"
        author_disabled = author is None

        source_url = "https://www.reddit.com"
        if source is not None:
            source_url = source
        source_disabled = source is None

        self.add_item(discord.ui.Button(emoji="👤", label=author_lbl, url=author_url, disabled=author_disabled))

        self.add_item(discord.ui.Button(emoji="🌐", url=source_url, disabled=source_disabled))


class RedditMM(commands.Cog):
    """A reddit auto posting cog."""

    __version__ = "0.7.4"

    def format_help_for_context(self, ctx):
        """Thanks Sinbad."""
        pre_processed = super().format_help_for_context(ctx)
        return f"{pre_processed}\nCog Version: {self.__version__}"

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=141445739606515601, force_registration=True)
        self.config.register_channel(reddits={})
        self.config.register_global(delay=300, SCHEMA_VERSION=1)
        self.data_path = data_manager.cog_data_path(self)
        self.db = RedditMMDB(self.data_path)
        self.session = aiohttp.ClientSession()
        self.bg_loop_task: Optional[asyncio.Task] = None
        self.notified = False
        self.client = None
        self.bot.loop.create_task(self.init())

    async def red_get_data_for_user(self, *, user_id: int):
        # this cog does not store any data for user
        return {}

    async def red_delete_data_for_user(self, *, requester, user_id: int) -> None:
        # this cog does not store any data for user
        pass

    async def init(self):
        await self.db.init()
        await self.bot.wait_until_red_ready()
        if await self.config.SCHEMA_VERSION() == 1:
            data = await self.config.all_channels()
            for channel, _ in data.items():
                async with self.config.channel_from_id(channel).reddits() as sub_data:
                    for feed in sub_data:
                        try:
                            sub_data[feed]["subreddit"] = sub_data[feed]["url"].split("/")[4]
                        except IndexError:
                            sub_data[feed]["subreddit"] = None
            await self.bot.send_to_owners(
                "Hi there.\nRedditMM accomodates the new reddit ratelimits. This cog requires authenthication.\nTo setup the cog create an application via https://www.reddit.com/prefs/apps/. Once this is done, copy the client ID found under the name and the secret found inside.\nYou can then setup this cog by using `[p]set api redditmm clientid CLIENT_ID_HERE clientsecret CLIENT_SECRET_HERE`\n"
            )
            await self.config.SCHEMA_VERSION.set(2)

        token = await self.bot.get_shared_api_tokens("redditmm")
        try:
            self.client = asyncpraw.Reddit(
                client_id=token.get("clientid", None),
                client_secret=token.get("clientsecret", None),
                user_agent=f"{self.bot.user.name} Discord Bot",
            )

            self.bg_loop_task = self.bot.loop.create_task(self.bg_loop())
        except Exception as exc:
            log.error("Exception in init: ", exc_info=exc)
            await self.bot.send_to_owners(
                "An exception occured in the authenthication. Please ensure the client id and secret are set correctly.\nTo setup the cog create an application via https://www.reddit.com/prefs/apps/. Once this is done, copy the client ID found under the name and the secret found inside.\nYou can then setup this cog by using `[p]set api redditmm clientid CLIENT_ID_HERE clientsecret CLIENT_SECRET_HERE`"
            )

    async def cog_unload(self):
        if self.bg_loop_task:
            self.bg_loop_task.cancel()
        await self.session.close()
        await self.client.close()
        self.db.close()

    @commands.Cog.listener()
    async def on_red_api_tokens_update(self, service_name, api_tokens):
        if service_name == "redditmm":
            try:
                self.client = asyncpraw.Reddit(
                    client_id=api_tokens.get("clientid", None),
                    client_secret=api_tokens.get("clientsecret", None),
                    user_agent=f"{self.bot.user.name} Discord Bot",
                )
            except Exception as exc:
                log.error("Exception in init: ", exc_info=exc)
                await self.bot.send_to_owners(
                    "An exception occured in the authenthication. Please ensure the client id and secret are set correctly.\nTo setup the cog create an application via https://www.reddit.com/prefs/apps/. Once this is done, copy the client ID found under the name and the secret found inside.\nYou can then setup this cog by using `[p]set api redditmm clientid CLIENT_ID_HERE clientsecret CLIENT_SECRET_HERE`"
                )

    async def bg_loop(self):
        await self.bot.wait_until_ready()
        while True:
            try:
                await self.do_feeds()
                delay = await self.config.delay()
                await asyncio.sleep(delay)
            except Exception as exc:
                log.error("Exception in bg_loop: ", exc_info=exc)
                if not self.notified:
                    msg = "An exception occured in the background loop for `redditmm`. Check your logs for more details and if possible, report them to the cog creator.\nYou will no longer receive this message until you reload the cog to reduce spam."
                    await self.bot.send_to_owners(msg)
                    self.notified = True

    async def do_feeds(self):
        if self.client is None:
            return
        feeds = {}
        channel_data = await self.config.all_channels()
        for channel_id, data in channel_data.items():
            channel = self.bot.get_channel(channel_id)
            if not channel:
                continue
            for sub, feed in data["reddits"].items():
                url = feed.get("subreddit", None)
                if not url:
                    continue
                if url in feeds:
                    response = feeds[url]
                else:
                    response = await self.fetch_feed(url)
                    feeds[url] = response
                if response is None:
                    continue
                time = await self.format_send(
                    response,
                    channel,
                    feed["last_post"],
                    url,
                    {
                        "latest": feed.get("latest", False),
                        "webhooks": feed.get("webhooks", False),
                        "logo": feed.get("logo", REDDIT_LOGO),
                        "image_only": feed.get("image_only", False),
                        "publish": feed.get("publish", False),
                    },
                )
                if time is not None:
                    async with self.config.channel(channel).reddits() as feeds_data:
                        feeds_data[sub]["last_post"] = time

    @staticmethod
    def _clean_subreddit(subreddit: str):
        subreddit = subreddit.lstrip("/")
        if match := REDDIT_REGEX.fullmatch(subreddit):
            return match.groups()[-1].lower()
        return None

    async def add_temporary_reaction(self, message, emoji, delay=5):
        await message.add_reaction(emoji)
        await asyncio.sleep(delay)
        await message.clear_reaction(emoji)

    @commands.Cog.listener()
    async def on_raw_reaction_add(
        self, payload: discord.RawReactionActionEvent
    ) -> None:
        reaction_emoji = str(payload.emoji)

        if payload.guild_id is None:
            return  # Reaction is on a private message
        guild = self.bot.get_guild(payload.guild_id)

        channel = self.bot.get_channel(payload.channel_id)
        message = await channel.fetch_message(payload.message_id)
        user = guild.get_member(payload.user_id)
        if not user:
            user = await guild.fetch_member(payload.user_id)

        if await self.bot.cog_disabled_in_guild(
            cog=self, guild=guild
        ) or not await self.bot.allowed_by_whitelist_blacklist(who=user):
            return
        if user.id == guild.me.id:
            return

        # if the message did not originate from this cog
        ctx: commands.Context = await self.bot.get_context(message)
        if ctx.valid and (ctx.command.cog_name != "RedditMM"):
            return

        # ignore user
        if reaction_emoji == "❌":
            # only allow admins and mods to do this
            if not (await self.bot.is_admin(user) or await self.bot.is_mod(user)):
                return

            async with ctx.typing():
                redditor = self.get_msg_redditor(message)
                if redditor is None:
                    await self.add_temporary_reaction(message, "⛔")
                    return

                if await self.db.get_ignored_redditor(guild.id, redditor) is not None:
                    await self.add_temporary_reaction(message, "♻")
                    return

                if await self.db.add_ignored_redditor(guild.id, redditor) is None:
                    await self.add_temporary_reaction(message, "⚠")
                    return

                await self.add_temporary_reaction(message, "✅")
            return

        # favorite post
        if reaction_emoji == "⭐":
            async with ctx.typing():
                redditor = self.get_msg_redditor(message)
                if redditor is None:
                    await self.add_temporary_reaction(message, "⛔")
                    return

                content_url = self.get_msg_content_url(message)
                if content_url is None:
                    content_url = ''
                if await self.db.get_favorite(guild.id, redditor, content_url, user.id) is not None:
                    await self.add_temporary_reaction(message, "♻")
                    return

                postlink = self.get_msg_source(message)
                if await self.db.add_favorite(guild.id, redditor, content_url, user.id, postlink) is None:
                    await self.add_temporary_reaction(message, "⚠")
                    return

                await self.add_temporary_reaction(message, "✅")
            return


    @commands.Cog.listener()
    async def on_raw_reaction_remove(
        self, payload: discord.RawReactionActionEvent
    ) -> None:
        reaction_emoji = str(payload.emoji)

        if payload.guild_id is None:
            return  # Reaction is on a private message
        guild = self.bot.get_guild(payload.guild_id)

        channel = self.bot.get_channel(payload.channel_id)
        message = await channel.fetch_message(payload.message_id)
        user = guild.get_member(payload.user_id)
        if not user:
            user = await guild.fetch_member(payload.user_id)

        if await self.bot.cog_disabled_in_guild(
            cog=self, guild=guild
        ) or not await self.bot.allowed_by_whitelist_blacklist(who=user):
            return
        if user.id == guild.me.id:
            return

        # if the message did not originate from this cog
        ctx: commands.Context = await self.bot.get_context(message)
        if ctx.valid and (ctx.command.cog_name != "RedditMM"):
            return

        # remove ignore user
        if reaction_emoji == "❌":
            # only allow admins and mods to do this
            if not (await self.bot.is_admin(user) or await self.bot.is_mod(user)):
                return

            async with ctx.typing():
                redditor = self.get_msg_redditor(message)
                if redditor is None:
                    await self.add_temporary_reaction(message, "⛔")
                    return

                if await self.db.get_ignored_redditor(guild.id, redditor) is None:
                    await self.add_temporary_reaction(message, "♻")
                    return

                cnt = await self.db.del_ignored_redditor(guild.id, redditor)
                if cnt is None or cnt < 1:
                    await self.add_temporary_reaction(message, "⚠")
                    return

                await self.add_temporary_reaction(message, "✅")
            return

        # remove favorite post
        if reaction_emoji == "⭐":
            async with ctx.typing():
                redditor = self.get_msg_redditor(message)
                if redditor is None:
                    await self.add_temporary_reaction(message, "⛔")
                    return

                content_url = self.get_msg_content_url(message)
                if content_url is None:
                    content_url = ''
                if await self.db.get_favorite(guild.id, redditor, content_url, user.id) is None:
                    await self.add_temporary_reaction(message, "♻")
                    return

                cnt = await self.db.del_favorite(guild.id, redditor, content_url, user.id)
                if cnt is None or cnt < 1:
                    await self.add_temporary_reaction(message, "⚠")
                    return

                await self.add_temporary_reaction(message, "✅")
            return

    @commands.admin_or_permissions(manage_channels=True)
    @commands.guild_only()
    @commands.hybrid_group(aliases=["reddit"])
    async def redditmm(self, ctx):
        """Reddit auto-feed posting.
        Use ❌ reaction on message to ignore the reddit user.
        Use ⭐ reaction on message to favorite the post.
        """

    @redditmm.command()
    @commands.is_owner()
    async def setup(self, ctx):
        """Details on setting up RedditMM"""
        msg = "To setup the cog create an application via https://www.reddit.com/prefs/apps/. Once this is done, copy the client ID found under the name and the secret found inside.\nYou can then setup this cog by using `[p]set api redditmm clientid CLIENT_ID_HERE clientsecret CLIENT_SECRET_HERE`"
        await ctx.send(msg)

    @redditmm.command()
    @commands.is_owner()
    async def delay(
        self,
        ctx,
        time: TimedeltaConverter(
            minimum=timedelta(seconds=15), maximum=timedelta(hours=3), default_unit="seconds"
        ),
    ):
        """Set the delay used to check for new content."""
        seconds = time.total_seconds()
        await self.config.delay.set(seconds)
        if not ctx.interaction:
            await ctx.tick()
        await ctx.send(
            f"The {humanize_timedelta(seconds=seconds)} delay will come into effect on the next loop."
        )

    @redditmm.command()
    @app_commands.describe(
        image_only="Whether to show only posts with images.", 
        subreddit="The subreddit to add.", 
        channel="The channel to post in."
    )
    @commands.bot_has_permissions(send_messages=True, embed_links=True)
    async def add(self, ctx, image_only: bool, subreddit: str, channel: Optional[discord.TextChannel] = None):
        """Add a subreddit to post new content from."""
        channel = channel or ctx.channel
        subreddit = self._clean_subreddit(subreddit)
        if not subreddit:
            return await ctx.send("That doesn't look like a subreddit name to me.")
        if self.client is None:
            await ctx.send(
                f"Please setup the client correctly, `{ctx.clean_prefix}redditmm setup` for more information"
            )
            return
        async with ctx.typing():
            try:
                subreddit_info = await self.client.subreddit(subreddit, fetch=True)
            except asyncprawcore.Forbidden:
                return await ctx.send("I can't view private subreddits.")
            except asyncprawcore.NotFound:
                return await ctx.send("This subreddit doesn't exist.")
            except Exception:
                return await ctx.send("Something went wrong while searching for this subreddit.")

        if subreddit_info.over18 and not channel.is_nsfw():
            return await ctx.send(
                "You're trying to add an NSFW subreddit to a SFW channel. Please edit the channel or try another."
            )
        logo = subreddit_info.icon_img or REDDIT_LOGO

        async with self.config.channel(channel).reddits() as feeds:
            if subreddit in feeds:
                return await ctx.send("That subreddit is already set to post.")

            response = await self.fetch_feed(subreddit)

            if response is None:
                return await ctx.send("That didn't seem to be a valid reddit feed.")

            feeds[subreddit] = {
                "subreddit": subreddit,
                "last_post": datetime.now().timestamp(),
                "latest": False,
                "logo": logo,
                "webhooks": False,
                "image_only": image_only,
            }
        if ctx.interaction:
            await ctx.send("Subreddit added.", ephemeral=True)
        else:
            await ctx.tick()

    @redditmm.command()
    @app_commands.describe(channel="The channel to list subreddits for.")
    @commands.bot_has_permissions(send_messages=True, embed_links=True)
    async def list(self, ctx, channel: discord.TextChannel = None):
        """Lists the current subreddits for the current channel, or a provided one."""

        channel = channel or ctx.channel

        data = await self.config.channel(channel).reddits()
        if not data:
            return await ctx.send("No subreddits here.")
        output = [[k, v.get("webhooks", "False"), v.get("latest", False), v.get("image_only", False)] for k, v in data.items()]

        out = tabulate.tabulate(output, headers=["Subreddit", "Webhooks", "Latest Posts", "Image Only"])
        for page in pagify(str(out)):
            await ctx.send(
                embed=discord.Embed(
                    title=f"Subreddits for {channel}.",
                    description=box(page, lang="prolog"),
                    color=(await ctx.embed_color()),
                )
            )

    @redditmm.command(name="remove")
    @app_commands.describe(
        subreddit="The subreddit to remove.", channel="The channel to remove from."
    )
    @commands.bot_has_permissions(send_messages=True, embed_links=True)
    async def remove_feed(
        self, ctx, subreddit: str, channel: Optional[discord.TextChannel] = None
    ):
        """Removes a subreddit from the current channel, or a provided one."""
        channel = channel or ctx.channel
        subreddit = self._clean_subreddit(subreddit)
        if not subreddit:
            return await ctx.send("That doesn't look like a subreddit name to me.")
        async with self.config.channel(channel).reddits() as feeds:
            if subreddit not in feeds:
                await ctx.send(f"No subreddit named {subreddit} in {channel.mention}.")
                return

            del feeds[subreddit]
        if ctx.interaction:
            await ctx.send("Subreddit removed.", ephemeral=True)
        else:
            await ctx.tick()

    @redditmm.command(name="force")
    @app_commands.describe(subreddit="The subreddit to force.", channel="The channel to force in.")
    @commands.bot_has_permissions(send_messages=True, embed_links=True)
    async def force(self, ctx, subreddit: str, channel: Optional[discord.TextChannel] = None):
        """Force the latest post."""
        channel = channel or ctx.channel
        subreddit = self._clean_subreddit(subreddit)
        if not subreddit:
            return await ctx.send("That doesn't look like a subreddit name to me.")
        feeds = await self.config.channel(channel).reddits()
        if subreddit not in feeds:
            await ctx.send(f"No subreddit named {subreddit} in {channel.mention}.")
            return
        if self.client is None:
            await ctx.send(
                f"Please setup the client correctly, `{ctx.clean_prefix}redditmm setup` for more information"
            )
            return
        data = await self.fetch_feed(feeds[subreddit]["subreddit"])
        if data is None:
            return await ctx.send("No post could be found.")
        if ctx.interaction:
            await ctx.send("Post sent.", ephemeral=True)
        await self.format_send(
            data,
            channel,
            0,
            subreddit,
            {
                "latest": False,
                "webhooks": feeds[subreddit].get("webhooks", False),
                "logo": feeds[subreddit].get("logo", REDDIT_LOGO),
                "image_only": False,
                "publish": False,
            },
        )
        if ctx.interaction:
            await ctx.send("Post sent.", ephemeral=True)
        else:
            await ctx.tick()

    @redditmm.command(name="latest")
    @app_commands.describe(
        subreddit="The subreddit to check for single latest post (or !all for all subreddits).",
        channel="The channel for the subreddit.",
        on_or_off="Whether to enable or disable single latest post only.",
    )
    @commands.bot_has_permissions(send_messages=True, embed_links=True)
    async def latest(self, ctx, subreddit: str, on_or_off: bool, channel: discord.TextChannel = None):
        """Whether to fetch all posts or just the latest post."""
        channel = channel or ctx.channel
        if subreddit != "!all":
            subreddit = self._clean_subreddit(subreddit)
        if not subreddit:
            return await ctx.send("That doesn't look like a subreddit name to me.")
        async with self.config.channel(channel).reddits() as feeds:
            if subreddit == "!all":
                for feed in feeds:
                    feed["latest"] = on_or_off
            else:
                if subreddit not in feeds:
                    await ctx.send(f"No subreddit named {subreddit} in {channel.mention}.")
                    return

                feeds[subreddit]["latest"] = on_or_off 
        if ctx.interaction:
            await ctx.send("Subreddit updated.", ephemeral=True)
        else:
            await ctx.tick()

    @redditmm.command()
    @app_commands.describe(
        subreddit="The subreddit name (or !all for all subreddits).",
        channel="The channel for the subreddit.",
        on_or_off="Whether to enable or disable images only.",
    )
    @commands.bot_has_permissions(send_messages=True, embed_links=True)
    async def imageonly(
        self, ctx, subreddit: str, on_or_off: bool, channel: discord.TextChannel = None
    ):
        """Whether to only post posts that contain an image."""
        channel = channel or ctx.channel
        if subreddit != "!all":
            subreddit = self._clean_subreddit(subreddit)
        if not subreddit:
            return await ctx.send("That doesn't look like a subreddit name to me.")
        async with self.config.channel(channel).reddits() as feeds:
            if subreddit == "!all":
                for feed in feeds:
                    feed["image_only"] = on_or_off
            else:
                if subreddit not in feeds:
                    await ctx.send(f"No subreddit named {subreddit} in {channel.mention}.")
                    return

                feeds[subreddit]["image_only"] = on_or_off
        if ctx.interaction:
            await ctx.send("Subreddit updated.", ephemeral=True)
        else:
            await ctx.tick()

    @redditmm.command()
    @app_commands.describe(
        subreddit="The subreddit name",
        channel="The channel for the subreddit.",
        on_or_off="Whether to enable or disable publishing of messages.",
    )
    @commands.bot_has_permissions(send_messages=True, embed_links=True)
    async def publish(
        self, ctx, subreddit: str, on_or_off: bool, channel: discord.TextChannel = None
    ):
        """Whether to publish posts - must be a news channel."""
        channel = channel or ctx.channel
        subreddit = self._clean_subreddit(subreddit)
        if not subreddit:
            return await ctx.send("That doesn't look like a subreddit name to me.")
        async with self.config.channel(channel).reddits() as feeds:
            if subreddit not in feeds:
                await ctx.send(f"No subreddit named {subreddit} in {channel.mention}.")
                return

            if not channel.permissions_for(ctx.me).manage_messages:
                return await ctx.send("I need manage messages permissions to publish messages.")
            if not channel.is_news():
                return await ctx.send("I can only publish messages in news channels.")

            feeds[subreddit]["publish"] = on_or_off
        if ctx.interaction:
            await ctx.send("Subreddit updated.", ephemeral=True)
        else:
            await ctx.tick()

    @redditmm.command(
        name="webhook", aliases=["webhooks"], usage="<subreddit> <true_or_false> [channel]"
    )
    @app_commands.describe(
        subreddit="The subreddit name",
        channel="The channel for the subreddit.",
        webhook="Whether to enable or disable webhooks.",
    )
    @commands.bot_has_permissions(send_messages=True, embed_links=True, manage_webhooks=True)
    async def webhook(
        self, ctx, subreddit: str, webhook: bool, channel: discord.TextChannel = None
    ):
        """Whether to send the post as a webhook or message from the bot."""
        channel = channel or ctx.channel
        subreddit = self._clean_subreddit(subreddit)
        if not subreddit:
            return await ctx.send("That doesn't look like a subreddit name to me.")
        async with self.config.channel(channel).reddits() as feeds:
            if subreddit not in feeds:
                await ctx.send(f"No subreddit named {subreddit} in {channel.mention}.")
                return

            feeds[subreddit]["webhooks"] = webhook

        if webhook:
            await ctx.send(f"New posts from r/{subreddit} will be sent as webhooks.")
        else:
            await ctx.send(f"New posts from r/{subreddit} will be sent as bot messages.")

        if ctx.interaction:
            await ctx.send("Subreddit updated.", ephemeral=True)
        else:
            await ctx.tick()

    async def fetch_feed(self, subreddit: str):
        try:
            subreddit = await self.client.subreddit(subreddit)
            resp = [submission async for submission in subreddit.new(limit=20)]
            return resp or None
        except Exception:
            return None

    def get_msg_redditor(self, message: discord.Message):
        if message is None:
            log.info("Cannot call get_msg_redditor on None message.")
            return None

        if message.components is None or len(message.components) == 0:
            log.info(f"No message components.")
            return None

        comps = message.components
        if comps[0].type == discord.ComponentType.action_row:
            comps = comps[0].children

        for comp in comps:
            if str(comp.emoji) == "👤":
                author = comp.label
                if not author.startswith("u/"):
                    return None
                return author[2:]

        log.info("Author component not found")
        return None

    def get_msg_source(self, message: discord.Message):
        if message is None:
            log.info("Cannot call get_msg_source on None message.")
            return None

        if message.components is None or len(message.components) == 0:
            log.info(f"No message components.")
            return None

        comps = message.components
        if comps[0].type == discord.ComponentType.action_row:
            comps = comps[0].children

        for comp in comps:
            if str(comp.emoji) == "🌐":
                return comp.url

        log.info("Source component not found")
        return None

    def get_msg_content_url(self, message: discord.Message):
        if message is None or message.content is None:
            return None

        content = message.content

        # first try current format
        # > _ < {post['content_link']} > _ \n
        carr = content.rsplit('> _ < http', maxsplit=1)
        if len(carr) == 2:
            carr = carr[1].split(" > _ ", maxsplit=1)
            if len(carr) == 2:
                return 'http' + carr[0]

        # try old format
        # > _ {post['content_link']} _\n
        carr = content.rsplit('> _ http', maxsplit=1)
        if len(carr) == 2:
            carr = carr[1].split(" _", maxsplit=1)
            if len(carr) == 2:
                return 'http' + carr[0]

        return None

    async def prepare_post(self, feed, subreddit, guildID, settings):
        post = {}
        post["subreddit"] = unescape(subreddit)
        title = unescape(feed.title)
        if len(title) > 252:
            title = f"{title[:252]}..."
        post["title"] = title

        desc = unescape(feed.selftext)
        if len(desc) > 2000:
            desc = f"{desc[:2000]}..."
        if feed.spoiler:
            desc = "(spoiler)\n" + spoiler(desc)
        post["desc"] = desc
        post["source"] = unescape(f"https://reddit.com{feed.permalink}")
        post["time_text"] = f"<t:{int(feed.created_utc)}>"

        if feed.author:
            post["author"] = unescape(feed.author.name)
            post["author_favs"] = await self.db.get_favorite(guildID, post['author'])
        else:
            post["author"] = None
            post["author_favs"] = None


        post["content_link"] = None
        url = unescape(feed.url)
        if feed.permalink not in url and validators.url(url):
            if "i.redgifs.com" in url and url.endswith(("png", "jpg", "jpeg", "gif")):
                content_link = url.replace("i.redgifs.com", "www.redgifs.com").replace("/i/", "/watch/")
                post["content_link"] = content_link.rsplit('.', maxsplit=1)[0]
            else:
                post["content_link"] = url

        post["embeds"] = None

        return post

    async def send_post(self, post, channel, settings, webhook):
        if webhook is None:
            try:
                text = f"> _[[r/{post['subreddit']}](https://www.reddit.com/r/{post['subreddit']}/)]_ \n"
                text+= f"> ### {post['title']}\n"
                if post['desc'] is not None and len(post['desc']) > 0:
                    text+= f"> _{post['desc']}_\n"
                # WARN: content link MUST be "> _ < {url} > _ \n" for get_msg_content_url() to work
                #       it also MUST be the last thing in content surrounded like this
                text+= f"> _ < {post['content_link']} > _ \n"

                # CAREFUL WITH UNDERSCORES AFTER THIS
                fav_text = ""
                if post['author_favs'] is not None:
                    fav_text = f"⭐ {post['author_favs']}      "
                    
                text+= f"> {fav_text}_{post['time_text']}_"

                msg = await channel.send(
                    content=text,
                    embeds=post["embeds"],
                    view=PostMenuView(post['author'], post['source']),
                )  # TODO: More approprriate error handling

                if settings.get("publish", False):
                    try:
                        await msg.publish()
                    except discord.Forbidden:
                        log.info(
                            f"Error publishing message feed in {channel}. Bypassing"
                        )
            except (discord.Forbidden, discord.HTTPException) as e:
                log.error(f"Error sending message feed in {channel}. Bypassing", exc_info=e)
        else:
            # FIXME: Send proper content (embeds are not used for content anymore)
            await webhook.send(
                username=f"r/{post['subreddit']} (u/{post['author']})",
                avatar_url=settings.get("icon", REDDIT_LOGO),
                embeds=post["embeds"],
            )

    async def format_send(self, data, channel, last_post, subreddit, settings):
        posts = []
        data = data[:1] if settings.get("latest", False) else data
        webhook = None
        try:
            if (
                settings.get("webhooks", False)
                and channel.permissions_for(channel.guild.me).manage_webhooks
            ):
                for hook in await channel.webhooks():
                    if hook.name == channel.guild.me.name:
                        webhook = hook
                if webhook is None:
                    webhook = await channel.create_webhook(name=channel.guild.me.name)
        except Exception as e:
            log.error("Error in webhooks during reddit feed posting", exc_info=e)

        latest_timestamp = last_post
        for feed in data:
            timestamp = feed.created_utc
            if timestamp > last_post:
                latest_timestamp = timestamp
            if feed.over_18 and not channel.is_nsfw():
                continue
            if timestamp <= last_post:
                break

            # check if redditor is ignored and skip if yes
            if feed.author:
                author = unescape(feed.author.name)
                if await self.db.get_ignored_redditor(channel.guild.id, author) is not None:
                    #log.info(f'Redditor {author} ignored. Skip.')
                    continue

            # if already seen (posted in any channel) in this server, skip
            if feed.url is not None and await self.db.get_seen_url(channel.guild.id, unescape(feed.url)) is not None:
                #log.info(f'URL {unescape(feed.url)} already seen. Skip.')
                continue

            post = await self.prepare_post(feed, subreddit, channel.guild.id, settings)
            if settings.get("image_only") and post["content_link"] is None:
                continue

            # TODO: gallery view, fetch images to multiple embeds
            posts.append(post)

            # remember we've seen this url for this server
            await self.db.add_seen_url(channel.guild.id, feed.url)

        if latest_timestamp > last_post:
            if len(posts) > 0:
                try:
                    for post in posts[::-1]:
                        await self.send_post(post, channel, settings, webhook)
                except discord.HTTPException as exc:
                    log.error("Exception in bg_loop while sending message: ", exc_info=exc)
            return latest_timestamp
        return None

