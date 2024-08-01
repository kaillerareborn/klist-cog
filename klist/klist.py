import asyncio
import discord
from redbot.core import commands
from discord.ext import tasks
import aiohttp
import os
import json
import logging
from typing import Dict, List, Optional, Any
import aiofiles
from aiolimiter import AsyncLimiter

class Klist(commands.Cog):
    """Kaillera Reborn list checker"""

    def __init__(self, bot):
        self.bot = bot
        self.session = aiohttp.ClientSession()
        self.message_ids = self.load_message_ids()
        self.channel_ids = self.load_channel_ids()
        self.rate_limiter = AsyncLimiter(10, 1)  # 10 requests per second
        self.printer_task = None
        
        state = self.load_bot_state()
        self.is_deleted = state["is_deleted"]
        
        if state["is_active"]:
            self.bot.loop.create_task(self.start_printer())
        
        logging.info("Klist initialized")

    def cog_unload(self):
        asyncio.create_task(self.session.close())
        if self.printer_task:
            self.printer_task.cancel()
        asyncio.create_task(self.save_bot_state())
        logging.info("Klist unloaded")

    def load_message_ids(self):
        message_ids = {'games': [], 'servers': []}
        for category in ['games', 'servers']:
            file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), f'ids_{category}.json')
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    message_ids[category] = json.load(f)
        return message_ids

    def load_channel_ids(self):
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'ids_channels.json')
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return json.load(f)
        return {}

    async def save_channel_ids(self):
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'ids_channels.json')
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(self.channel_ids))

    async def save_bot_state(self):
        state = {
            "is_active": self.printer_task is not None and not self.printer_task.cancelled(),
            "is_deleted": self.is_deleted
        }
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'bot_state.json')
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(state))

    def load_bot_state(self):
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'bot_state.json')
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                state = json.load(f)
            return state
        return {"is_active": False, "is_deleted": False}

    async def start_printer(self):
        self.printer_task = self.bot.loop.create_task(self.printer())

    @commands.group(invoke_without_command=True)
    @commands.guild_only()
    @commands.admin_or_permissions(manage_guild=True)
    async def klist(self, ctx):
        """Klist settings and commands"""
        await ctx.send_help(ctx.command)

    @klist.command(name="info")
    async def klist_info(self, ctx):
        """Show current settings and status"""
        guild_id = str(ctx.guild.id)
        guild_data = self.channel_ids.get(guild_id, {})
        games_channel_id = guild_data.get("games")
        servers_channel_id = guild_data.get("servers")
        
        status = "Active" if self.printer_task and not self.printer_task.cancelled() else "Stopped"
        
        embed = discord.Embed(title="Klist Info", color=0x7289DA)
        embed.add_field(name="Games Channel ID", value=str(games_channel_id) if games_channel_id else "None", inline=False)
        embed.add_field(name="Servers Channel ID", value=str(servers_channel_id) if servers_channel_id else "None", inline=False)
        embed.add_field(name="Status", value=status, inline=False)
        
        await ctx.send(embed=embed)

    @klist.command(name="setchannelgames")
    async def set_channel_games(self, ctx, channel_id: str):
        """Set the channel ID for games list"""
        if channel_id.lower() == 'none':
            if str(ctx.guild.id) in self.channel_ids:
                self.channel_ids[str(ctx.guild.id)].pop("games", None)
                if not self.channel_ids[str(ctx.guild.id)]:
                    self.channel_ids.pop(str(ctx.guild.id), None)
            await self.save_channel_ids()
            await ctx.send("Games list channel has been reset.")
            return

        try:
            channel_id = int(channel_id)
        except ValueError:
            await ctx.send("Invalid channel ID. Please provide a valid channel ID or 'none' to reset.")
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            await ctx.send("Invalid channel ID. Please provide a valid channel ID.")
            return
        if str(ctx.guild.id) not in self.channel_ids:
            self.channel_ids[str(ctx.guild.id)] = {}
        self.channel_ids[str(ctx.guild.id)]["games"] = channel_id
        await self.save_channel_ids()
        await ctx.send(f"Games list channel set to {channel.mention}")

    @klist.command(name="setchannelservers")
    async def set_channel_servers(self, ctx, channel_id: str):
        """Set the channel ID for servers list"""
        if channel_id.lower() == 'none':
            if str(ctx.guild.id) in self.channel_ids:
                self.channel_ids[str(ctx.guild.id)].pop("servers", None)
                if not self.channel_ids[str(ctx.guild.id)]:
                    self.channel_ids.pop(str(ctx.guild.id), None)
            await self.save_channel_ids()
            await ctx.send("Servers list channel has been reset.")
            return

        try:
            channel_id = int(channel_id)
        except ValueError:
            await ctx.send("Invalid channel ID. Please provide a valid channel ID or 'none' to reset.")
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            await ctx.send("Invalid channel ID. Please provide a valid channel ID.")
            return
        if str(ctx.guild.id) not in self.channel_ids:
            self.channel_ids[str(ctx.guild.id)] = {}
        self.channel_ids[str(ctx.guild.id)]["servers"] = channel_id
        await self.save_channel_ids()
        await ctx.send(f"Servers list channel set to {channel.mention}")

    @klist.command(name="start")
    async def start_updates(self, ctx):
        """Start updating the lists"""
        if self.printer_task and not self.printer_task.cancelled():
            await ctx.send("Updates are already running.")
            return
        self.is_deleted = False  # Clear the deleted state
        await self.start_printer()
        await self.save_bot_state()
        await ctx.send("Started updating lists.")

    @klist.command(name="stop")
    async def stop_updates(self, ctx):
        """Stop updating the lists"""
        if self.printer_task:
            self.printer_task.cancel()
            self.printer_task = None
            await self.save_bot_state()
            await ctx.send("Stopped updating lists.")
        else:
            await ctx.send("Updates are not running.")

    @klist.command(name="delete")
    async def delete_messages(self, ctx):
        """Delete all messages"""
        if self.printer_task:
            self.printer_task.cancel()
            self.printer_task = None
        await self.delete_all_messages(ctx.guild)
        await self.delete_json_files()
        self.message_ids = {'games': [], 'servers': []}
        self.is_deleted = True  # Set the deleted state
        await self.save_bot_state()
        await ctx.send("All messages removed. Use 'klist start' to begin updates again.")

    async def delete_all_messages(self, guild):
        guild_id = str(guild.id)
        guild_data = self.channel_ids.get(guild_id, {})
        games_channel_id = guild_data.get("games")
        servers_channel_id = guild_data.get("servers")
        
        for channel_id in [games_channel_id, servers_channel_id]:
            if channel_id:
                channel = guild.get_channel(channel_id)
                if channel:
                    async for message in channel.history(limit=None):
                        try:
                            await message.delete()
                        except discord.errors.NotFound:
                            pass  # Message was already deleted

    async def delete_json_files(self):
        for filename in ['ids_games.json', 'ids_servers.json']:
            file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
            if os.path.exists(file_path):
                os.remove(file_path)

    async def fetch_data(self, url: str) -> Optional[str]:
        """Fetch data from a URL with retry logic and error handling."""
        async with self.rate_limiter:
            for attempt in range(5):
                try:
                    async with self.session.get(url) as resp:
                        if resp.status == 503:
                            logging.warning(f"503 Service Unavailable. Retrying... (Attempt {attempt + 1}/5)")
                            await asyncio.sleep(1)
                            continue
                        if resp.status == 200:
                            return await resp.text()
                        logging.error(f"Failed to fetch data: HTTP {resp.status}")
                        return None
                except aiohttp.ClientError as e:
                    logging.error(f"HTTP request failed: {e}. Retrying... (Attempt {attempt + 1}/5)")
                    await asyncio.sleep(1)
            logging.error(f"Failed to fetch data after 5 attempts: {url}")
            return None

    async def update_games_list(self, guild, games_data: Optional[str]) -> None:
        """Update the games list in Discord."""
        guild_id = str(guild.id)
        guild_data = self.channel_ids.get(guild_id, {})
        games_channel_id = guild_data.get("games")
        if not games_channel_id:
            return
        games_channel = guild.get_channel(games_channel_id)
        if not games_channel:
            return

        if not games_data:
            logging.warning("No games data received")
            return

        games = games_data.strip().split('|')
        games_parsed_data = self.parse_games_data(games)
        games_parsed_data.sort(key=lambda x: x['Game'].lower())

        if not games_parsed_data:
            embed = discord.Embed(title="_Waiting Games List (0 games found)_", color=0x7289DA)
            await self.send_or_edit(games_channel, embed, 0, 'games')
            return

        await self.send_game_embeds(games_channel, games_parsed_data)

    def parse_games_data(self, games: List[str]) -> List[Dict[str, str]]:
        """Parse raw games data into structured format."""
        return [
            {
                'Game': games[i],
                'Emulator': games[i + 3],
                'Server': games[i + 5],
                'Location': games[i + 6],
                'IP address': games[i + 1],
                'User': games[i + 2],
                'Waiting': games[i + 4],
            }
            for i in range(0, len(games), 7) if i + 6 < len(games) and not games[i].startswith('*')
        ]

    async def send_game_embeds(self, channel: discord.TextChannel, games_data: List[Dict[str, str]]) -> None:
        """Send or edit game embeds in the channel."""
        for i in range(0, len(games_data), 25):
            embed = discord.Embed(title=f"_Waiting Games List ({len(games_data)} games found)_", color=0x7289DA)
            for item in games_data[i:i + 25]:
                embed.add_field(
                    name=f"**__{item['Game']}__**",
                    value=(
                        f"* Emulator: {item['Emulator']}\n"
                        f"* Server: {item['Server']}\n"
                        f"* Location: {item['Location']}\n"
                        f"* IP address: {item['IP address']}\n"
                        f"* User: {item['User']}\n"
                        f"* Waiting: {item['Waiting']}"
                    ),
                    inline=False
                )
            await self.send_or_edit(channel, embed, i // 25, 'games')

        message_counter = (len(games_data) + 24) // 25
        while message_counter < len(self.message_ids['games']):
            embed = discord.Embed(title=f"_Waiting Games List ({len(games_data)} games found)_", color=0x7289DA)
            await self.send_or_edit(channel, embed, message_counter, 'games')
            message_counter += 1

    async def update_servers_list(self, guild, server_data: Optional[str]) -> None:
        """Update the servers list in Discord."""
        guild_id = str(guild.id)
        guild_data = self.channel_ids.get(guild_id, {})
        servers_channel_id = guild_data.get("servers")
        if not servers_channel_id:
            return
        servers_channel = guild.get_channel(servers_channel_id)
        if not servers_channel:
            return

        if not server_data:
            logging.warning("No server data received")
            return

        server_parsed_data = self.parse_server_data(server_data)
        server_parsed_data.sort(key=lambda x: x['Name'].lower())

        await self.send_server_embeds(servers_channel, server_parsed_data)

    def parse_server_data(self, server_data: str) -> List[Dict[str, str]]:
        """Parse raw server data into structured format."""
        lines = server_data.strip().split('\n')
        server_parsed_data = []
        server_name = None
        for line in lines:
            if ';' in line:
                fields = line.split(';')
                server_parsed_data.append({
                    'Name': server_name,
                    'Location': fields[4],
                    'Users': fields[1],
                    'Games': fields[2],
                    'Version': fields[3],
                    'IP address': fields[0],
                })
                server_name = None
            else:
                server_name = line
        return server_parsed_data

    async def send_server_embeds(self, channel: discord.TextChannel, server_data: List[Dict[str, str]]) -> None:
        """Send or edit server embeds in the channel."""
        for i in range(0, len(server_data), 25):
            embed = discord.Embed(title=f"_Kaillera Servers List ({len(server_data)} servers found)_", color=0x7289DA)
            for server in server_data[i:i + 25]:
                embed.add_field(
                    name=f"**__{server['Name']}__**",
                    value=(
                        f"* Location: {server['Location']}\n"
                        f"* Users: {server['Users']}\n"
                        f"* Games: {server['Games']}\n"
                        f"* Version: {server['Version']}\n"
                        f"* IP address: {server['IP address']}"
                    ),
                    inline=False
                )
            await self.send_or_edit(channel, embed, i // 25, 'servers')

        message_counter = (len(server_data) + 24) // 25
        while message_counter < len(self.message_ids['servers']):
            embed = discord.Embed(title=f"_Kaillera Servers List ({len(server_data)} servers found)_", color=0x7289DA)
            await self.send_or_edit(channel, embed, message_counter, 'servers')
            message_counter += 1

    async def send_or_edit(self, channel: discord.TextChannel, content: Any, index: int, category: str) -> None:
        """Send a new message or edit an existing one."""
        max_retries, retry_delay = 5, 1
        for attempt in range(max_retries):
            try:
                if index < len(self.message_ids[category]):
                    message = await channel.fetch_message(self.message_ids[category][index])
                    if isinstance(content, discord.Embed):
                        await message.edit(embed=content)
                    else:
                        await message.edit(content=content)
                else:
                    message = await channel.send(embed=content) if isinstance(content, discord.Embed) else await channel.send(content=content)
                    self.message_ids[category].append(message.id)
                await self.save_message_ids(f'ids_{category}.json', self.message_ids[category])
                return
            except discord.NotFound:
                message = await channel.send(embed=content) if isinstance(content, discord.Embed) else await channel.send(content=content)
                self.message_ids[category][index] = message.id
                await self.save_message_ids(f'ids_{category}.json', self.message_ids[category])
                return
            except discord.errors.HTTPException as e:
                if e.status == 429:
                    retry_after = float(e.response.headers.get('Retry-After', retry_delay))
                    logging.warning(f"Rate limited. Retrying in {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                else:
                    logging.error(f"HTTP error occurred: {e}. Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
            except Exception as e:
                logging.error(f"Unexpected error: {e}. Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)

        logging.error(f"Failed to send or edit message after {max_retries} attempts.")

    async def save_message_ids(self, filename: str, message_ids: List[int]) -> None:
        """Save message IDs to a JSON file."""
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(message_ids))

    async def printer(self):
        """Main loop to update games and servers lists."""
        while True:
            try:
                if self.is_deleted:
                    await asyncio.sleep(60)
                    continue

                logging.info("Starting update loop")
                for guild in self.bot.guilds:
                    guild_id = str(guild.id)
                    guild_data = self.channel_ids.get(guild_id, {})
                    games_channel_id = guild_data.get("games")
                    servers_channel_id = guild_data.get("servers")
                    
                    if not games_channel_id and not servers_channel_id:
                        continue

                    games_data, server_data = await asyncio.gather(
                        self.fetch_data('http://kaillerareborn.2manygames.fr/game_list.php'),
                        self.fetch_data('http://kaillerareborn.2manygames.fr/server_list.php')
                    )
                    
                    update_tasks = []
                    if games_channel_id:
                        update_tasks.append(self.update_games_list(guild, games_data))
                    if servers_channel_id:
                        update_tasks.append(self.update_servers_list(guild, server_data))
                    
                    await asyncio.gather(*update_tasks)

                await asyncio.sleep(60)  # Wait for 60 seconds before the next update
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error in printer loop: {e}")
                await asyncio.sleep(60)  # Wait before retrying

async def setup(bot):
    await bot.add_cog(Klist(bot))