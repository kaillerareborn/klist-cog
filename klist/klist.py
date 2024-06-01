import asyncio
import discord
from discord.ext import commands, tasks
import aiohttp
import os
import json

class Klist(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.session = aiohttp.ClientSession(loop=bot.loop)
        self.games_channel_id = int(os.getenv('GAMES_CHANNEL_ID', '0000'))  # replace with your games channel id
        self.servers_channel_id = int(os.getenv('SERVERS_CHANNEL_ID', '0000'))  # replace with your servers channel id
        self.message_ids = {
            'games': self.load_message_ids('games_message_id.json'),
            'servers': self.load_message_ids('servers_message_id.json'),
        }
        self.printer.start()
        print("Klist initialized")

    def cog_unload(self):
        self.bot.loop.run_until_complete(self.session.close())
        self.printer.cancel()
        print("Klist unloaded")

    @staticmethod
    def load_message_ids(filename):
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return json.load(f)
        else:
            return []

    async def save_message_ids(self, filename, message_ids):
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
        with open(file_path, 'w') as f:
            json.dump(message_ids, f)

    async def fetch_data(self, url):
        for _ in range(5):
            try:
                async with self.session.get(url) as resp:
                    if resp.status == 503:
                        print("503 Service Unavailable. Retrying...")
                        await asyncio.sleep(1)
                        continue
                    if resp.status == 200:
                        return await resp.text()
                    else:
                        print(f"Failed to fetch data: {resp.status}")
                        return None
            except aiohttp.ClientError as e:
                print(f"HTTP request failed: {e}")
                await asyncio.sleep(1)
        return None

    async def update_games_list(self, games_data):
        games_channel = self.bot.get_channel(self.games_channel_id)
        if games_data:
            games = games_data.strip().split('|')
            games_parsed_data = [
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
            games_parsed_data.sort(key=lambda x: x['Game'].lower())

            if not games_parsed_data:
                embed = discord.Embed(title="Waiting Games List (0 games found)", color=0x7289DA)
                await self.send_or_edit(games_channel, embed, 0, 'games')
                return

            for i in range(0, len(games_parsed_data), 25):
                embed = discord.Embed(title=f"Waiting Games List ({len(games_parsed_data)} games found)", color=0x7289DA)
                for item in games_parsed_data[i:i + 25]:
                    embed.add_field(
                        name=f"**{item['Game']}**",
                        value=f"Emulator: {item['Emulator']}\nServer: {item['Server']}\nLocation: {item['Location']}\nIP address: {item['IP address']}\nUser: {item['User']}\nWaiting: {item['Waiting']}",
                        inline=False
                    )

                await self.send_or_edit(games_channel, embed, i // 25, 'games')

            # add placeholders if there are remaining unused messages
            message_counter = (len(games_parsed_data) + 24) // 25
            while message_counter < len(self.message_ids['games']):
                embed = discord.Embed(title=f"Waiting Games List", color=0x7289DA)
                embed.add_field(name='-', value='-', inline=False)
                await self.send_or_edit(games_channel, embed, message_counter, 'games')
                message_counter += 1

            await self.save_message_ids('games_message_id.json', self.message_ids['games'])

    async def update_servers_list(self, server_data):
        servers_channel = self.bot.get_channel(self.servers_channel_id)
        if server_data:
            lines = server_data.strip().split('\n')
            server_name = None
            server_parsed_data = []
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

            server_parsed_data.sort(key=lambda x: x['Name'].lower())
            column_widths = {key: max(len(server[key]) for server in server_parsed_data) for key in ['Name', 'Location', 'Users', 'Games', 'Version', 'IP address']}
            table_header = self.format_table_row(column_widths, 'Name', 'Location', 'Users', 'Games', 'Version', 'IP address', header=True)
            table_separator = self.format_table_row(column_widths, '\u2014' * column_widths['Name'], '\u2014' * column_widths['Location'], '\u2014' * column_widths['Users'], '\u2014' * column_widths['Games'], '\u2014' * column_widths['Version'], '\u2014' * column_widths['IP address'])

            table_string = f"```Kaillera Servers List ({len(server_parsed_data)} servers found)\n\n{table_header}{table_separator}"

            message_counter = 0
            for server in server_parsed_data:
                new_line = self.format_table_row(column_widths, server['Name'], server['Location'], server['Users'], server['Games'], server['Version'], server['IP address'])
                if len(table_string + new_line + table_separator) > 2000 - 6:
                    table_string += "```"
                    await self.send_or_edit(servers_channel, table_string, message_counter, 'servers')
                    table_string = f"```{table_header}{table_separator}{new_line}"
                    message_counter += 1
                else:
                    table_string += new_line

            if table_string and table_string[-3:] != "```":
                table_string += "```"
                await self.send_or_edit(servers_channel, table_string, message_counter, 'servers')
                message_counter += 1

            # leave a dash when there is not enough data to fill existing messages
            while message_counter < len(self.message_ids['servers']):
                placeholder_string = f"```-```"
                await self.send_or_edit(servers_channel, placeholder_string, message_counter, 'servers')
                message_counter += 1

            await self.save_message_ids('servers_message_id.json', self.message_ids['servers'])

    def format_table_row(self, widths, name, location, users, games, version, ip_address, header=False):
        if header:
            return f"| {'Name':<{widths['Name']}} | {'Location':<{widths['Location']}} | {'Users':<{widths['Users']}} | {'Games':<{widths['Games']}} | {'Version':<{widths['Version']}} | {'IP address':<{widths['IP address']}} |\n"
        else:
            return f"| {name:<{widths['Name']}} | {location:<{widths['Location']}} | {users:<{widths['Users']}} | {games:<{widths['Games']}} | {version:<{widths['Version']}} | {ip_address:<{widths['IP address']}} |\n"

    async def send_or_edit(self, channel, content, index, category):
        max_retries, retry_delay = 5, 1
        for attempt in range(max_retries):
            try:
                message = await channel.fetch_message(self.message_ids[category][index])
                if isinstance(content, discord.Embed):
                    await message.edit(embed=content)
                else:
                    await message.edit(content=content)
                return
            except (discord.NotFound, IndexError):
                message = await channel.send(embed=content) if isinstance(content, discord.Embed) else await channel.send(content=content)
                self.adjust_message_ids(self.message_ids[category], message.id, index)
                await self.save_message_ids(f'{category}_message_id.json', self.message_ids[category])
                return
            except discord.errors.HTTPException as e:
                if e.status == 429:
                    retry_after = float(e.response.headers.get('Retry-After', retry_delay))
                    print(f"Rate limited. Retrying in {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                else:
                    print(f"HTTP error occurred: {e}. Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
            except Exception as e:
                print(f"Unexpected error: {e}. Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)

        print(f"Failed to send or edit message after {max_retries} attempts.")

    def adjust_message_ids(self, ids, new_id, index):
        if len(ids) <= index:
            ids.append(new_id)
        else:
            ids[index] = new_id

    @tasks.loop(seconds=60.0)
    async def printer(self):
        print("Starting loop")
        games_data = await self.fetch_data('http://kaillerareborn.2manygames.fr/game_list.php')
        await self.update_games_list(games_data)

        server_data = await self.fetch_data('http://kaillerareborn.2manygames.fr/server_list.php')
        await self.update_servers_list(server_data)

    @printer.before_loop
    async def before_printer(self):
        print("Waiting for bot to be ready...")
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(Klist(bot))
