from .klist import Klist

async def setup(bot):
    bot.add_cog(Klist(bot))
