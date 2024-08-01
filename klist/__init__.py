from .klist import Klist

async def setup(bot):
    await bot.add_cog(Klist(bot))