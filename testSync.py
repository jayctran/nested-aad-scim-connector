from nestedaaddb.nested_groups import SyncNestedGroups
import asyncio

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

sn = SyncNestedGroups(loop)
sn.sync("Parent",True)
loop.close()