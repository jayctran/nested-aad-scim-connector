from azure.identity import DefaultAzureCredential
from msgraph import GraphServiceClient
from msgraph.generated.groups.groups_request_builder import GroupsRequestBuilder
from kiota_abstractions.base_request_configuration import RequestConfiguration
from collections import defaultdict

'''
A wrapper for Graph to interact with Graph API's
https://learn.microsoft.com/en-us/graph/overview
'''

class HashableDict(dict):
    def __hash__(self):
        return hash(frozenset(self.items()))


class Graph:
    client: GraphServiceClient

    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.scopes = ['https://graph.microsoft.com/.default']
        self.client = GraphServiceClient(credentials=self.credential, scopes=self.scopes)

    '''
    Initialises the client
    '''

    async def get_group_by_name(self, group_name):
        query_params = GroupsRequestBuilder.GroupsRequestBuilderGetQueryParameters(
            select=['displayName','id'],
            filter=f"displayName eq '{group_name}'"
        )

        request_config = RequestConfiguration(
            query_parameters=query_params
        )

        return await self.client.groups.get(request_configuration=request_config)
    
    async def check_group_exists(self, group_name):
        group = await self.get_group_by_name(group_name)
        if not group.value:
            return False
        else:
            return True

    '''
    Get all the groups from AAD
    '''

    def get_group_id_by_name(self, group_name):
        return self.get_group_by_name(group_name).value[0].id

    async def get_groups(self):
        query_params = GroupsRequestBuilder.GroupsRequestBuilderGetQueryParameters(
            select=['displayName','id'],
            orderby='displayName'
        )

        request_config = RequestConfiguration(
            query_parameters=query_params
        )
        return await self.client.groups.get(request_configuration=request_config)

    '''
    Get all the group members from the group
    '''

    async def get_group_members(self, gid):
        query_params = GroupsRequestBuilder.GroupsRequestBuilderGetQueryParameters(
            select=['displayName','id','userPrincipalName']
        )

        request_config = RequestConfiguration(
            query_parameters=query_params
        )

        return await self.client.groups.by_group_id(gid).members.get(request_configuration=request_config)

    '''
    Extract the user and group mapping .
    This method makes recursive call to get all the group and member relationships even within nested group
    '''

    async def extract_from_group(self, gid, displayname, groupusermap, usergroupmap):
        gms = await self.get_group_members(gid)
        if gms and gms.value:
            for gm in gms.value:
                if gm.odata_type == "#microsoft.graph.user":
                    for gp in str(displayname).split(":"):
                        groupusermap[gp].add((gm.display_name, gm.user_principal_name))
                        usergroupmap[(gm.display_name, gm.user_principal_name)].add(gp)

                elif gm.odata_type == "#microsoft.graph.group":
                    await self.extract_from_group(gm.id, displayname + ":" + gm.display_name, groupusermap,
                                            usergroupmap)

        return groupusermap, usergroupmap

    async def extract_children_from_group(self, gid, displayname, distinct_groups: set,
                                    distinct_users: set, entra_group_parent_map: defaultdict, depth = 0):

        gms = await self.get_group_members(gid)
        distinct_groups.add(displayname)
        if gms and gms.value:
            for gm in gms.value:
                if gm.odata_type == "#microsoft.graph.user":

                    entra_group_parent_map[displayname].add(
                        HashableDict({'type': 'user', 'display_name': gm.display_name, 'user_principal_name': gm.user_principal_name})
                    )
                    distinct_users.add((gm.display_name, gm.user_principal_name))
                elif gm.odata_type == "#microsoft.graph.group":

                    entra_group_parent_map[displayname].add(HashableDict({'type': 'group', 'display_name': gm.display_name, depth: depth}))
                    distinct_groups.add(gm.display_name)
                    await self.extract_children_from_group(gm.id, gm.display_name, distinct_groups,
                                                    distinct_users, entra_group_parent_map, depth + 1)

        return distinct_groups, distinct_users, entra_group_parent_map