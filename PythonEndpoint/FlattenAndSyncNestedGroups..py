import sys

import configparser

from model.DatabricksClient import DatabricksClient
from model.Graph import Graph
from collections import defaultdict

'''
Dictionaries used for extracting and reusing user and group mappings(Including nestedAAD groups) in AAD
'''
groupUsermap = defaultdict(set)
userGroupmap = defaultdict(set)
groupUsermapU = defaultdict(set)
userGroupmapU = defaultdict(set)

'''
Entry point of the application
Can provide --dryrun to do a dryrun
Can provide a top level group as program argument as top level group
Ex:
python PythonEndpoint/FlattenAndSyncNestedGroups.py parent
python PythonEndpoint/FlattenAndSyncNestedGroups.py parent --dryrun
'''


def main(group):
    global groupUsermapU
    dryrun = False;

    print('Number of arguments:', len(sys.argv[1:]), 'arguments.')
    print('Argument List:', str(sys.argv[1:]))

    '''
    Validation for program arguments
    '''
    if len(sys.argv[1:]) > 2:
        print("only 2 arguments supported")
        return

    toplevelgroup = ""

    for arg in sys.argv[1:]:
        if arg.casefold() == "--dryrun":
            dryrun = True;
        else:
            if toplevelgroup == "":
                toplevelgroup = arg;
            else:
                print("Only one group supported")
                return

    '''
    set top level group
    Priority is for program argument and if not provided then checks method argument
    '''
    if toplevelgroup == "":
        toplevelgroup = group

    '''
    Initialise clients
    '''
    config = configparser.ConfigParser()
    config.read(['../config/config.cfg', 'config.dev.cfg'])
    azure_settings = config['azure']
    db_settings = config['databricks']

    graph: Graph = Graph(azure_settings)
    dbclient: DatabricksClient = DatabricksClient(db_settings)

    '''
    Read All Databricks users
    '''
    dbusers = dbclient.get_dbusers()

    print("1.All Databricks Users Read")

    groups_page = graph.get_groups()

    '''
    Read all groups from AAD
    '''
    print("2.All AAD groups Read done")
    print("3.Top level group requested is " + toplevelgroup)

    '''
    Indicates whether user and group collection are loaded successfully
    '''
    colInitialised = False;

    '''
    Iterate through each group in AAD and map members corresponding to it including nested child group members
    '''
    for group in groups_page['value']:
        print("Group is " + group["displayName"])
        for arg in sys.argv[1:]:
            if not arg.startswith("--") and toplevelgroup.casefold() == group["displayName"].casefold():
                groupUsermapU, userGroupmapU = graph.extract_from_group(graph, group["id"], group["displayName"],
                                                                        groupUsermap, userGroupmap);
                colInitialised = True

    print("4.Hierarchy analysed,going to create users and groups")

    if dryrun:
        print("THIS IS DRY RUN.NO CHANGES WILL TAKE PLACE ON DATABRICKS")

    if colInitialised:

        '''
        Create Users in Databricks as required
        '''
        for u in userGroupmapU.keys():

            exists = False

            for udb in dbusers["Resources"]:
                if u[0].casefold() == udb["displayName"].casefold():
                    exists = True;

            if not exists:
                dbclient.create_dbuser(u, dryrun)

        '''
        Reloading users from Databricks as we need id of new users as well added in last step
        '''
        dbusers = dbclient.get_dbusers()
        dbGroups = dbclient.get_dbgroups()

        '''
        Create groups or update membership of groups i.e. add/remove users from groups
        '''
        for u in groupUsermapU.keys():
            exists = False
            for dbg in dbGroups["Resources"]:
                if u.casefold() == dbg["displayName"].casefold():
                    exists = True
                    # compare and add remove the members as needed
                    dbclient.patch_dbgroup(dbg["id"], groupUsermap.get(u), dbg, dbusers, dryrun)

            if not exists:
                dbclient.create_dbgroup(u, groupUsermap.get(u), dbusers, dryrun)


if __name__ == '__main__':
    main("")
