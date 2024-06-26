import json
import requests
import os
import logging

'''
Databricks client to interact with Databricks SCIM API's
https://docs.databricks.com/dev-tools/api/latest/scim/account-scim.html
'''


class DatabricksClient:
    dbbaseUrl: str
    dbscimToken: str

    def __init__(self):
        self.dbbaseUrl = os.environ.get('DB_BASE_URL')
        self.dbscimToken = os.environ.get('DB_SCIM_TOKEN')

        if self.dbbaseUrl is None or self.dbscimToken is None:
            logging.error("Please set DB_BASE_URL and DB_SCIM_TOKEN environment variables")
            exit(1)

    '''
    Get all the users on Databricks
    '''

    def get_dbusers(self):

        # api_url = self.dbbaseUrl + "/Users"
        #
        # my_headers = {'Authorization': 'Bearer ' + self.dbscimToken}
        # response = requests.get(api_url, headers=my_headers).text
        # return json.loads(response)
        all_users = []

        api_url = self.dbbaseUrl + "/Users"
        start_index=1
        count=10000

        while True:
            my_headers = {'Authorization': 'Bearer ' + self.dbscimToken}
            params = {
                'startIndex': start_index,
                'count': count
            }

            response = requests.get(api_url, headers=my_headers, params=params).text
            users_data = json.loads(response)

            # Extract users from the current page and add them to the list
            if 'Resources' in users_data:
                all_users.extend(users_data['Resources'])

            if 'totalResults' in users_data and len(all_users) >= users_data['totalResults']:
                # If we have retrieved all users, break out of the loop
                break

            start_index += count  # Increment the startIndex for the next request

        return all_users

    '''
    Create Databricks User
    '''

    def create_dbuser(self, user, dryrun):
        api_url = self.dbbaseUrl + "/Users"
        u = {
            "schemas": [
                "urn:ietf:params:scim:schemas:core:2.0:User",
                "urn:ietf:params:scim:schemas:core:2.0:User"
            ],
            "userName": user[1],
            "displayName": user[0]
        }

        my_headers = {'Authorization': 'Bearer ' + self.dbscimToken}

        if not dryrun:
            response = requests.post(api_url, data=json.dumps(u), headers=my_headers)
            logging.info("User created " + str(user[1]))
            logging.debug("Response was :" + response.text)
        else:
            logging.info("User to be created " + str(user[0]))

    '''
    Add or remove users in Databricks group
    members : all the users/group that should be in the final state of the group.This is retrieved from Azure AAD
    dbg : databricks group with id and membership
    dbus: all databricks users
    dbgroups : all databricks groups
    '''

    def patch_dbgroup(self, dbg, members, dbus, dbgroups, dryrun):
        api_url = self.dbbaseUrl + "/Groups/" + dbg["id"]
        u = {
            "schemas": [
                "urn:ietf:params:scim:api:messages:2.0:PatchOp"
            ]
        }

        toadd = []
        toremove = []

        if members:
            for member in members:
                logging.info("-----1m-----")
                logging.debug(member)
                exists = False
                if "members" in dbg:
                    for dbmember in dbg["members"]:
                        '''
                        If it is user we are storing both name and email
                        If group we only store name
                        check if user or group exists
                        '''

                        # Retrieve username from db user within dbus matching on dbu["id"] and dbmember["value"]
                        username_add = ""
                        for dbu in dbus:
                            if dbu["id"] == dbmember["value"]:
                                username_add = dbu["userName"]
                                break
                        
                        logging.info(f"dbm is {dbmember}")

                        # Note that dbmember response is coming from databricks group api calls which gives members
                        # This does not have member email-only display
                        # The display/user name of user in AAD and Databricks must match
                        # Even if not matched it will just be added to the to add list
                        if (member["type"] == "user" and member["user_principal_name"].casefold() == username_add.casefold()) \
                                or (member["type"] == "group" and member["display_name"].casefold() == dbmember["display"].casefold()):
                            exists = True
                            logging.info("-----2m")
                            logging.debug(member)
                            break
                if not exists:
                    logging.info("-----3m")
                    logging.debug(member)
                    toadd.append(member)

        if "members" in dbg:
            for dbmember in dbg["members"]:
                exists = False
                
                # Retrieve username from db user within dbus matching on dbu["id"] and dbmember["value"]
                username_remove = ""
                for dbu in dbus:
                    if dbu["id"] == dbmember["value"]:
                        username_remove = dbu["userName"]
                        break

                for member in members:
                    if (member["type"] == "user" and member["user_principal_name"].casefold() == username_remove.casefold()) \
                            or (member["type"] == "group" and member["display_name"].casefold() == dbmember["display"].casefold()):
                        exists = True
                        break
                if not exists:
                    toremove.append(dbmember)

        ops = []

        logging.debug("Inside patchop")

        if len(toadd) == 0 and len(toremove) == 0:
            return

        if len(toadd) > 0:
            dictsub = {'op': "add", 'path': "members", "value": []}
            
            for member in toadd:

                logging.info("----15m-----Going to add user in group-----")
                logging.debug(member)

                # check if it's a user
                if member["type"] == "user":
                    for dbu in dbus:
                        if dbu["userName"].casefold() == member["user_principal_name"].casefold():
                            dictsub["value"].append({"value": dbu["id"]})
                            break
                # or if it is a group
                elif member["type"] == "group":
                    for dbgg in dbgroups:
                        if dbgg.get("displayName", "").casefold() == member["display_name"].casefold():
                            dictsub["value"].append({"value": dbgg["id"]})
                            break

            ops.append(dictsub)

        if len(toremove) > 0:

            dictsub = {'op': "remove", 'path': "members", "value": []}
            for member in toremove:
                dictsub["value"].append({"value": member["value"]})
            ops.append(dictsub)

        gdata = json.loads(json.dumps(u))
        gdata["Operations"] = ops
        ujson = json.dumps(gdata)
        my_headers = {'Authorization': 'Bearer ' + self.dbscimToken}
        if not dryrun:
            response = requests.patch(api_url, data=ujson, headers=my_headers)
            logging.info("Group Existed but membership updated. Request was :" + ujson)
            logging.debug("Response was :" + response.text)

        else:
            logging.info("Group Exists but membership need to be updated for :"
                  + dbg.get("displayName", "NoNameExist") + ". Request details-> data " + ujson + ",EndPoint :" + api_url)

    '''
    Get all Databricks groups
    '''

    def get_dbgroups(self, parent_group_name=None):
        all_groups = []

        if parent_group_name:
            api_url = f"{self.dbbaseUrl}/Groups?filter=displayName eq \"{parent_group_name}\""
        else:
            api_url = f"{self.dbbaseUrl}/Groups"

        start_index=1
        count=10000

        while True:
            my_headers = {'Authorization': 'Bearer ' + self.dbscimToken}
            params = {
                'startIndex': start_index,
                'count': count
            }

            response = requests.get(api_url, headers=my_headers, params=params).text
            groups_data = json.loads(response)

            # Extract users from the current page and add them to the list
            if 'Resources' in groups_data:
                all_groups.extend(groups_data['Resources'])

            if 'totalResults' in groups_data and len(all_groups) >= groups_data['totalResults']:
                # If we have retrieved all users, break out of the loop
                break

            start_index += count  # Increment the startIndex for the next request

        return all_groups

    '''
    Get all nested Databricks groups from Parent
    '''

    def get_distinct_nested_dbgroups(self, parent_group_name, all_group_names: set = set()):
        parent_group = self.get_dbgroups(parent_group_name)[0]
        all_group_names.add((parent_group["id"], parent_group["displayName"]))

        if parent_group.get("members"):
            for dbmember in parent_group["members"]:
                if dbmember["$ref"].startswith("Groups/"):
                    all_group_names.add((dbmember["value"], dbmember["display"]))

                    self.get_distinct_nested_dbgroups(dbmember["display"], all_group_names)

        return all_group_names

    '''
    Delete a Databricks User
    '''

    def delete_user(self, uid):
        api_url = self.dbbaseUrl + "/Users/" + uid

        my_headers = {'Authorization': 'Bearer ' + self.dbscimToken}
        response = requests.delete(api_url, headers=my_headers).text
        return response

    '''
    Delete a Databricks group
    '''

    def delete_group(self, uid, dryrun):
        api_url = self.dbbaseUrl + "/Groups/" + uid

        my_headers = {'Authorization': 'Bearer ' + self.dbscimToken}
        if not dryrun:
            response = requests.delete(api_url, headers=my_headers).text
            return response

    def create_blank_dbgroup(self, group, dryrun):
        api_url = self.dbbaseUrl + "/Groups"
        u = {
            "displayName": group,
            "schemas": [
                "urn:ietf:params:scim:schemas:core:2.0:Group"
            ]
        }

        gdata = json.loads(json.dumps(u))
        ujson = json.dumps(gdata)
        my_headers = {'Authorization': 'Bearer ' + self.dbscimToken}
        if not dryrun:
            response = requests.post(api_url, data=ujson, headers=my_headers)
            logging.debug("Blank Group Created.Request was " + ujson)
            logging.debug("Response was :" + response.text)
        else:
            logging.debug("Blank Group to be created :" + group)