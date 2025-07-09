from flask import Flask
from flask import request
from flask import jsonify
import requests
import os
import time
import threading
import copy
from collections import deque
from hashring import ConsistentHashing
server = Flask(__name__)
gossipQueue = deque()
store = {}
versions = {}
view = {}
known_versions = set()
viewQueue = deque()
writeQueue = deque()
current_shard = None
shard_ring = ConsistentHashing(100)

node_id = int(os.getenv("NODE_IDENTIFIER", "1"))
Gossip_interval = 2


def create_new_version(key, client_meta_data):
    if key not in versions:
        recentKeyVersion = None
        for log in client_meta_data:
            logList = log.split(".")
            if key == logList[2]:
                if recentKeyVersion == None:
                    recentKeyVersion = int(logList[0])
                else:
                    recentKeyVersion = max(recentKeyVersion, int(logList[0]))
        if recentKeyVersion is None:
            versions[key] = 1
        else:
            versions[key] = recentKeyVersion + 1
    else:
        recentKeyVersion = None
        for log in client_meta_data:
            logList = log.split(".")
            if key == logList[2]:
                if recentKeyVersion == None:
                    recentKeyVersion = int(logList[0])
                else:
                    recentKeyVersion = max(recentKeyVersion, int(logList[0]))
        if recentKeyVersion is None:
            versions[key] += 1
        else:
            if recentKeyVersion < versions[key]:
                versions[key] += 1
            else:
                versions[key] = recentKeyVersion + 1
    # version = f"{versions[key]}.{node_id}.{key}.1" ## set to 1 to ensure causal consistency
    version = f"{versions[key]}.{node_id}.{key}"
    deps = set(client_meta_data)
    if key in store:
        deps.add(store[key]["version"])
    
    return version, deps

def merge_versions(local, client):

    if local["version"] in client["deps"]: 
        return client, 3
    elif client["version"] in local["deps"]:
        return local, 3
    else:
        client_ver_list = client["version"].split(".")
        local_ver_list = local["version"].split(".")
        if int(client_ver_list[0]) > int(local_ver_list[0]):
            return client, 0
        elif int(client_ver_list[0]) < int(local_ver_list[0]):
            return local, 1
        else:
            if client_ver_list[1] > local_ver_list[1]:
                return client, 0
            else:
                return local, 1
        #     else:
        #         # if client_ver_list[3] == "1":
        #         #     return client, 0
        #         # elif local_ver_list[3] == "1":
        #         #     return local, 1
        #         pass
        # return client, 0 if client["version"] > local["version"] else local, 1 ## compare actual version #s, then if marked, then node ids as tie breaker
    

def wait_for_dependencies(client_meta_data, key, timeout=10):
    start = time.time()
    missing_deps = [v for v in client_meta_data if v not in known_versions]
    if not client_meta_data:
        return True
    if not missing_deps:
        return True
    
    while time.time() - start < timeout:
        still_missing = [v for v in missing_deps if v not in known_versions]
        if not still_missing:
            return True
            
        if request_missing_dependencies(still_missing):
            return True
            
        time.sleep(0.5)
    
    # if is_isolated() and not strictly_requires_dependencies(key, missing_deps):
    #     return True
        
    return False

# def is_isolated():
#     unreachable = 0
#     for nid, addr in view.items():
#         if nid == node_id:
#             continue
#         try:
#             url = f"http://{addr}/ping"
#             requests.get(url, timeout=0.5)
#         except requests.exceptions.RequestException:
#             unreachable += 1

#     return unreachable == len(view) - 1 if len(view) > 1 else False

def strictly_requires_dependencies(key, deps):
    if key not in store and deps:
        return True

    if key in store and deps:
        return False
        
    return False

def request_missing_dependencies(client_meta_data, key):
    ## This method should check if our kv store has a version of the key
    greatestVersion = None ## [<version#>, <node_id>, <key>, <marked (0 | 1)>]
    for item in (client_meta_data):
        logList = item.split(".")
        if logList[2] == key:
            if greatestVersion is None:
                greatestVersion = logList
            else:
                if int(logList[0]) > int(greatestVersion[0]):
                    greatestVersion = logList
    if greatestVersion is None:
        return True, 3
    if key in versions:
        if versions[key] > int(greatestVersion[0]):
            return True, 3
        elif versions[key] == int(greatestVersion[0]):
            item = store[key]["version"]
            item = item.split(".")
            if item[1] == greatestVersion[1]:
                return True, 3
            else:
                if item[1] > greatestVersion[1]:
                    return True, 3
                # if greatestVersion[3] == "0": ## if its not marked, we decide it and mark it 
                #     return True, 4 ## we must remember to mark this version

    success = False
    while(success == False):
        for nid, addr in view[current_shard].items():
            if nid == node_id:
                continue
            
            try: 
                url = f"http://{addr}/request_versions" ## I can request a specific version, you can give me either that specific version or a more up to date version and also give me all the causal history for that version
                resp = requests.post(url, json={"versions": ".".join(greatestVersion)}, timeout=1)
                
                if resp.status_code == 200:
                    response_data = resp.json()
                    
                    if "versions" in response_data and response_data["versions"]:
                        for key, entry in response_data["versions"].items():
                            client_payload = {
                                "value": entry["value"],
                                "version": entry["version"],
                                "deps": set(entry["deps"])
                            }
                            if key in store:
                                local_payload = store[key]
                                merged, notSelectedId = merge_versions(local_payload, client_payload)
                                    
                                store[key] = merged
                                mergedList = merged["version"].split(".")
                                versions[key] = int(mergedList[0])
                                # store[key] = merged
                                known_versions.update(store[key]["deps"])
                            else:
                                store[key] = client_payload
                                clientVerList = client_payload["version"].split(".")
                                versions[key] = int(clientVerList[0])
                                known_versions.update(store[key]["deps"])
                                notSelectedId = 3
                    #         else:
                    #             store[key] = client_payload
                    #             known_versions.add(client_payload["version"])
                        success = True
                        break
            except requests.exceptions.RequestException:
                continue
        
        if key in versions:
            if versions[key] > int(greatestVersion[0]):
                return True, 3
            elif versions[key] == int(greatestVersion[0]):
                item = store[key]["version"]
                item = item.split(".")
                if item[1] == greatestVersion[1]:
                    return True, 3
                else:
                    if item[1] > greatestVersion[1]:
                        return True, 3
        
    return success, notSelectedId


@server.route("/request_versions", methods=["POST"])
def handle_version_request():
    data = request.get_json()
    if not data or "versions" not in data:
        return jsonify({"error": "Invalid request format"}), 400
        
    requested_versions = data["versions"]
    response_data = {}
    requestedList = requested_versions.split(".")
    for key, entry in store.items():
        versionList = entry["version"].split(".")
        if versionList[2] == requestedList[2]:
            if int(versionList[0]) > int(requestedList[0]):
                response_data[key] = {
                    "value": entry["value"],
                    "version": entry["version"],
                    "deps": list(entry["deps"])
                }
                break
            elif int(versionList[0]) == int(requestedList[0]):
                if versionList[1] != requestedList[1]:
                    return jsonify({"error": "Don't have requested version!"}), 404
                else:
                    response_data[key] = {
                    "value": entry["value"],
                    "version": entry["version"],
                    "deps": list(entry["deps"])
                    }
                    break
    
    return jsonify({"versions": response_data}), 200

def add_gossip_queue(key, value, version, deps):
    content = {"key": key, "value": value, "version": version, "deps": list(deps)}
    gossipQueue.append(content)


def process_gossip():
    while True:
        if not gossipQueue:
            continue
        payload = gossipQueue.popleft()
        acked_nodes = set()

        while len(acked_nodes) < (len(view[current_shard]) - 1):
            viewKeys = list(view[current_shard].keys())
            for nid in viewKeys:
                addr = view[current_shard][nid]
                if nid == node_id or nid in acked_nodes:
                    continue
                url = f"http://{addr}/gossip"
                try:
                    resp = requests.post(url, json=payload, timeout=1)
                    if resp.status_code == 200:
                        acked_nodes.add(nid)
                except Exception:
                    pass



def process_gossip_sync(key, value, version, deps, target_shard, nid): ## not related to process gossip
    payload = {"key": key, "value": value, "version": version, "deps": list(deps)}

    while True:
        # if nid in view[target_shard]:
        #     addr = view[target_shard][nid]
        # else:
        #     break
        addr = view[target_shard][nid]
        if nid == node_id:
            break
        url = f"http://{addr}/gossip"
        try:
            resp = requests.post(url, json=payload, timeout=1)
            if resp.status_code == 200:
                break
        except Exception:
            continue


                
# if a new node ever joins the view and we cant reach them, we hang forever
def periodic_gossip(nid, addr, shardToSend):
    while True:
        if not store:
            return
        payload = {
            "store": {
                key: {
                    "value": entry["value"],
                    "version": entry["version"],
                    "deps": list(entry["deps"])
                } for key, entry in store.items()
            }
        }
        if nid == node_id:
            continue
        found = False
        for item in view[shardToSend]:
            if nid == item:
                found = True
                break
        if not found:
            return
        try:
            url = f"http://{addr}/gossip"
            requests.post(url, json=payload, timeout=1)
            return
        except Exception:
            continue
        

@server.route("/gossip", methods=["POST"])
def recieve_protocol():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid gossip payload"}), 400

    if "store" in data:
        client_store = data["store"]
        for key, client_entry in client_store.items():
            client_payload = {
                "value": client_entry["value"],
                "version": client_entry["version"],
                "deps": set(client_entry["deps"])
            }
            if key in store:
                local_payload = store[key]
                merged, _= merge_versions(local_payload, client_payload)
                store[key] = merged
                mergedList = merged["version"].split(".")
                versions[key] = int(mergedList[0])
                known_versions.add(merged["version"])
            else:
                store[key] = client_payload ## why dont we update the versions dictionary too?
                client_payloadList = client_payload["version"].split(".")
                versions[key] = int(client_payloadList[0])
                known_versions.add(client_payload["version"])

    else:  
        key = data["key"]
        client_content = {"value": data["value"], "version": data["version"], "deps": set(data["deps"])}

        if key in store:
            local_content = store[key]
            merged, _ = merge_versions(local_content, client_content)
            store[key] = merged
            mergedList = merged["version"].split(".")
            versions[key] = int(mergedList[0])
            known_versions.add(merged["version"])
        else:
            store[key] = client_content ## why not update the versions dictionary here too?
            client_payloadList = client_content["version"].split(".")
            versions[key] = int(client_payloadList[0])
            known_versions.add(client_content["version"])
        
    return jsonify({"message": "Gossip protocol merged"}), 200


@server.route("/ping", methods=["GET"])
def ping():
    return jsonify({"value": "Ready to recieve requests"}), 200

    
def copyView(data): ## format the data json dump to fit our view format
    out = {}
    for shard in data["view"]:
        out[shard] = {}
        for node in data["view"][shard]:
            out[shard][int(node["id"])] = node["address"]
    return out

@server.route("/view", methods=["PUT"])
def update_view():
    global view, shard_ring, current_shard, store, versions

    data = request.get_json()
    if not data or "view" not in data:
        return jsonify({"error": "view is not included"}), 400

    if not data["view"]:
        for item in view:
            shard_ring.remove_node(item)        
        view.clear()
        current_shard = None
        return jsonify({"value": "View is empty"}), 200


    old_shard = current_shard
    current_shard = None
    # for shard, nodes in data["view"].items():
    #     for node in nodes:
    #         if int(node["id"]) == node_id:
    #             current_shard = shard
    #             break
    #     if current_shard:
    #         break
    if not view: # if we have empty view
        if not store: # if we are actually a new node
            view = copyView(data)
            for item in view:
                shard_ring.add_node(item)
            for shard in view:
                for node in view[shard]:
                    if node == node_id:
                        current_shard = shard
                        return jsonify({"message": f"View recieved, CURRENT SHARD: {current_shard}"}), 200
            return jsonify({"error": "Node not found in view"}), 200

    old_view = view
    view = copyView(data)
    
    
    for shard in old_view:
        if shard not in view:
            shard_ring.remove_node(shard)
    for shard in view:
        if shard not in old_view:
            shard_ring.add_node(shard)
    
    toBeDeleted = set()
    for key in store:
        target_shard = shard_ring.get_node(key)
        for nid in view[target_shard]:
            if target_shard != old_shard and nid != node_id:
                process_gossip_sync(key, store[key]["value"], store[key]["version"], store[key]["deps"], target_shard, nid)
                toBeDeleted.add(key)
    for item in toBeDeleted:
        if item in store:
            del store[item]
    for shard in view:
        for node in view[shard]:
            if shard == old_shard:
                if node not in old_view[shard] and node != node_id:
                    periodic_gossip(node, view[shard][node], shard)
            if node == node_id:
                current_shard = shard
                #periodic_gossip(nid, view[target_shard][nid], target_shard)
            # if nid != node_id:
            #     periodic_gossip(nid, view[target_shard][nid], target_shard)
            
    if not current_shard:  #nodeNotInView
        return jsonify({"error": "Node not found in view"}), 200
    # After updated all new nodes in your old shard, go through and delete any keys that dont hash to your current shard
    if current_shard != old_shard:
        storeKeys = list(store.keys())
        for key in storeKeys:
            target_shard = shard_ring.get_node(key)
            if target_shard != current_shard:
                del store[key] 
    
        
    return jsonify({"message": "View recieved"}), 200
    
def do_Get(key, client_meta_data):
    returnShard = shard_ring.get_node(key)
    if returnShard != current_shard:
        return None, None
    if key in store:
        temp = store[key]["version"]
    else:
        temp = None
    if client_meta_data is None:
        client_meta_data = []
    new_meta_data = set(client_meta_data)
    our_dep_data = set()
    for dep in new_meta_data:
        depList = dep.split(".")
        depShard = shard_ring.get_node(depList[2])
        if depShard == current_shard:
            our_dep_data.add(dep)
    success, notSelectedId = request_missing_dependencies(our_dep_data, key)
    # if notSelectedId == 4:
    #     old_temp = store[key]["version"]
    #     tmp = old_temp.split(".")
    #     tmp[3] = "1"
    #     tmp = ".".join(tmp)
    #     store[key]["version"] = tmp
    #     if old_temp in store[key]["deps"]:
    #         store[key]["deps"].remove(old_temp)
    #     store[key]["deps"].add(tmp)
    #     greatestVersion = None ## [<version#>, <node_id>, <key>, <marked (0 | 1)>]
    #     for item in (client_meta_data):
    #         logList = item.split(".")
    #         if logList[2] == key:
    #             if greatestVersion is None:
    #                 greatestVersion = logList
    #             else:
    #                 if int(logList[0]) > int(greatestVersion[0]):
    #                     greatestVersion = logList
    #     greatestVersionStr= ".".join(greatestVersion)
    #     new_meta_data.remove(greatestVersionStr)
    #     if old_temp in new_meta_data:
    #         new_meta_data.remove(old_temp)
    #         new_meta_data.add(tmp)
        
    #     add_gossip_queue(key, store[key]["value"], store[key]["version"], store[key]["deps"])
    entry = store[key]
    if notSelectedId == 1:
        greatestVersion = None ## [<version#>, <node_id>, <key>, <marked (0 | 1)>]
        for item in (client_meta_data):
            logList = item.split(".")
            if logList[2] == key:
                if greatestVersion is None:
                    greatestVersion = logList
                else:
                    if int(logList[0]) > int(greatestVersion[0]):
                        greatestVersion = logList
        greatestVersionStr= ".".join(greatestVersion)
        new_meta_data.remove(greatestVersionStr)
    elif notSelectedId == 0:
        if temp in new_meta_data:
            new_meta_data.remove(temp)
    new_meta_data.update(entry["deps"])
    new_meta_data.add(entry["version"])
        ## If we picked up a different version of this key from another node, that was concurrent with ours, we replace our version in the client's meta data with the other version we picked up

    return entry["value"], list(new_meta_data)

@server.route("/data/<key>", methods=["GET", "PUT", "DELETE"])
def handle_data(key):
    view_error = check_view_status()
    if view_error:
        return view_error

    data = request.get_json()
    client_meta_data = data.get("causal-metadata", [])
    if request.method == "GET":
        shard_name = shard_ring.get_node(key)
        if shard_name != current_shard:
            while(True):
                for _, addr in view[shard_name].items():
                    try:
                        url = f"http://{addr}/data/{key}"
                        resp = requests.get(url, json={"causal-metadata": list(client_meta_data)}, timeout=1)
                        return resp.json(), resp.status_code
                        
                    except Exception:
                        continue

        if key in store:
            temp = store[key]["version"]
        else:
            temp = None
        new_meta_data = set(client_meta_data)
        our_dep_data = set()
        for dep in new_meta_data:
            depList = dep.split(".")
            depShard = shard_ring.get_node(depList[2])
            if depShard == current_shard:
                our_dep_data.add(dep)
        success, notSelectedId = request_missing_dependencies(our_dep_data, key)

        if notSelectedId == 4:
            old_temp = store[key]["version"]
            tmp = old_temp.split(".")
            tmp[3] = "1"
            tmp = ".".join(tmp)
            store[key]["version"] = tmp
            if old_temp in store[key]["deps"]:
                store[key]["deps"].remove(old_temp)
            store[key]["deps"].add(tmp)
            greatestVersion = None ## [<version#>, <node_id>, <key>, <marked (0 | 1)>]
            for item in (client_meta_data):
                logList = item.split(".")
                if logList[2] == key:
                    if greatestVersion is None:
                        greatestVersion = logList
                    else:
                        if int(logList[0]) > int(greatestVersion[0]):
                            greatestVersion = logList
            greatestVersionStr= ".".join(greatestVersion)
            new_meta_data.remove(greatestVersionStr)
            if old_temp in new_meta_data:
                new_meta_data.remove(old_temp)
                new_meta_data.add(tmp)
            
            add_gossip_queue(key, store[key]["value"], store[key]["version"], store[key]["deps"])
        if key not in store or store[key]["version"] is None:
            return jsonify({
                "error": "Key not found in store", 
                "causal-metadata": list(client_meta_data)
            }), 404
        entry = store[key]
        if notSelectedId == 1:
            greatestVersion = None ## [<version#>, <node_id>, <key>, <marked (0 | 1)>]
            for item in (client_meta_data):
                logList = item.split(".")
                if logList[2] == key:
                    if greatestVersion is None:
                        greatestVersion = logList
                    else:
                        if int(logList[0]) > int(greatestVersion[0]):
                            greatestVersion = logList
            greatestVersionStr= ".".join(greatestVersion)
            new_meta_data.remove(greatestVersionStr)
        elif notSelectedId == 0:
            if temp in new_meta_data:
                new_meta_data.remove(temp)
        new_meta_data.update(entry["deps"])
        new_meta_data.add(entry["version"])
         ## If we picked up a different version of this key from another node, that was concurrent with ours, we replace our version in the client's meta data with the other version we picked up
            
        return jsonify({"value": entry["value"], "causal-metadata": list(new_meta_data)}), 200

    elif request.method == "PUT":
        if "value" not in data:
            return jsonify({"error": "Missing value input"}), 400
        
        shard_name = shard_ring.get_node(key)
        if shard_name != current_shard:
            while(True):
                for _, addr in view[shard_name].items():
                    try:
                        url = f"http://{addr}/data/{key}"
                        resp = requests.put(url, json={"value": data["value"], "causal-metadata": list(client_meta_data)}, timeout=1)
                        return resp.json(), resp.status_code
                        
                    except Exception:
                        continue

        # mergeCausalHistories(key, client_meta_data)
        new_version, deps = create_new_version(key, client_meta_data)
        new_entry = {"value": data["value"], "version": new_version, "deps": deps}
        if key in store:
            # with open(f"testboofarmfs{node_id}.txt", "w") as file:
            #     file.write(f"{node_id}: {store[key]}, {new_entry}\n")
            merged, _ = merge_versions(store[key], new_entry)
            store[key] = merged
        else:
            store[key] = new_entry

        known_versions.add(new_version)
        new_meta_data = set(client_meta_data)
        new_meta_data.update(deps)
        new_meta_data.add(new_version)

        add_gossip_queue(key, data["value"], new_version, deps)
        store[key]["deps"] = deps
        return jsonify({"message": "Put request successfull", "causal-metadata": list(new_meta_data)}), 200
    
    elif request.method == "DELETE":
        pass

@server.route("/data", methods=["GET"])
def get_all():
    view_error = check_view_status()
    if view_error:
        return view_error

    data = request.get_json()
    client_meta_data = data.get("causal-metadata", [])
    if client_meta_data == None:
        client_meta_data = []
    result = {}
    additional = set()
    for item in client_meta_data:
        listItem= item.split(".")
        if listItem[2] not in store:
            additional.add(listItem[2])
    store_keys = list(store.keys())  
    for key in store_keys:
        entry = store[key]
        value, client_meta_data = do_Get(key, client_meta_data)
        if not value and not client_meta_data:
            continue
        if client_meta_data == None:
            client_meta_data = []
        result[key] = value
    if client_meta_data == None:
        client_meta_data = []
    for item in additional:
        value, client_meta_data = do_Get(item, client_meta_data)
        if not value and not client_meta_data:
            continue
        if client_meta_data == None:
            client_meta_data = []
        result[key] = value
    if client_meta_data == None:
        client_meta_data = []
    for item in client_meta_data:
        listItem= item.split(".")
        if listItem[2] not in store:
            additional.add(listItem[2])
    for key in store_keys:
        value, client_meta_data = do_Get(key, client_meta_data)
        if not value and not client_meta_data:
            continue
        if client_meta_data == None:
            client_meta_data = []
        result[key] = value
    if client_meta_data == None:
        client_meta_data = []
    for item in additional:
        value, client_meta_data = do_Get(item, client_meta_data)
        if not value and not client_meta_data:
            continue
        if client_meta_data == None:
            client_meta_data = []
        result[key] = value
    if client_meta_data == None:
        client_meta_data = []
    return jsonify({"items": result, "causal-metadata": client_meta_data}), 200

def check_view_status():
    node_id = os.getenv("NODE_IDENTIFIER")
    if node_id is None:
        return jsonify({"error": "Node ID not found"}), 503
    node_id = int(node_id)
    if not current_shard:
        return jsonify({"error": "Node is not online"}), 503
    elif not view or node_id not in view[current_shard]:
        return jsonify({"error": "Node is not online"}), 503
    return None

if __name__ == "__main__":
    threading.Thread(target=process_gossip, daemon=True).start()
    # threading.Thread(target=periodic_gossip, daemon=True).start()
    server.run(host="0.0.0.0", port=8081, threaded=True)