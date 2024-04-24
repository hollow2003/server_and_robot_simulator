from flask import Flask,request, jsonify
import socket
import threading
import requests
import time
import json
import cv2
import jsonschema
from sqlalchemy import Column, Float, Integer, String, ForeignKey, create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor
import sqlite3
engine = create_engine('sqlite:///server.db', echo=False)
app = Flask(__name__)
Base = declarative_base()
lock = threading.Lock()
raw_cache_lock = threading.Lock()
deal_cache_lock = threading.Lock()
hostlist_lock = threading.Lock()
Session = sessionmaker(bind=engine)
session = Session()
# 存储接收到的 UDP 消息
udp_message = None
hostname_schemas_orms = {}
class_name_suffix = 1
local_data = threading.local()
raw_data_cache = []
deal_data_cache = []
timer = 1
def receive_udp_message():
    global udp_message
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind(('localhost', 12345))

    while True:
        data, addr = udp_socket.recvfrom(1024)
        udp_message = data.decode()
        parts = udp_message.split(' ')
        port, hostname, ip = None, None, None
        
        for part in parts:
            if "PORT" in part:
                port = part.split(":")[1]
            elif "hostname" in part:
                hostname = part.split(":")[1]
            elif "IP" in part:
                ip = part.split(":")[1]

        if port and hostname:
            address = f"http://{ip}:{port}"
            hostlist_lock.acquire()
            if hostname not in hostname_schemas_orms:
                hostname_schemas_orms[hostname] = {}
                get_schema(address, hostname)
            hostlist_lock.release()
        else:
            print("无法解析消息中的端口号和主机名")
            #print(f"Received UDP message: {udp_message}")

def get_schema(address, hostname):
    url = f"{address}/data_discovery"
    response = requests.get(url)  
    if response.status_code == 200:
        schemas = response.json()
        hostname_schemas_orms[hostname]["schemas"] = schemas
        for schema in schemas:
            schema["established"] = 0
            schema["interested"] = 0
            lock.acquire()
            schema["orm"] = generate_orm(schema_to_tree(schema['schema']),hostname,Path = hostname,required = extract_required(schema['schema']))          
            Base.metadata.create_all(engine)
            Base.metadata.clear()
            lock.release()
            if schema["orm"] != None:
                schema["established"] = 1
            if schema.get('API').get('protocol') == 'RTSP':
                schema["c_flag"] = 0
    else:
        print("Failed to get schema.")

def generate_orm(json_tree,host_name,parent_name=None,Path = '', required=[]):
    require = required
    orm_classes = {}
    global class_name_suffix
    table = {}
    if type(json_tree) == list:
        json_tree = json_tree[0]
        if parent_name==None:
            Path = '\\root'
    for key,value in json_tree.items():
        if type(json_tree[key]) == str:
            table[key] = json_tree[key]
        elif type(json_tree[key]) == dict:
            new_path = Path + "\\" + key
            sub_orm_classes = generate_orm(json_tree[key],host_name,parent_name=key, Path = new_path, required=require)
            orm_classes.update(sub_orm_classes)
        elif type(json_tree[key]) == list:
            new_path = Path + "\\" + key
            sub_orm_classes = generate_orm(json_tree[key][0],host_name,parent_name=key, Path = new_path, required=require)
            orm_classes.update(sub_orm_classes)
    if parent_name != None and table != {}:
        tmp = Path
        orm_classes[parent_name] = create_orm_class(Path,host_name,table,parent_name = tmp,required_fields = require)
    elif parent_name == None and table != {}:
        orm_classes['root'] = create_orm_class('\\root',host_name,table,required_fields = require)
    return orm_classes

def create_orm_class(table_name,host_name,properties,parent_name = None,required_fields = []):
    global class_name_suffix
    class_name = f"DynamicORM_{class_name_suffix}"
    class_name_suffix += 1
    true_table_name = table_name
    if parent_name != None:
        last_backslash_index = parent_name.rfind("\\")
        if last_backslash_index != -1:
            new_path = parent_name[:last_backslash_index]
        else:
            new_path = parent_name
        #print(new_path)
    else:
        new_path = ''
    class ORM(Base):
        __tablename__ = true_table_name
        __classname__ = class_name
        if new_path !='' and new_path != host_name:
            timestamp = Column(String, ForeignKey(f"{new_path}.timestamp")) if parent_name else None
            id = Column(Integer, primary_key=True, autoincrement=True)
        else:
            timestamp = Column(String, primary_key=True)
    for key,value in properties.items():
        field_type = value
        is_required = key in required_fields
        if key == "timestamp":
            continue 
        if field_type == 'string':
            field = Column(String, nullable=not is_required)
        elif field_type == 'integer':
            field = Column(Integer, nullable=not is_required)
        elif field_type == 'number':
            field = Column(Float, nullable=not is_required)

        setattr(ORM, key, field)

    return ORM

def extract_required(schema):
    required_fields = []

    if 'required' in schema:
        required_fields.extend(schema['required'])

    if 'properties' in schema:
        for prop_schema in schema['properties'].values():
            required_fields.extend(extract_required(prop_schema))

    if 'items' in schema:
        if isinstance(schema['items'], dict):
            required_fields.extend(extract_required(schema['items']))
        elif isinstance(schema['items'], list):
            for item_schema in schema['items']:
                required_fields.extend(extract_required(item_schema))

    return required_fields

def schema_to_tree(schema):
    if isinstance(schema, dict):
        if 'properties' in schema and isinstance(schema['properties'], dict):
            properties_tree = {}
            for prop_name, prop_schema in schema['properties'].items():
                properties_tree[prop_name] = schema_to_tree(prop_schema)
            return properties_tree
        elif 'items' in schema:
            if isinstance(schema['items'], dict):
                if schema['type'] == 'array':
                    items_tree = [schema_to_tree(schema['items'])]
                    return items_tree
                else:
                    return schema_to_tree(schema['items'])
            elif isinstance(schema['items'], list):
                items_tree = []
                for item_schema in schema['items']:
                    items_tree.append(schema_to_tree(item_schema))
                return items_tree
        elif 'type' in schema:
            return schema['type']
    return None

def rtsp_thread(address):
    cap = cv2.VideoCapture(address)

    while True:
        ret, frame = cap.read()
        if ret:
            cv2.imshow('RTSP Video', frame)
            cv2.waitKey(1)
        else:
            print("Failed to read frame from RTSP stream.")
            break

    cap.release()
    cv2.destroyAllWindows()

def validate_data(schema, data):
    try:
        jsonschema.validate(instance=data, schema=schema)
    except jsonschema.ValidationError as e:
        print(f"Validation error:{e}")
        return False
    return True

def extract_and_remove_sub_dicts(dictionary, sister_dic):
    keys_to_delete = []
    if type (dictionary) == dict:
        for key, value in dictionary.items():
            if isinstance(value, dict):
                sister_dic[key] = value
                keys_to_delete.append(key)
                extract_and_remove_sub_dicts(value, sister_dic)
            if isinstance(value, list):
                sister_dic[key] = value
                keys_to_delete.append(key)
                extract_and_remove_sub_dicts(value, sister_dic)
        for key in keys_to_delete:
            del dictionary[key]
    if type(dictionary) == list:
        for item in dictionary:
            extract_and_remove_sub_dicts(item,sister_dic)

    return dictionary,sister_dic

def init_local_data():
    global hostname_schemas_orms
    local_data.hostname_schemas_orms = hostname_schemas_orms.copy()

def get_local_data():
    return local_data.hostname_schemas_orms

def get_data_periodically():
    start_time = time.time()
    init_local_data()
    global timer
    local_hostname_schemas_orms = get_local_data()
    for key in local_hostname_schemas_orms:
        if "schemas" not in local_hostname_schemas_orms[key]:  # 如果不存在 schemas，则跳过该 hostname
            continue
        for schema in local_hostname_schemas_orms[key]["schemas"]:
            #if schema["interested"] != 1:
            #    continue
            #if timer%schema["cycle"] != 0:
            #    continue
            address = schema.get("API").get("address")    
            if (address) and (schema.get("API").get("protocol") == 'REST'):
                method = schema.get("API").get("method")
                try:
                    response = requests.request(method, address)
                    if response.status_code == 200:
                        data = response.json()
                        #print(f"Received data from {key}: {data}")
                        if schema.get("established") == 1 and validate_data(schema["schema"], data):
                            if type(data) == list:
                                data = {"root": data}
                            data["orm"] = schema["orm"]
                            raw_cache_lock.acquire()
                            #print(str(data) + "add to raw data")
                            raw_data_cache.append(data)
                            raw_cache_lock.release()
                    else:
                        print(f"Failed to get data from {key}.")
                except requests.ConnectionError as e:
                    print(f"Connection error: {e}. Skipping {address}")
            elif (address) and (schema.get("API").get("protocol") == 'RTSP') and (schema["c_flag"] == 0):
                schema["c_flag"] = 1
                rtsp_thread_worker = threading.Thread(target=rtsp_thread, args=(address,))
                rtsp_thread_worker.start()
    end_time = time.time()
    duration = end_time - start_time
    print(f"100API took {duration} seconds to execute.")

def periodic_data_thread():
     with ThreadPoolExecutor(max_workers=16) as executor:
        while True:
            future = executor.submit(get_data_periodically)
            time.sleep(1)
            future.result()
            

def storage_thread():
    while True:
        loop_start_time = time.time()
        deal_cache_lock.acquire()
        if len(deal_data_cache) >= 30:
            for item in deal_data_cache:
                if isinstance(item, list):
                    session.add_all(item)
                else:
                    session.add(item)
            deal_data_cache.clear()
            session.commit()
        deal_cache_lock.release()
        end_time = time.time()
        duration = end_time - loop_start_time
        print(f"100API took {duration} seconds to storage.")
        wait_time = max(0, 1 - duration)
        time.sleep(wait_time)

def extrace_thread():
    while True:
        loop_start_time = time.time()
        raw_cache_lock.acquire()
        print(len(raw_data_cache))
        if len(raw_data_cache) >= 30:
            for item in raw_data_cache:
                orm = item["orm"]
                del item["orm"]
                sister_dic = {}
                sub_dicts, sister_dic = extract_and_remove_sub_dicts(item, sister_dic)
                deal_cache_lock.acquire()
                for key, value in sister_dic.items():
                    if key in orm:
                        orm_class = orm[key]
                        if type(value) == list:
                            orm_instances = [orm_class(**item) for item in value]
                            deal_data_cache.append(orm_instances)
                        elif type(value) == dict:
                            orm_instance = orm_class(**value)
                            timestamp = None
                            if timestamp == None:
                                orm_instance.timestamp = str(time.time())
                            elif timestamp != None:
                                orm_instance.timestamp = timestamp      
                            deal_data_cache.append(orm_instance)
                deal_cache_lock.release()
            raw_data_cache.clear()
        raw_cache_lock.release()
        end_time = time.time()
        duration = end_time - loop_start_time
        print(f"100API took {duration} seconds to extrace.")
        wait_time = max(0, 1 - duration)
        time.sleep(wait_time)

def clock_thread():
    while True:
        global timer
        timer = (timer + 1)%10000
        time.sleep(1)

@app.route('/udp_message')
def get_udp_message():
    global udp_message
    return udp_message

@app.route('/interest_topic', methods=['POST'])
def get_interest_topic():
    data = request.json
    hostlist_lock.acquire()
    for item in data["interest"]:
        if item["host_name"] in hostname_schemas_orms:
            for topic_and_cycle in item["interest_topic"]:
                for schema in hostname_schemas_orms[item["host_name"]]["schemas"]:
                    if topic_and_cycle["topic"] in schema["orm"]:
                        schema["interested"] = 1
                        schema["cycle"] = int(topic_and_cycle["cycle"])
                        break
    hostlist_lock.release()
    print("Receive interest topic")
    return "receive interest topic"

# {
#     "interest":[
#         {
#             "host_name" : "robot_1",
#             "interest_topic" :  [{"topic": "",
#                                   "cycle": "1"}]
#         }
#     ]
# }
@app.route('/latest_location', methods=['GET'])
def get_latest_location():
    data = request.json
    host_name = data["host_name"]
    try:
        # 连接数据库
        conn = sqlite3.connect('server.db')
        cursor = conn.cursor()

        # 构建 SQL 查询语句
        sql_query = f"SELECT * FROM `{host_name}\Location` ORDER BY timestamp DESC LIMIT 1;"

        # 执行查询
        cursor.execute(sql_query)

        # 获取查询结果
        result = cursor.fetchone()
        column_names = [description[0] for description in cursor.description]
        if result:
            # 获取查询结果的列名
            column_names = [description[0] for description in cursor.description]
            # 将查询结果转换为字典
            result_dict = dict(zip(column_names, result))
            # 返回 JSON 格式的响应
            # 关闭数据库连接
            conn.close()
            print(result_dict)
            return jsonify(result_dict)
        else:
            return jsonify({"message": "No data found"}), 404
    except Exception as e:
        # 打印异常信息以便调试
        print("An error occurred:", e)
        return jsonify({"error": str(e)}), 500

timer_thread = threading.Thread(target=periodic_data_thread)
timer_thread.start()
udp_thread = threading.Thread(target=receive_udp_message)
udp_thread.start()
str_thread = threading.Thread(target=storage_thread)
str_thread.start()
ext_thread = threading.Thread(target=extrace_thread)
ext_thread.start()
clock_thread = threading.Thread(target=clock_thread)
clock_thread.start()
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
    session.close()
#1s 82api