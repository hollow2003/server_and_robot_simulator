import socket
import threading
from flask import Flask, jsonify,Response
import random
import time
from datetime import datetime
import cv2
import json
import base64
import argparse

app = Flask(__name__)
parser = argparse.ArgumentParser(description='Robot Simulation Program')
parser.add_argument('--port', type=int, default=8081, help='Port number for the robot simulation program')
parser.add_argument('--hostname', type=str, default='robot_1', help='Hostname for the robot simulation program')
args = parser.parse_args()

# 定义UDP发送函数
def send_udp_message(message, host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(message.encode(), (host, port))
    sock.close()

def start_udp_sender():
    # 持续发送UDP消息
    target_host = "localhost"
    target_port = 12345
    message = f"UDP PORT:{args.port} hostname:{args.hostname} IP:127.0.0.1"
    while True:
        send_udp_message(message, target_host, target_port)
        time.sleep(5)

def start_http_server():
    # 启动HTTP服务器
    app.run(host='0.0.0.0', port=args.port, threaded=True)  # 使用多线程模式

def get_camera_properties():
    # 打开摄像头
    cap = cv2.VideoCapture(0)
    
    # 获取摄像头帧率
    fps = cap.get(cv2.CAP_PROP_FPS)
    # 获取摄像头分辨率
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    resolution = f"{width}x{height}"

    # 释放资源
    cap.release()

    return fps, resolution

def generate_h264_stream():
    # 打开摄像头
    cap = cv2.VideoCapture(0)

    # 设置视频编码器和输出视频尺寸
    # fourcc = cv2.VideoWriter_fourcc(*'H264')
    # out = cv2.VideoWriter('output.h264', fourcc, 20.0, (640, 480))

    # while cap.isOpened():
    #     ret, frame = cap.read()
    #     if not ret:
    #         break
        
    #     # 将每一帧写入输出视频流
    #     out.write(frame)

    #     # 将帧转换为字节流
    #     ret, buffer = cv2.imencode('.h264', frame)
    #     frame_bytes = buffer.tobytes()

    #     # 生成视频流
    #     yield frame_bytes

    # 释放资源
    while True:
        # 读取视频帧
        ret, frame = cap.read()
        if not ret:
            break

        # 将帧转换为字节流
        ret, buffer = cv2.imencode('.jpg', frame)
        frame_bytes = buffer.tobytes()

        # 生成视频流
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n')

    # 释放资源
    cap.release()
    # cap.release()
    # out.release()

# 全局变量，控制是否发送 UDP 消息
send_udp = True

@app.route('/data_discovery')
def get_schema():
    global send_udp
    # 停止发送 UDP 消息
    send_udp = False
    
    schemas = [{
    "API": {
        "address": f"http://127.0.0.1:{args.port}/api/move",
        "protocol": "REST",
        "method": "GET"
    },
    "schema": {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "Location": {
            "type": "object",
            "properties": {
                "test": {
                    "type": "object",
                    "properties": {
                        "test_msg": {
                            "type": "object",
                            "properties": {
                                "test_t": {
                                    "type": "integer"
                                }
                            },
                            "required": [
                                "test_t"
                            ]
                        },
                        "timestamp1": {
                            "type": "string"
                        }
                    },
                    "required": [
                        "test_msg",
                        "timestamp1"
                    ]
                },
                "timestamp": {
                    "type": "string"
                },
                "x": {
                    "type": "number"
                },
                "y": {
                    "type": "number"
                },
                "z": {
                    "type": "number"
                }
            },
            "required": [
                "test",
                "timestamp",
                "x",
                "y",
                "z"
            ]
        }
    },
    "required": [
        "Location"
    ]
}
    # {"API":{
    #     "address": "rtsp://127.0.0.1:8081/api/video0",
    #     "protocol": "RTSP"
    # },
    # "ORM": {
    #     "class_name": "robot_1_Video0",
    #     "table_name": "robot_1_video0s"
    # },  
    # "fields": {
    #     "fps": {"type": "Float", "options": {"nullable": False}},
    #     "codec": {"type": "String", "options": {"nullable": False}},
    #     "resolution":{"type": "String", "options": {"nullable": False}}
    #     }
    # }
    }]
    return jsonify(schemas)

@app.route('/api/video0')
def video_feed():
    # 生成元数据
    fps, resolution = get_camera_properties()
    metadata = {
        "fps": fps,  # 帧率
        "codec": "H.264",  # 编解码器
        "resolution": resolution  # 分辨率
    }
    # 编码元数据为 JSON 字符串
    metadata_json = json.dumps(metadata)

    # 读取 H.264 视频流数据
    video_stream = generate_h264_stream()

    # 将 H.264 视频流数据编码为 base64
    encoded_video_stream = base64.b64encode(video_stream.read())

    # 构建要返回的 JSON 数据
    response_data = {
        "video_data": encoded_video_stream.decode(),  # 将 base64 编码后的视频流数据放入 JSON 数据中
        "metadata": metadata_json  # 将元数据放入 JSON 数据中
    }

    # 返回 JSON 格式的数据
    #return Response(generate_h264_stream(), mimetype='video/H264')
    # return video_stream
    Response(video_stream,
                    mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/api/move')
def get_move():
    # 生成随机坐标数据
    location = {
            "Location": {
            "timestamp": datetime.now(),
            "x": random.uniform(0, 100),
            "y": random.uniform(0, 100),
            "z": random.uniform(0, 100),
            "test": {"timestamp1": datetime.now(),"test_msg":{"test_t": 1}}
    }
    }
    return jsonify(location)

if __name__ == "__main__":
    # 启动HTTP服务器
    http_thread = threading.Thread(target=start_http_server)
    http_thread.start()
    # 持续发送UDP消息
    target_host = "localhost"
    target_port = 12345
    time.sleep(1)
    udp_thread = threading.Thread(target=start_udp_sender)
    udp_thread.start()

