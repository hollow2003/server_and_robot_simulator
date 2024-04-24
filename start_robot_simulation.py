import os
import time
import threading

# 定义启动机器人模拟程序的函数
def start_robot_simulation(port, hostname):
    command = f"/usr/bin/python3.8 robot_simulation.py --port {port} --hostname {hostname}"
    os.system(command)

# 主函数，用于生成和启动多个机器人模拟程序
def main():
    # 定义要启动的机器人数量
    num_robots = 100

    # 遍历所有机器人，生成并启动机器人模拟程序
    for i in range(num_robots):
        port = 8081 + i  # 每个机器人使用不同的端口
        hostname = f"robot_{i + 1}"  # 每个机器人有不同的名称
        threading.Thread(target=start_robot_simulation, args=(port, hostname)).start()
        time.sleep(0.1)  # 等待0.1秒，以免太快启动过多进程

if __name__ == "__main__":
    main()