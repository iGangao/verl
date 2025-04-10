      
import os
import sys
import time
import random
import socket
import argparse
import subprocess

# ray cluster 工作目录路径
WORK_DIR = "ray_work_dir"
HEAD_ADDRESS_FILE = "head_address"
JOB_DONE_FILE = "job_done"


def init_ray_work_dir(work_dir):
    global WORK_DIR, HEAD_ADDRESS_FILE, JOB_DONE_FILE
    merlin_job_id = os.environ.get('MERLIN_JOB_ID', 'UNKNOWN_JOB_ID')
    arnold_trial_id = os.environ.get('ARNOLD_TRIAL_ID', 'UNKNOWN_TRIAL_ID')
    WORK_DIR = os.path.join(work_dir, merlin_job_id)
    HEAD_ADDRESS_FILE = os.path.join(WORK_DIR, f"{arnold_trial_id}_head_address")
    JOB_DONE_FILE = os.path.join(WORK_DIR, f"{arnold_trial_id}_job_done")
    if not os.path.exists(WORK_DIR):
        os.makedirs(WORK_DIR)
    print(f"Ray 工作目录: {WORK_DIR}")


def _find_free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Binding to port 0 will cause the OS to find an available port for us
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    # NOTE: there is still a chance the port could be taken by other processes.
    return port


def start_ray_head(master_port):
    """
    在节点 worker_rank=0 上启动 Ray head，并从启动输出中解析 head_address.
    """
    # 运行 Ray head 命令，此处指定 port=master_port
    print("Starting ray head ...")
    proc = subprocess.Popen(
        ["ray", "start", "--head", f"--port={master_port}"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    # 通过 communicate 获取完整的标准输出和错误信息
    stdout, stderr = proc.communicate()
    # 从输出中查找类似 "ray start --address=" 的行
    head_addr = None
    for line in stdout.splitlines():
        if "ray start --address=" in line:
            # 假设格式类似: "ray start --address=192.168.1.100:37000"
            head_addr = line.split("ray start --address=")[-1].strip().replace("'", "")
            break

    if not head_addr:
        print("ERROR: 无法解析 head 地址，请检查 ray 启动输出。")
        sys.exit(1)

    print("Head address:", head_addr)

    # 将 head_address 写入固定文件
    with open(HEAD_ADDRESS_FILE, "w") as f:
        f.write(head_addr)
    print(f"Head address 信息写入 {HEAD_ADDRESS_FILE}")
    return head_addr


def wait_for_all_nodes(num_workers):
    """
    在 head 节点上循环监控工作目录下各个节点的标识文件，直到所有非 head 节点都已加入。
    :param num_workers: 总节点数（包括 head 节点）
    """
    print("监控其它节点的加入情况 ...")
    # head 节点对应 rank=0，所以需要等待 num_workers-1 个文件
    expected = num_workers - 1
    joined = set()
    arnold_trial_id = os.environ.get('ARNOLD_TRIAL_ID', 'UNKNOWN_TRIAL_ID')
    while len(joined) < expected:
        for fname in os.listdir(WORK_DIR):
            if fname.startswith(f"{arnold_trial_id}_worker_rank"):
                try:
                    # 文件名格式 "{arnold_trial_id}_worker_rank_{worker_rank}"
                    worker_rank = int(fname.strip('_')[-1])
                    joined.add(worker_rank)
                except Exception as e:
                    continue
        print(f"当前已加入的节点数：{len(joined)}/{expected}")
        time.sleep(5)
    print("所有节点均已加入！")


def submit_job_and_monitor(job_cmd):
    """
    在 head 节点上提交任务，并监控任务是否结束后才退出。
    :param job_cmd: 任务命令列表，例如 ["python", "job_script.py"]
    """
    print("提交任务，任务命令为：", " ".join(job_cmd))
    # 使用 Popen 启动任务
    proc = subprocess.Popen(job_cmd)
    print("任务已启动，等待任务结束...")

    # 循环监控任务
    while True:
        ret = proc.poll()
        if ret is not None:
            print("任务结束，返回码:", ret)
            break
        else:
            # 可增加进度输出或睡眠等待
            time.sleep(10)
    # 任务结束后，创建一个文件以表明任务已完成
    with open(JOB_DONE_FILE, "w") as f:
        f.write("done")
    print("任务监控完成。")


def join_ray_cluster(head_addr):
    """
    其它节点使用 head_addr 加入 Ray 集群。
    """
    print(f"使用 head_address {head_addr} 加入 ray 集群 ...")
    subprocess.call(["ray", "start", "--address", head_addr])
    print("已加入 Ray 集群。")


def signal_node_join(rank):
    """
    在工作目录下创建一个文件以表明该节点（非 head）已加入。
    """
    arnold_trial_id = os.environ.get('ARNOLD_TRIAL_ID', 'UNKNOWN_TRIAL_ID')
    node_file = os.path.join(WORK_DIR, f"{arnold_trial_id}_worker_rank_{rank}")
    with open(node_file, "w") as f:
        f.write("joined")
    print(f"创建文件 {node_file}，通知 head 节点该节点已加入。")


def wait_for_head_address():
    """
    循环等待直到 head_address 文件可用，然后读取地址。
    """
    print("等待 head_address 文件的生成 ...")
    while not os.path.exists(HEAD_ADDRESS_FILE):
        time.sleep(5)
    with open(HEAD_ADDRESS_FILE, "r") as f:
        head_addr = f.read().strip()
    print("检测到 head_address:", head_addr)
    return head_addr


def wait_for_job_done():
    """
    工作节点循环等待 job_done 文件生成后，方才退出。
    """
    print("等待 job_done 文件生成 ...")
    while not os.path.exists(JOB_DONE_FILE):
        time.sleep(5)
    print("检测到 job_done 文件，退出程序。")


def main():
    parser = argparse.ArgumentParser(description='Haggs Launcher')
    parser.add_argument('--launch', type=str, required=True, nargs="+",
                        help='Specify launcher command.')
    parser.add_argument('--ray-work-dir', type=str, default=WORK_DIR,
                        help='Specify ray work dir.')
    parser.add_argument('--port', type=int, default=-1,
                        help='master port for communication')
    args = parser.parse_args()

    num_workers = int(os.getenv('ARNOLD_WORKER_NUM'))
    worker_rank = int(os.getenv('ARNOLD_ID'))

    init_ray_work_dir(args.ray_work_dir)

    if args.port > 0:
        master_port = args.port
    else:
        master_port = _find_free_port()

    if worker_rank == 0:
        # 头节点负责启动 ray head
        head_addr = start_ray_head(master_port)
        # 监控其它节点加入
        wait_for_all_nodes(num_workers)
        # 当全部节点加入后，提交任务
        submit_job_and_monitor(args.launch)
    else:
        # 其它节点等待 head_address 文件生成，然后加入 ray 集群
        head_addr = wait_for_head_address()
        join_ray_cluster(head_addr)
        # 任务执行后，创建自身的标识文件
        signal_node_join(worker_rank)
        # 等待任务结束才退出
        wait_for_job_done()

if __name__ == '__main__':
    main()