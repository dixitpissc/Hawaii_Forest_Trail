# main.py (Optimized + Accurate CPU Usage)

import psutil
import time
import datetime
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich import box

console = Console()


def format_bytes(b):
    return f"{b / (1024 * 1024):.2f} MB"


def format_speed(bps):
    return f"{bps / (1024 * 1024):.2f} MB/s"


def format_time(seconds):
    return str(datetime.timedelta(seconds=int(seconds)))


def get_python_processes():
    return [
        proc for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time'])
        if proc.info['name'] and 'python' in proc.info['name'].lower()
        and proc.info['cmdline'] and any('.py' in arg for arg in proc.info['cmdline'])
    ]

prev_net = {}
prev_disk = {}
prev_system_net = psutil.net_io_counters()
proc_cpu_snapshots = {}


def create_monitor_table(processes):
    global prev_net, prev_disk, prev_system_net, proc_cpu_snapshots

    current_net = psutil.net_io_counters()
    net_recv_speed = format_speed(current_net.bytes_recv - prev_system_net.bytes_recv)
    net_sent_speed = format_speed(current_net.bytes_sent - prev_system_net.bytes_sent)
    prev_system_net = current_net

    table = Table(title=f"::=>> Processx Monitor (CPU / RAM / NET / DISK)  \U0001F4F6 Down: {net_recv_speed} | Up: {net_sent_speed}", box=box.SIMPLE, expand=True)
    table.add_column("PID", justify="right", no_wrap=True)
    table.add_column("Script", justify="left")
    table.add_column("CPU %", justify="right")
    table.add_column("Memory", justify="right")
    table.add_column("Threads", justify="right")
    table.add_column("Uptime", justify="right")
    table.add_column("Net R", justify="right")
    table.add_column("Net W", justify="right")
    table.add_column("Disk R", justify="right")
    table.add_column("Disk W", justify="right")

    for proc in processes:
        try:
            pid = proc.pid
            cmdline = proc.cmdline()
            script_name = next((arg for arg in cmdline if arg.endswith(".py")), "N/A")
            mem = format_bytes(proc.memory_info().rss)
            threads = str(proc.num_threads())
            uptime = format_time(time.time() - proc.create_time())

            # Accurate CPU usage snapshot
            current_cpu_time = sum(proc.cpu_times()[:2])  # user + system
            prev_cpu_time, prev_timestamp = proc_cpu_snapshots.get(pid, (0.0, time.time() - 1))
            now = time.time()
            elapsed_time = now - prev_timestamp
            cpu_percent = ((current_cpu_time - prev_cpu_time) / elapsed_time) * 100 / psutil.cpu_count()
            proc_cpu_snapshots[pid] = (current_cpu_time, now)

            # Network I/O per process
            net_io = proc.io_counters() if proc.is_running() else None
            prev_net_io = prev_net.get(pid)
            net_read = net_write = "0.00 MB/s"
            if net_io and prev_net_io:
                net_read = format_speed(net_io.read_bytes - prev_net_io.read_bytes)
                net_write = format_speed(net_io.write_bytes - prev_net_io.write_bytes)
            prev_net[pid] = net_io

            # Disk I/O
            disk_io = proc.io_counters() if proc.is_running() else None
            prev_disk_io = prev_disk.get(pid)
            disk_read = disk_write = "0.00 MB/s"
            if disk_io and prev_disk_io:
                disk_read = format_speed(disk_io.read_bytes - prev_disk_io.read_bytes)
                disk_write = format_speed(disk_io.write_bytes - prev_disk_io.write_bytes)
            prev_disk[pid] = disk_io

            table.add_row(str(pid), script_name, f"{cpu_percent:.1f}", mem, threads, uptime, net_read, net_write, disk_read, disk_write)
        except (psutil.NoSuchProcess, psutil.AccessDenied, IndexError):
            continue

    return table


def monitor():
    console.clear()
    console.print("\n\U0001F50D Monitoring Python scripts...\n", style="bold cyan")
    time.sleep(1)
    with Live(console=console, refresh_per_second=0.5):
        while True:
            processes = get_python_processes()
            table = create_monitor_table(processes)
            console.print(table)
            time.sleep(2)


if __name__ == "__main__":
    try:
        monitor()
    except KeyboardInterrupt:
        console.print("\n\U0001F6A9 Monitoring stopped.", style="bold red")
