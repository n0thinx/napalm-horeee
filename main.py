import csv
import os
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from napalm import get_network_driver
from ntc_templates.parse import parse_output

from rich.console import Console
from rich.progress import Progress
from rich.table import Table


# ================= CONFIG ================= #

MAX_WORKERS = 10

SKIP_CMDS = (
    "show tech",
    "show tech-support",
    "show support",
)

DRIVER_MAP = {
    "cisco_ios": "ios",
    "cisco_iosxe": "ios",
    "cisco_iosxr": "iosxr",
    "cisco_nxos": "nxos",
    "huawei_vrp": "huawei_vrp",
    "huawei_ce": "huawei_vrp",
    "huawei_yunshan": "huawei_vrp",
    "aruba_aoscx": "aoscx",
}

TEXTFSM_PLATFORM_MAP = {
    "cisco_ios": "cisco_ios",
    "cisco_iosxe": "cisco_ios",
    "cisco_iosxr": "cisco_xr",
    "cisco_nxos": "cisco_nxos",
    "huawei_vrp": "huawei",
    "huawei_ce": "huawei",
    "huawei_yunshan": "huawei",
    "aruba_aoscx": "aruba_aoscx",
}

console = Console()


# ================= UTIL ================= #

def should_skip(cmd: str) -> bool:
    return cmd.lower().startswith(SKIP_CMDS)


def load_devices(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def load_commands(path):
    with open(path) as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#")
        ]


def parse_with_textfsm(platform, command, output):
    tfsm_platform = TEXTFSM_PLATFORM_MAP.get(platform)
    if not tfsm_platform:
        return None

    try:
        return parse_output(
            platform=tfsm_platform,
            command=command,
            data=output,
        )
    except Exception:
        return None


def save_outputs(hostname, platform, raw, parsed):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    os.makedirs("outputs/raw", exist_ok=True)
    os.makedirs("outputs/parsed", exist_ok=True)

    raw_file = f"outputs/raw/{hostname}_{platform}_{ts}.txt"
    parsed_file = f"outputs/parsed/{hostname}_{platform}_{ts}.json"

    with open(raw_file, "w") as f:
        for cmd, out in raw.items():
            f.write("=" * 80 + "\n")
            f.write(f"COMMAND: {cmd}\n")
            f.write("=" * 80 + "\n")
            f.write(out + "\n\n")

    if parsed:
        with open(parsed_file, "w") as f:
            json.dump(parsed, f, indent=2)

    return raw_file, parsed_file if parsed else None


# ================= WORKER ================= #

def process_device(device, commands):
    hostname = device["hostname"]
    platform = device["platform"]

    if platform not in DRIVER_MAP:
        return hostname, False, "Unsupported platform"

    driver = get_network_driver(DRIVER_MAP[platform])

    conn = driver(
        hostname=device["ip"],
        username=device["username"],
        password=device["password"],
        optional_args={"timeout": 120}
    )

    raw_results = {}
    parsed_results = {}

    try:
        conn.open()

        for cmd in commands:
            if should_skip(cmd):
                console.log(
                    f"[yellow]SKIP[/yellow] {hostname} | {cmd} "
                    "(unsupported in Napalm)"
                )
                continue

            output = conn.cli([cmd])[cmd]
            raw_results[cmd] = output

            parsed = parse_with_textfsm(platform, cmd, output)
            if parsed:
                parsed_results[cmd] = parsed

        return hostname, True, (platform, raw_results, parsed_results)

    except Exception as e:
        return hostname, False, str(e)

    finally:
        try:
            conn.close()
        except Exception:
            pass


# ================= MAIN ================= #

def main():
    devices = load_devices("devices.csv")
    commands = load_commands("commands.txt")

    results = []

    with Progress(console=console) as progress:
        task = progress.add_task(
            "[cyan]Processing devices...", total=len(devices)
        )

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_device, d, commands): d
                for d in devices
            }

            for future in as_completed(futures):
                hostname, success, result = future.result()
                progress.advance(task)

                if success:
                    platform, raw, parsed = result
                    save_outputs(hostname, platform, raw, parsed)
                    results.append((hostname, "OK"))
                else:
                    results.append((hostname, f"FAIL: {result}"))

    table = Table(title="Execution Summary")
    table.add_column("Device", style="bold")
    table.add_column("Status")

    for host, status in results:
        color = "green" if status == "OK" else "red"
        table.add_row(host, f"[{color}]{status}[/{color}]")

    console.print(table)


if __name__ == "__main__":
    main()
