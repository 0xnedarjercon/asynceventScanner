import json
from dotenv import load_dotenv
import os
import yaml
dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv()
folderPath = os.getenv("FOLDER_PATH")
configPath = f"{dir}/settings/{folderPath}/"
abiPath = f"{dir}/settings/ABIs/"
fileSettings = {}
scanSettings = {}
rpcSettings = {}
web3Settings = {}


def overrideSettings(cfg, rpcSetting):
    for key, value in cfg["RPCOVERRIDE"].items():
        if value != "":
            rpcSetting[key] = value


def loadConfig(config):
    if isinstance(config, dict):
        cfg = config

    else:
        with open(config, "r") as config_file:
            cfg = yaml.safe_load(config_file)
    fileSettings = cfg["FILESETTINGS"]
    scanSettings = cfg["SCANSETTINGS"]
    rpcSettings = cfg["RPCSETTINGS"]
    web3Settings = cfg["W3SETTINGS"]

    for rpcSetting in rpcSettings:
        overrideSettings(cfg, rpcSetting)
    return fileSettings, scanSettings, rpcSettings, web3Settings


def refreshLogs():
    print("removing old logfiles...")
    if not os.path.exists(f"{dir}/settings/{folderPath}/logs/"):
        os.makedirs(f"{dir}/settings/{folderPath}/logs/")
    for item_name in os.listdir(f"{dir}/settings/{folderPath}/logs/"):
        item_path = os.path.join(f"{dir}/settings/{folderPath}/logs", item_name)
        if os.path.isfile(item_path):
            os.remove(item_path)
            print(f"Deleted file: {item_path}")


def getConfig():
    return fileSettings, scanSettings, rpcSettings, web3Settings


cfg = loadConfig(configPath + "config.yml")
refreshLogs()
