#TODO 
import sys
print(sys.executable)
import collections

from web3 import Web3
import time
import json
from web3._utils.events import get_event_data
from tqdm import tqdm
import os
from logger import Logger
from configLoader import loadConfig
from rpc import RPC
directory = os.path.dirname(os.path.abspath(__file__))
from fileHandler import FileHandler
from dotenv import load_dotenv
import asyncio
from utils import getW3
# from Verifier import Verifier

def processEvents(event):
    eventName = event["name"]
    inputTypes = [input_abi["type"] for input_abi in event["inputs"]]
    eventSig = '0x'+Web3.keccak(text=f"{eventName}({','.join(inputTypes)})").hex()
    topicCount = sum(1 for inp in event["inputs"] if inp["indexed"]) + 1
    return eventSig, topicCount


def getEventParameters(param):
    if "event" in param:
        event = str(param["event"]) + " " + str(param["logIndex"])
    else:
        event = "unkown " + str(param["logIndex"])

    return (
        param["blockNumber"],
        param["transactionHash"].hex(),
        param["address"],
        event,
    )


class EventScanner(Logger):

    def __init__(
        self,
        showprogress=True,
    ):
        load_dotenv()
        folderPath = os.getenv("FOLDER_PATH")
        configPath = f"{os.path.dirname(os.path.abspath(__file__))}/settings/{folderPath}/"
        fileSettings, scanSettings, rpcSettings,web3Settings = loadConfig(configPath + "/config.json")
        self.live = False
        self.configPath = configPath
        Logger.setProcessName("scanner")
        self.loadSettings(scanSettings)
        super().__init__("es")
        self.w3 = None
        self.fileHandler = FileHandler(fileSettings, configPath, "fh")
        self.showProgress = showprogress
        self.rpcSettings = rpcSettings
        self.rpcs = []
        
    #--------------------------- Setup functions ------------------------------------------------
    async def initRpcs(self ):
        for apiUrl, rpcSetting in self.rpcSettings.items():
                self.rpcs.append(await RPC(apiUrl, rpcSetting).init())
                if self.w3 is None:
                    self.w3, self.websocket = await getW3(apiUrl)
        return self

        # stores relevent settings from config file
    def loadSettings(self, scanSettings):
        self.scanMode = scanSettings["MODE"]
        self.events = scanSettings["EVENTS"]
        self.loadAbis()
        self.processContracts(scanSettings["CONTRACTS"])
        self.processAbis()
        self.startBlock = scanSettings["STARTBLOCK"]
        self.endBlock = scanSettings["ENDBLOCK"]
        self.liveThreshold = scanSettings["LIVETHRESHOLD"]
    # processes the passed contracts and stores the event signiatures
    def processContracts(self, contracts):
        self.contracts = {}
        self.abiLookups = {}
        for contract, abiFile in contracts.items():
            checksumAddress = Web3.to_checksum_address(contract)
            self.contracts[checksumAddress] = {}
            for entry in self.abis[abiFile]:
                if entry["type"] == "event":
                    eventSig, topicCount = processEvents(entry)
                    self.contracts[checksumAddress][eventSig] = entry

    def processAbis(self):
        for abiFile, abi in self.abis.items():
            for entry in abi:
                if entry["type"] == "event" and entry["name"] in self.events:
                    eventSig, topicCount = processEvents(entry)
                    self.abiLookups[eventSig] = {topicCount: entry}



    def loadAbis(self):
        self.abis = {}
        files = os.listdir(self.configPath + "ABIs/")
        for file in files:
            if file.endswith(".json"):
                self.abis[file[:-5]] = json.load(open(self.configPath + "ABIs/" + file))
                
                
                
    #---------------------------- event processing -----------------------------------
    # breaks down event data into a usable dict
    def getEventData(self, events):
        decodedEvents = {}
        for param in events:
            blockNumber, txHash, address, index = getEventParameters(param)
            address = self.w3.to_checksum_address(address)
            if blockNumber not in decodedEvents:
                decodedEvents[blockNumber] = {}
            if txHash not in decodedEvents[blockNumber]:
                decodedEvents[blockNumber][txHash] = {}
            if address not in decodedEvents[blockNumber][txHash]:
                decodedEvents[blockNumber][txHash][address] = {}
            decodedEvents[blockNumber][txHash][address][index] = {}
            for eventName, eventParam in param["args"].items():
                decodedEvents[blockNumber][txHash][address][index][
                    eventName
                ] = eventParam
        return decodedEvents

    # decodes events based on scansettings
    def decodeEvents(self, events):
        decodedEvents = []
        if self.scanMode == "ANYEVENT":
            for event in events:
                signiature = '0x'+event["topics"][0].hex()
                eventAbi = self.contracts[event["address"]].get(signiature)
                if eventAbi is not None:
                    evt = get_event_data(
                        self.w3.codec,
                        eventAbi,
                        event,
                    )
                    decodedEvents.append(evt)
        elif self.scanMode == "ANYCONTRACT":
            for event in events:
                signiature = '0x'+event["topics"][0].hex()
                eventLookup = self.abiLookups.get(signiature)
                if eventLookup is not None:
                    numTopics = len(event["topics"])
                    if numTopics in eventLookup:
                        evt = get_event_data(
                            self.w3.codec,
                            self.abiLookups[signiature][numTopics],
                            event,
                        )
                        decodedEvents.append(evt)
        return self.getEventData(decodedEvents)



    # returns the last block stored by the filehandler
    def getLastStoredBlock(self):
        return self.fileHandler.latest

    # saves the currently available data
    def saveState(self):
        self.fileHandler.save()

    # graceful exit on keyboard interrupt
    def interrupt(self):
        self.logInfo("keyboard interrupt")
        self.saveState()

#--------------------------- scan functions -----------------------------

    # generates a filter for get_logs based on what is configured
    def getFilter(self, start, end):
        if self.scanMode == "ANYEVENT":
            return {
                "fromBlock": start,
                "toBlock": end,
                "topics": [],
                "address": list(self.contracts.keys()),
            }
        elif self.scanMode == "ANYCONTRACT":
            return {
                "fromBlock": start,
                "toBlock": end,
                "topics": [list(self.abiLookups.keys())],
                "address": [],
            }
    def start_get_logs(self, remaining, filterParams, results = None, jobLock =None, rpcs = None):
        if results is None:
            results = asyncio.Queue()
        if jobLock is None:
            jobLock = asyncio.Lock()
        if rpcs is None:
            rpcs=self.rpcs
        found = False
        usedRpcs=[]
        if remaining[-1] == 'latest':
            rpcModes = ['live_logs', 'live_blocks']
        elif isinstance(remaining[-1], int):
            rpcModes = ['get_logs']
        else:
            raise Exception('unkown search type')
        for rpc in rpcs:
            if any(mode in rpc.modes for mode in rpcModes):
                task = asyncio.create_task(rpc.get_logs(remaining, filterParams, results, jobLock))
                found = True
                usedRpcs.append(rpc)
        assert found, 'no rpcs support get_logs, add this to MODES in config'
        return results, remaining, jobLock, usedRpcs
    
    async def scanFixedEnd(self, start, endBlock):
        filterParams = self.getFilter(start, endBlock)
        remaining  = [filterParams['fromBlock'], filterParams['toBlock']]
        startTime = time.time()
        self.logInfo(
            f"starting fixed scan at {time.asctime(time.localtime(startTime))}, scanning {start} to {endBlock}",
            True,
        )
        results, remaining, jobLock, usedRpcs= self.start_get_logs(remaining, filterParams)
        blocksToScan = filterParams['toBlock']-filterParams['fromBlock']
        with tqdm(total=blocksToScan) as progress_bar:
            while self.fileHandler.latest < endBlock:
                resultsTmp = []
                while not results.empty():
                    resultsTmp.append(await results.get())
                    await self.storeResults(resultsTmp, decoded = False)
                    self.updateProgress(progress_bar, startTime, start, blocksToScan, resultsTmp[-1][-1]-resultsTmp[0][0])
                await asyncio.sleep(0.1)
        for rpc in usedRpcs:
            rpc.running = False    
        self.logInfo(
            f"Completed: Scanned blocks {start}-{self.endBlock} in {time.time()-startTime}s from {time.asctime(time.localtime(startTime))} to {time.asctime(time.localtime(time.time()))}",
            True,
        )
        self.logInfo(
            f"average {(endBlock-start)/(time.time()-startTime)} blocks per second",
            True,
        )
        await self.fileHandler.asyncSave()
        return results
        # updates progress bar for fixed scan

    def updateProgress(
        self,
        progress_bar,
        startTime,
        start,
        totalBlocks,
        numBlocks,
    ):
        elapsedTime = time.time() - startTime
        progress = self.fileHandler.latest - start
        avg = progress / elapsedTime + 0.1
        remainingTime = (totalBlocks - progress) / avg
        eta = f"{int(remainingTime)}s ({time.asctime(time.localtime(time.time()+remainingTime))})"
        progress_bar.set_description(
            f"Stored up to: {self.fileHandler.latest} ETA:{eta} avg: {avg} blocks/s {progress}/{totalBlocks}"
        )
        progress_bar.update(numBlocks)
        
    async def getResults(self, results, decode = True):
            tmp = []
            while not results.empty():
                res = await results.get()
                if decode:
                    decoded = self.decodeEvents(res[1])
                    if len(decoded)>0:
                        tmp.append([res[0], decoded, res[1]])
            return tmp  
    # stores get_logs results into the file handler scanResults is a list of listProxy jobs
    async def storeResults(
        self, scanResults, forceSave=False, decoded=False, guarunteedContinuous =False
    ):
        storedData = []
        if len(scanResults) > 0:
            for scanResult in scanResults:
                if not decoded:
                    decodedEvents = self.decodeEvents(scanResult[1])
                else:
                    decodedEvents = scanResult[1]
                self.logInfo(
                    f"events found: {len(decodedEvents)}  {list(decodedEvents.keys())[0]} {list(decodedEvents.keys())[-1]}"
                )
                blockNums = list(decodedEvents.keys())
                i = 0
                blockNum = blockNums[i]
                while blockNum <= self.fileHandler.latest and i < len(blockNums):
                    blockNum = blockNums[i]
                    if len(decodedEvents) == 0:
                        return
                    del decodedEvents[blockNum]
                    i += 1
                    
                if isinstance(scanResult[2], int):
                    end = scanResult[2]
                else:
                    end = list(decodedEvents.keys())[-1]

                storedData.append(
                    [
                        max(self.fileHandler.latest, scanResult[0]),
                        decodedEvents,
                        end,
                    ]
                )
            if forceSave:
                self.fileHandler.save()
            return await self.fileHandler.process(storedData, guarunteedContinuous=guarunteedContinuous)
        else:
            return 0

    # scans from a specified block, then transitions to live mode, polling for latest blocks
    async def scanBlocks(self, start=None, end=None):
        results = []
        if start is None:
            start = self.startBlock
        if start == 'current':
            start = await self.w3.eth.get_block_number()-120
        if end is None:
            end = self.endBlock
        if end == "current":
            end = await self.w3.eth.get_block_number()
        if isinstance(end, int):
            results+=self.scanMissingBlocks(start, end)
            return results
        else:
            self.fileHandler.setup(start)
            _end = await self.w3.eth.get_block_number()
            self.logInfo(f"latest block {_end}, latest stored {end}")
            while _end - self.fileHandler.latest > self.liveThreshold:
                _end = await self.w3.eth.get_block_number()
                
                results.append(await self.scanMissingBlocks(start, _end))
            self.logInfo(
                f"------------------going into live mode, current block: {_end} latest: {self.fileHandler.latest}------------------",
                True,
            )
            self.fileHandler.setup(start)
            filterParams = self.getFilter(self.fileHandler.latest + 1, 'latest')
            remaining = [filterParams['fromBlock'], 'latest' ]
            results, remaining, jobLock, usedRpcs = self.start_get_logs(remaining, filterParams)
            self.live = True
            return results, remaining, jobLock, usedRpcs

    # triggers fixed scan for any gaps in the stored data for the range provided
    async def scanMissingBlocks(self, start, end, callback=None):
        results = []
        missingBlocks = self.fileHandler.checkMissing(start, end)
        self.logInfo(f"missing blocks: {missingBlocks}")
        for missingBlock in missingBlocks:
            self.fileHandler.setup(missingBlock[0])
            results.append(await self.scanFixedEnd(missingBlock[0], missingBlock[1]))
        self.fileHandler.setup(end)
        return results

    async def getEvents(self, start, end, results={}):
        await self.scanMissingBlocks(start, end)
        self.fileHandler.getEvents(start, end, results)
        return results

    def findGasRpc(self, rpcs):
        for rpc in rpcs:
            if rpc.websocket and ('live_logs' in rpc.modes or 'live_blocks' in rpc.modes):
                return rpc
            
def readConfig(configPath):
    with open(configPath + "config.json") as f:
        cfg = json.load(f)
    return cfg["RPCSETTINGS"], cfg["SCANSETTINGS"], cfg["FILESETTINGS"]
import copy
async def main():
    from dotenv import load_dotenv

    es = await EventScanner().initRpcs()
    results, remaining, jobLock, usedRpcs = await es.scanBlocks('current', 'latest')
    gasRpc = es.findGasRpc(usedRpcs)
    while es.live:
        receivedResults = []
        receivedResults = await es.getResults(results)
        if (len(receivedResults)) > 0:
            # do stuff with the data here
            print(f'current gas price {gasRpc.gasPrice}')
            print(len(receivedResults))
            await es.storeResults(receivedResults, guarunteedContinuous = True, decoded =True)
            print(es.fileHandler.latest)
        await asyncio.sleep(1)
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

# PersistentConnection(self.w3.socket)
# <multiprocessingUtils.SharedResult object at 0x7f5c60b51120>
# <multiprocessingUtils.SharedResult object at 0x7f5c60b51120>
# AttributeDict({'address': '0xeF4B763385838FfFc708000f884026B8c0434275', 'blockHash': HexBytes('0x0004c98e00000e811de5037ee0593869a58fbd025b3499d96fe7643cf2c35f46'), 'blockNumber': 95948274, 'data': HexBytes('0x0000000000000000000000000000000000000000000000000000000000000000'), 'logIndex': 0, 'removed': False, 'topics': [HexBytes('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'), HexBytes('0x0000000000000000000000000000000000000000000000000000000000000000'), HexBytes('0x000000000000000000000000a9768b6449b0b1a1ba019efafc91c7c2faa47400')], 'transactionHash': HexBytes('0x3dfb31eecfcb1a93a88c4bdf24ffee373d4dbbb1c540b920976d6cafe4ebbb06'), 'transactionIndex': 0})