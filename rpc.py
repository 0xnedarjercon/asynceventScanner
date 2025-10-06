from web3._utils.events import get_event_data
import time
import re
from logger import Logger
import math
import asyncio
import traceback
import copy
from utils import blocks
from utils import getW3
import websockets
import web3




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

    

class RPC(Logger):

    def __init__(self, apiUrl, rpcSettings):
        self.apiUrl = apiUrl
        super().__init__(apiUrl[8:])
        self.apiUrl = apiUrl
        self.maxChunkSize = rpcSettings["MAXCHUNKSIZE"]
        self.currentChunkSize = rpcSettings["STARTCHUNKSIZE"]
        self.eventsTarget = rpcSettings["EVENTSTARGET"]
        self.modes = rpcSettings["MODES"]
        self.lastTime = 0
        self.gasPrice = None
        self.lastTimestamp = 0
        self.lastRxBlock = 0
        self.currentJobs = []
        self.running = False
        self.failCount = 0
        self.currentSubScriptions = []

    async def init(self):
        self.w3, self.websocket = await getW3(self.apiUrl)
        return self



    async def sendTx(self, tx):
        try:
            tx_hash = await self.w3.eth.send_raw_transaction(tx.raw_transaction)
            result = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
            self.logInfo(f'got success result for {tx} with result {result}')
        except:
            self.logInfo(f'got fail result for {tx} with result {result}')
            
    async def get_logs(self, remaining, filter, results,  jobLock):
        
        self.running = True
        self.filter = filter = filter.copy()
        async with jobLock:
                self.live = len(remaining)>0 and (remaining[1] == 'latest')
        if self.live:
            if 'live_logs' in self.modes:
                await self.liveScanLogs(remaining, filter, results,  jobLock)
            elif 'live_blocks' in self.modes:
                await self.liveScanBlocks(remaining, filter, results, jobLock)
        else:
            finished = False
            while self.running:
                await self.doScan(remaining, filter, results,  jobLock)
                if not self.running:
                    return
                await asyncio.sleep(0) 

        self.running = False
        self.logInfo('stopped scanning')
            
    
    async def liveScanLogs(self, remaining, filter, results, jobLock):
        self.logInfo('starting logs livescan')
        subscriptionId = await self.w3.eth.subscribe("newHeads")
        fails = 0
        while self.live:
            try:
                start = time.time()
                result, lastBlock = await self.checkSocket(jobLock, remaining)
                if lastBlock < self.lastRxBlock:
                    lastRxBlock= await self.processNewEvents(filter, remaining, jobLock, results, self.lastRxBlock)
                    fails = 0
                    end = time.time()
                    self.logInfo(f'logs time to process results: {end-start}')
                else:
                    self.logInfo(f'not new block, skipping')
            except (websockets.exceptions.ConnectionClosedError, asyncio.TimeoutError)as e:
                fails += 0
                self.logInfo(f'error {e}, restarting w3 {fails}/5')
                await asyncio.sleep(3)
                self.w3, self.websocket = await getW3(self.apiUrl)
                subscriptionId = await self.w3.eth.subscribe("newHeads")
                if fails >5:
                    self.live = False
                    self.running = False
                    self.logWarn(f'too many errors in rpc, shutting down')
                    return
            except web3.exceptions.Web3RPCError:
                try:
                    lastRxBlock= await self.processNewEvents(filter, remaining, jobLock, results, self.lastRxBlock)
                    fails = 0
                    end = time.time()
                    self.logInfo(f'logs time to process results: {end-start}')
                except Exception as e:
                    self.logWarn('retry failed')
            except Exception as e:
                self.logWarn(f'unhandled error in livescan {e}')
                await asyncio.sleep(1)

    async def liveScanBlocks(self, remaining, filter, results, jobLock):
        self.logInfo('starting blocks livescan')
        subscriptionId = await self.w3.eth.subscribe("newHeads")
        fails = 0
        while self.live:
            try:
                start = time.time()
                _, lastStoredBlock = await self.checkSocket(jobLock, remaining)
                if lastStoredBlock < self.lastRxBlock:
                    lastRxBlock= await self.processNewBlocks(filter, remaining, jobLock, results, self.lastRxBlock)
                    fails = 0
                    end = time.time()
                    self.logInfo(f'logs time to process results: {end-start}')
                else:
                    self.logInfo(f'not new block, skipping')
            except (websockets.exceptions.ConnectionClosedError, asyncio.TimeoutError )as e:
                fails += 1
                self.logInfo(f'error {e}, restarting w3 {fails}/3')
                await asyncio.sleep(3)
                self.w3, self.websocket = await getW3(self.apiUrl)
                subscriptionId = await self.w3.eth.subscribe("newHeads")
                if fails >5:
                    self.live = False
                    self.running = False
                    self.logWarn(f'too many errors in rpc, shutting down')
                    return
            except Exception as e:
                self.logWarn(f'unhandled error in livescan {e}')
                
    async def processNewBlocks(self, filter, remaining, jobLock, results, lastRxBlock):
        start = time.time()
        block = await self.w3.eth.get_block(lastRxBlock)
        events = []
        for txHash in block.transactions:
            receipt = (await self.w3.eth.get_transaction_receipt(txHash))
            events += receipt.logs
        end = time.time()
        self.logInfo(f'got blocks events in {end-start}')
        async with jobLock:
            remaining[0] = max(lastRxBlock, remaining[0])
            self.logInfo(f'remaining updated to {remaining}, {lastRxBlock}')
        filter = filter.copy()
        await results.put([lastRxBlock, events, lastRxBlock])
        return lastRxBlock
    
    async def checkSocket(self, jobLock, remaining):
        payload = await asyncio.wait_for(self.w3.socket._manager._get_next_message(), 10)
        result = payload['result']
        self.lastRxBlock = result['number']
        self.gasPrice = result.baseFeePerGas
        self.lastTimestamp = result.timestamp
        self.logInfo(f'message received {time.time()} blockTime{self.lastTimestamp} age: {self.lastTimestamp-time.time()}')
        async with jobLock:
            lastStoredBlock = remaining[0]
        self.logInfo(f'new block {self.lastRxBlock}')
        return result, lastStoredBlock
    
            #takes a eth.get_logs job based on its scan parameters and the remaining range to be scanned
    async def doScan(self, remaining, filter, results, jobLock):
        if len(self.currentJobs)==0:
            async with jobLock:
                if len(remaining)>0:
                    startBlock = remaining[-2]
                    if remaining[-2]+self.currentChunkSize >= remaining[-1]:
                        endBlock = remaining[-1]
                        remaining.pop(-1)
                        remaining.pop(-1)
                    else:
                        endBlock = startBlock+self.currentChunkSize
                        remaining[-2] = endBlock + 1
                else:
                    return
            filter['fromBlock'] = startBlock
            filter['toBlock'] = endBlock
            self.logInfo(f'took job {blocks(filter)}')
            self.currentJobs.append(filter)
        filter = self.currentJobs.pop(0)
        try:
            result = await self.w3.eth.get_logs(filter)
            if len(result)>0:
                await results.put([filter['fromBlock'] ,result, filter['toBlock']])
                self.logInfo(f'added results {filter}')
            self.logInfo(f'successful job')
            self.failCount = 0
            self.throttle(result, filter['toBlock']-filter['fromBlock'])     
        except Exception as error:
                self.logInfo(f'error with job {error}')
                if self.failCount >20:
                    self.logWarn(f'too many failures, shutting down rpc...')
                    self.currentJobs.append(filter)
                    _min = min(x['fromBlock'] for x in self.currentJobs+[filter])
                    _max = max(x['toBlock'] for x in self.currentJobs+[filter])
                    filter['fromBlock'] = _min
                    filter['toBlock'] = _max
                    remaining.append(_min)
                    remaining.append(_max)
                    self.running = False
                    return
                else:
                    self.handleError([filter, error])
                    
    async def processNewEvents(self, filter, remaining, jobLock, results, blockNum = None):
        async with jobLock:
            remaining[0] = max(self.lastRxBlock, remaining[0])
            self.logInfo(f'remaining updated to {remaining}, {self.lastRxBlock}')
            filter['fromBlock'] = remaining[0]
        result = await self.w3.eth.get_logs(filter)
        if len(result)>0:
            self.lastRxBlock = result[-1]['blockNumber']
            await results.put([filter['fromBlock'] ,result, self.lastRxBlock])
            self.logInfo(f'livescan update to {self.lastRxBlock}')
        self.logInfo(f'job success {len(result)} events')


            
    def throttle(self, events, blockRange):
        if len(events) > 0:
            ratio = self.eventsTarget / (len(events))
            targetBlocks = math.ceil(ratio * blockRange)
            self.currentChunkSize = min(targetBlocks, self.maxChunkSize)
            self.currentChunkSize = max(self.currentChunkSize, 1)
            self.logInfo(
                    f"processed events: {len(events)}, ({blockRange}) blocks, throttled to {self.currentChunkSize}"
                )
    #-----------------------rpc error handling------------------------------------
    
    def handleRangeTooLargeError(self, failingJob):
        try:
            for word in failingJob[-1].args[0]["message"].split(' '):
                word = (word.replace('k', '000'))
                if word[0].isdigit():
                    maxBlock = int(word)
                    if maxBlock >0:
                        if maxBlock > self.currentChunkSize:
                            raise Exception
                        else:
                            filter = failingJob[0]
                            self.maxChunk = maxBlock
                            self.currentChunkSize = maxBlock                                
        except Exception as error:
                    self.maxChunk = int(self.currentChunkSize * 0.95)
                    self.currentChunkSize = min(self.maxChunk, self.currentChunkSize )
        self.splitJob(
                    math.ceil((filter['toBlock']-filter['fromBlock']) / maxBlock), failingJob
                )
        self.logInfo(f"blockrange too wide, reduced max to {self.currentChunkSize}")
    def handleInvalidParamsError(self, failingJob):
        if "Try with this block range" in failingJob[-1].args[0]["message"]:
            match = re.search(
                r"\[0x([0-9a-fA-F]+), 0x([0-9a-fA-F]+)\]", failingJob[-1].args[0]["data"]
            )
            if match:
                start_hex, end_hex = match.groups()
                suggestedLength = int(end_hex, 16) - int(start_hex, 16)
                self.logInfo(
                    f"too many events, suggested range {suggestedLength}"
                )
                self.splitJob(
                    math.ceil(self.currentChunkSize / suggestedLength), failingJob
                )
            else:
                self.logWarn(
                    f"unable to find suggested block range, splitting jobs"
                )
                self.splitJob(2, failingJob)
    def handleResponseSizeExceeded(self, failingJob):
        self.eventsTarget = self.eventsTarget*0.95
        self.splitJob(2, failingJob)
        
    def handleError(self, failingJob):
            e = failingJob[-1]
            if type(e) == ValueError:
                if any(phrase in e.args[0]["message"] for phrase in ["block range is too wide",'range is too large','eth_getLogs is limited to']):
                    self.handleRangeTooLargeError(failingJob)
                elif e.args[0]["message"] == "invalid params" or "response size should not greater than" in e.args[0]["message"]:
                    self.handleInvalidParamsError(failingJob)
                elif "response size exceed" in e.args[0]["message"]:
                    self.handleResponseSizeExceeded(failingJob)
                    
                elif e.args[0]["message"] == "rate limit exceeded":
                    self.logInfo(f"rate limited trying again")     
                else:
                    self.logWarn(
                        f"unhandled error {type(e), e}, {traceback.format_exc()} splitting jobs",
                        True,
                    )
                    self.splitJob(2, failingJob)
                    self.failCount += 1
            elif type(e) == asyncio.exceptions.TimeoutError:
                
                self.logInfo(f"timeout error, splitting jobs")
                self.splitJob(2, failingJob)
                self.failCount += 1
            elif type(e) == KeyboardInterrupt:
                pass
            else:
                self.logWarn(
                    f"unhandled error {type(e), e},{traceback.format_exc()}  splitting jobs",
                    True,
                )
                self.splitJob(2, failingJob)
                self.failCount += 1

    # reduces the scan range by a specified factor, 
    # removes all jobs in the jobmanager, splits them based on the new scan range and adds them back
    def splitJob(self,numJobs, failingJob ,chunkSize =None, reduceChunkSize=True):
        self.logInfo(f'spitting jobs from {blocks(failingJob[0])}')
        if type(chunkSize) !=(int):
            chunkSize = math.ceil((failingJob[0]['toBlock'] - failingJob[0]['fromBlock']) / numJobs)
        if reduceChunkSize:
            self.currentChunkSize = max(chunkSize, 1)
        filter = failingJob[0]
        currentBlock = filter['fromBlock'] 
        while currentBlock <= failingJob[0]['toBlock']:
            _filter = copy.deepcopy(filter)
            _filter['fromBlock'] = currentBlock
            _filter['toBlock'] = min(currentBlock+chunkSize, filter['toBlock'])
            self.currentJobs.append(_filter)
            currentBlock = _filter['toBlock'] + 1
            self.logInfo(f'added job {blocks(_filter)}')


