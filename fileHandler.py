import os
import json
import time
from logger import Logger
import aiofiles
import aiosqlite
from SqlGenerators import sql_create_table, build_upsert_sql
import sqlite3

# currently assumes all files stored are sequential

class DBFileHandler(Logger):
    def __init__(self, fileSettings,
        configPath,name):
        super().__init__('fh', fileSettings["LOGNAMES"])
        self.loadSettings(fileSettings['DBSETTINGS'])
        self.filePath = configPath + "data/"
        os.makedirs(self.filePath, exist_ok=True)
        self.name = name
        self.conflicts = list(self.primaryKey if self.primaryKey is not None else self.uniqueKeys)
        self.columns = list(self.tableFormat.keys())
        self.placeHolder = '?'
        self.metadataTableFormat = {'minBlock': 'BIGINT', 'maxBlock': 'BIGINT'}
        for i in range(len(self.conflicts)):
            if self.conflicts[i].strip().endswith(' DESC'):
                self.conflicts[i] = self.conflicts[i].strip()[:-5]  
        with sqlite3.connect(f"{self.filePath}{name}.db") as db:
            db.execute(sql_create_table(self.name, self.tableFormat,self.primaryKey))
            db.execute(sql_create_table('metadata', {'minBlock': 'BIGINT', 'maxBlock': 'BIGINT'},[] ))
            #TODO add indexed fields option
            db.commit()
            query = f'SELECT * FROM "metadata" order by "minBlock";'
            self.metadata =  db.execute(query).fetchall()
            self.shrinkMetadata(db)
            db.commit()
        
        self.con = aiosqlite.connect(f"{self.filePath}{self.name}.db")
        
    async def setup(self, startBlock):
        self.logInfo(f"setting up new scan")
        self.start = startBlock
        self.pending = []
        if self.con.ident is None:
            await self.con.__aenter__()
        if len(self.metadata) == 0:
            self.latest = startBlock -1
            return 
        else:
            self.latest = 0
            for item in self.metadata:
                
                if self.latest == 0:
                    #if first block start is after start then start from startBlock
                    if item[0] > startBlock+1 :
                        self.latest = startBlock -1
                        return self.latest
                    # otherwise if end of first block is after start, check continuity of next blocks
                    elif item[1] >startBlock +1:
                        self.latest = item[1]
                #if next range is not continuous with current one, latest is the end of the last range
                elif item[0]> self.latest+1:
                    break
                #otherwise keep counting forward
                elif item[1]>= startBlock-1:
                        self.latest = item[1]
                else:
                    assert False
        if self.latest == 0:
            self.latest = startBlock -1
            self.logInfo('no previous data found')
        return self.latest
    
    def checkMissing(self, start, end):
        missing = []
        i = 0
        started = False
        current = start
        for rangeStart, rangeEnd in self.metadata:
            if current < rangeStart:
                missing.append((current, rangeStart-1))
            current = max(current, rangeEnd+1)
            if current >end: 
                break
        if current <= end:
            missing.append((current, end-1))
        self.logInfo(f"missing block ranges: {missing}")
        return missing
    
    def shrinkMetadata(self, con):
        if len(self.metadata)<=1 :
            return
        newMetadata = []
        start = None
        end = None
        while len(self.metadata)>0:
            current = self.metadata.pop(0)
            curStart = current[0]
            curEnd = current[1]
            if start is None:
                start = curStart
                end = curEnd
            elif curStart <= end + 1:
                end = max(end, curEnd)
            else:
                newMetadata.append((start, end))
                start = curStart
                end = curEnd
        newMetadata.append((start, end))
        self.metadata = newMetadata
        try:
            con.execute('DELETE FROM "metadata";')
            upsert_sql = build_upsert_sql(self.metadataTableFormat.keys(), 'metadata')
            con.executemany(upsert_sql, self.metadata)
            con.commit()
        except Exception as e:
            print(f"Metadata shrink failed: {e}")
            self.con.rollback()


    def loadSettings(self, cfg):
        # self.uniqueKeys = cfg["UNIQUEKEYS"] 
        self.tableFormat = cfg["TABLEFORMAT"]
        self.primaryKey = cfg["PRIMARYKEYS"]
        self.indexedFields = cfg["INDEXEDFIELDS"]
        self.needsUnique = cfg["NEEDSUNIQUE"] 

    async def asyncSave(self, deleteOld=True, indent=None):
        pass

    #takes json events and formats them for insertion into db
    def prepareJsonEvents(self, data):
        rows = []
        for block_str, block_data in data.items():
            block_number = int(block_str)
            for tx_hash, tx_data in block_data.items():
                for contract_address, pool_events in tx_data.items():
                    for event_name, payload in pool_events.items():
                        try:
                            event_type, idx_str = event_name.rsplit(" ", 1)
                            event_index = int(idx_str)
                        except Exception:
                            event_type = event_name
                            event_index = None
                        row = [None]*len(self.columns)
                        for i in range(len(self.columns)):
                            k = self.columns[i]
                            if k == 'block_number':
                                row[i] = block_number
                                continue
                            elif k == 'tx_hash':
                                row[i] = tx_hash
                                continue
                            elif k == 'contract_address':
                                row[i] = contract_address
                                continue
                            elif k == 'event_type':
                                row[i] = event_type
                                continue
                            elif k == 'event_index':
                                row[i] = event_index
                                continue
                            elif k in payload:
                                row[i] = payload[k]
                                del payload[k]
                                continue
                            elif k == 'event_data':
                                row[i] = json.dumps(payload) if payload else "{}"
                        rows.append(row)
        return rows
    #add s to the db from tuple format
    #must be entire row, in the order of columns
    async def addTupleEvents(self, rows):
        upsert_sql = build_upsert_sql(self.columns, self.name,tuple(self.conflicts) )
        try:
            await self.con.executemany(upsert_sql, rows)
            await self.con.commit()
        except Exception as e:
            print(f"Insert failed: {e}")
            await self.con.rollback()

    #results array [fromBlock, events, toBlock]
    async def process(self, results, guarunteedContinuous =False):
        for result in results:
            start, data, end = result
            rows = self.prepareJsonEvents(data)
            if guarunteedContinuous:
                self.latest = max(end, self.latest)
            else:
                self.addToPending((start, end))
                self.logInfo(f"data added to pending {start} to {end}")
            try:
                upsert_sql = build_upsert_sql(self.columns, self.name,tuple(self.conflicts) )
                metadata_sql = build_upsert_sql(self.metadataTableFormat.keys(), 'metadata' )
                await self.con.executemany(upsert_sql, rows)
                await self.con.execute(metadata_sql, (start, end))
                self.metadata.append((start, end))
                await self.con.commit()
            except Exception as e:
                print(e)
                self.con.rollback()
        self.mergePending()
        
        
    def mergePending(self):
        while len(self.pending) > 0 and self.pending[0][0] <= self.latest + 1:
            self.latest = max(self.pending[0][1], self.latest)
            self.pending.pop(0)
        self.logInfo(f"waiting for: {self.latest+1}")


    def addToPending(self, element):
        position = 0
        while position < len(self.pending) and self.pending[position][0] < element[0]:
            position += 1
        self.pending.insert(position, element)
        self.logInfo(f"data added to pending {element[0]} to {element[1]}")
    
    async def fetch(self, query, params = None, columns = None ):
        self.ensureCon()            
        query = f'SELECT {columns if columns else '*'} FROM "{self.name}" WHERE {query}'
        rows = await self.con.execute(query, params).fetchall()
        return rows


class JSONFileHandler(Logger):

    def __init__(self,   fileSettings,     configPath,name ):
        super().__init__('fh', fileSettings["LOGNAMES"])
        filePath = configPath + "data/"
        os.makedirs(filePath, exist_ok=True)
        self.currentFile = None
        self.currentData = {}
        self.start = 0
        self.filePath = filePath
        self.maxEntries = fileSettings["MAXENTRIES"]
        self.saveInterval = fileSettings["SAVEINTERVAL"]
        self.pending = []
        self.latest = 0
        self.maxBlock = 0
        self.next = None
        self.lastSave = time.time()

    def createNewFile(self, startBlock=None):
        if startBlock is None:
            startBlock = self.latest + 1
        self.start = self.latest = startBlock

        self.currentFile = (self.start, self.latest)
        self.currentData = {}
        self.logInfo(f"new file created starting {self.latest}")
    @property
    def currentFileName(self):
        return f"{self.currentFile[0]}.{self.currentFile[1]}.json"
    
    def createNewFile(self, startBlock=None):
        if startBlock is None:
            startBlock = self.latest + 1
        self.start = self.latest = startBlock

        self.currentFile = (self.start, self.latest)
        self.currentData = {}
        self.logInfo(f"new file created starting {self.latest}")
    
    def save(self, deleteOld=True, indent=None):
        if self.currentData and self.latest != self.start:
            newName = f"{self.start}.{self.latest}.json"
            with open(self.filePath + newName, "w") as f:
                f.write(json.dumps(self.currentData, indent=indent))
            if deleteOld and newName != self.currentFileName:
                self.logDebug(f"deleting {self.currentFile}")
                try:
                    os.remove(self.filePath + self.currentFileName)
                except FileNotFoundError as e:
                    self.logDebug("filenotfound error deleting {e}")
            self.currentFile = (self.start, self.latest)
            self.lastSave = time.time()
            self.logInfo(f"current data saved to {self.currentFileName}")
        else:
            self.logDebug(f"{self.currentFile} not saved, no changed data")
    async def asyncSave(self, deleteOld=True, indent=None):
        if self.currentData and self.latest != self.start:
            newName = f"{self.start}.{self.latest}.json"
            async with aiofiles.open(self.filePath + newName, "w") as f:
                await f.write(json.dumps(self.currentData, indent=indent))
            if deleteOld and newName != self.currentFileName:
                self.logDebug(f"deleting {self.currentFile}")
                try:
                    os.remove(self.filePath + self.currentFileName)
                except FileNotFoundError as e:
                    self.logDebug("file not found error deleting {e}")
            self.currentFile = (self.start, self.latest)
            self.lastSave = time.time()
            self.logInfo(f"current data saved to {self.currentFileName}")
        else:
            self.logDebug(f"{self.currentFile} not saved, no changed data")
    
    def getEvents(self, start, end, results = {}):
        files = self.getFiles()
        for file in files:
            if file[1] >= start:
                data = self.loadFile(self.toFileName(file))
                if file[0]< start:
                    data = {key:value for key, value in data.items() if int(key) >= start}
                if file[1]> end:
                    data = {key:value for key, value in data.items() if int(key) <= end}
                    results.append(data)
                    return results
                results.update(data)
        return results

    def checkMissing(self, start, end):
        files = self.getFiles()
        missing = []
        i = 0
        started = False
        while start < end and i < len(files):
            if not started:
                if files[i][0] > start:
                    missing.append((start, files[i][0] - 1))
                    started = True
                elif files[i][1] >= start:
                    started = True
            else:
                if files[i][0] > start:
                    missing.append((start, files[i][0] - 1))
            start = max(files[i][1] + 1, start)
            i += 1
        if start < end:
            missing.append((start, end))
        self.logDebug(f"missing files: {missing}")
        return missing
    
    def getLatestFileFrom(self, startBlock):
        fileTuples = self.getFiles()
        lastFile = None
        for fileTuple in fileTuples:
            if lastFile is None:
                if startBlock <= fileTuple[1]:
                    if startBlock >= fileTuple[0]:
                        lastFile = fileTuple
                    else:
                        break
            else:
                if lastFile[1] + 1 == fileTuple[0]:
                    lastFile = fileTuple
        return lastFile

    def toFileName(self, value):
        return f"{value[0]}.{value[1]}.json"

    def loadFile(self, file):
        with open(f"{self.filePath}{file}") as f:
            return json.load(f)

    def setup(self, startBlock):
        self.logInfo(f"setting up new scan")
        if self.currentFile != None:
            self.save(indent=4)
        latestFileTuple = self.getLatestFileFrom(startBlock)
        if latestFileTuple is None:
            self.createNewFile(startBlock)
            self.latest = startBlock
            self.start = startBlock
            self.currentFile = (self.start, self.latest)
            self.currentData = {}
        else:
            self.start = latestFileTuple[0]
            self.latest = latestFileTuple[1]
            self.currentFile = (self.start, self.latest)
            self.currentData = self.loadFile(self.currentFileName)

        self.logDebug(f"setup complete, {self.currentFile} waiting for {self.latest}")
        return self.latest
    def getFiles(self):
        all_files = os.listdir(self.filePath)
        json_files = [
            (int(start), int(end))
            for file in all_files
            if file.endswith(".json")
            for start, end in [file.rsplit(".", 2)[:2]]
        ]
        return sorted(json_files, key=lambda x: x[0])
    
        #results array [fromBlock, events, toBlock]
    async def process(self, results, guarunteedContinuous =False):
        if guarunteedContinuous:
            for result in results:
                self.currentData.update(result[1]) #TODO deep update
                self.latest = max(result[2], self.latest)
            await self.asyncSave()
        else:
            for result in results:
                self.addToPending(result)
            self.mergePending()
        if len(self.currentData) > self.maxEntries:
            await self.asyncSave(indent=4)
            self.createNewFile()
        elif (
            time.time() > self.lastSave + self.saveInterval and self.currentData != None
        ):
            await self.asyncSave()


    def mergePending(self):
        while len(self.pending) > 0 and self.pending[0][0] <= self.latest + 1:
            self.currentData.update(self.pending[0][1])
            self.latest = max(self.pending[0][2], self.latest)
            self.pending[0][2] - self.pending[0][0]
            self.pending.pop(0)
        self.logInfo(f"waiting for: {self.latest+1}")


    def addToPending(self, element):
        position = 0
        while position < len(self.pending) and self.pending[position][0] < element[0]:
            position += 1
        self.pending.insert(position, element)
        self.logInfo(f"data added to pending {element[0]} to {element[2]}")