import os
import json


def deep_update(target, source):
    """
    Recursively update a target dictionary with values from a source dictionary.

    Args:
        target (dict): The dictionary to update.
        source (dict): The dictionary with new values to merge.

    Returns:
        dict: The updated target dictionary.
    """
    for key, value in source.items():
        if key in target and isinstance(target[key], dict) and isinstance(value, dict):
            deep_update(target[key], value)  # Recurse into nested dictionaries
        else:
            target[key] = value  # Overwrite or add new key-value pair
    return target
def getAndSortFiles(path):
    files = []
    for file in os.listdir(path):
            if file.endswith('.json'):
                file_path = os.path.join(path, file)
                try:
                    parts = file.split('.')
                    if len(parts) >= 2:
                        start_block = int(parts[0])
                        end_block = int(parts[1])
                        files.append((start_block, file_path, end_block))
                except ValueError:
                    print(f"Skipping invalid filename: {file}")
                    continue
    return files
def sort(path):
    files = getAndSortFiles(path)
    files.sort(key=lambda x: x[0])
    while len(files)>0:
        file = files.pop(0)
        print(f'checking {file}')
        startBlock, file_path, endBlock = file
        with open(file_path, 'r') as f:
            data = json.load(f)
        newfile = dict(sorted(data.items(), key=lambda x: int(x[0])))
        with open(file_path, 'w') as f:
            json.dump(newfile, f, indent=4)

def check(path1, path2, newPath=None,contracts = None):
    merged = {}
    chunks = []
# Collect all JSON files from both paths with their start blocks
    files = []
    for path in [path1, path2]:
        files += getAndSortFiles(path)
    files.sort(key=lambda x: x[0])
    checkFiles = getAndSortFiles(newPath)
    while len(files)>0:
        file = files.pop(0)
        print(f'checking {file}')
        startBlock, file_path, endBlock = file
        with open(file_path, 'r') as f:
            data = json.load(f)
        filtered_data = filterContracts(data, contracts)
        checkAgainst = {}
        for checkFile in checkFiles:
            if int(checkFile[2]) > int(startBlock):
                with open(checkFile[1], 'r') as f:
                    checkAgainst = deep_update(checkAgainst,json.load(f) )
            
            if int(checkFile[2]) > int(endBlock):
                break
        for block, blockData in filtered_data.items():
            for tx, txData in blockData.items():
                for contract, events in txData.items():
                    for event, eventData in events.items():
                        try:
                            checkAgainst[block][tx][contract][event] = 1
                        except:
                            print(block, tx, contract, event)


    # Sort files by start_block
    files.sort(key=lambda x: x[0])
    # Process files in sorted order
    start = None
    while len(files)>0:
        file = files.pop(0)
        startBlock, file_path, endBlock = file
        if start is None:
            start = startBlock
        with open(file_path, 'r') as f:
            data = json.load(f)
        filtered_data = filterContracts(data, contracts)
        merged.update(filtered_data)
        end = endBlock

def delete(path):
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        os.remove(file_path)

def addToDb(path1,path2, database):
    files = []
    for path in [path1, path2]:
        for file in os.listdir(path):
            if file.endswith('.json'):
                file_path = os.path.join(path, file)
                try:
                    parts = file.split('.')
                    if len(parts) >= 2:
                        start_block = int(parts[0])
                        end_block = int(parts[1])
                        files.append((start_block, file_path, end_block))

                except ValueError:
                    print(f"Skipping invalid filename: {file}")
                    continue

    # Sort files by start_block
    files.sort(key=lambda x: x[0])
    for file in files:
        startBlock, file_path, endBlock = file
        with open(file_path, 'r') as f:
            data = json.load(f)
        print(f'saving {file_path}')
        preparedEvents = database.prepareJsonEvents(data)
        database.addTupleEvents(preparedEvents)
        res = database.fetch_events((startBlock, endBlock))

def mergeData(path1, path2, newPath=None,contracts = None, maxEntries=100000, start = None, end = None ):
    if not os.path.exists(newPath):
            os.makedirs(newPath)
    else:
        for file in os.listdir(newPath):
            file_path = os.path.join(newPath, file)
            os.remove(file_path)
    merged = {}

# Collect all JSON files from both paths with their start blocks
    files = []
    for path in [path1, path2]:
        for file in os.listdir(path):
            if file.endswith('.json'):
                file_path = os.path.join(path, file)
                try:
                    parts = file.split('.')
                    if len(parts) >= 2:
                        start_block = int(parts[0])
                        end_block = int(parts[1])
                        if start is not None and start > end_block:
                            continue
                        elif end is not None and end < start_block:
                            continue
                        else:
                            files.append((start_block, file_path, end_block))

                except ValueError:
                    print(f"Skipping invalid filename: {file}")
                    continue

    # Sort files by start_block
    files.sort(key=lambda x: x[0])
    # Process files in sorted order
    start = None
    end = 0
    while len(files)>0:
        file = files.pop(0)
        print(f'merging {file}')
        startBlock, file_path, endBlock = file
        if start is None:
            start = startBlock
        with open(file_path, 'r') as f:
            data = json.load(f)
        addFiltered(data, merged, contracts)
        if end < endBlock:
            end = endBlock
        
        if len(merged)> maxEntries:
            leftover = {}
            while len(files)>0 and files[0][0] < end:
                file = files.pop(0)
                startBlock, file_path, endBlock = file
                with open(file_path, 'r') as f:
                    data = json.load(f)                   
                leftover = addFiltered(data, merged, contracts)
            toAdd = {k:v for k,v in merged.items() if int(k) >= start and int(k) <= end}
            notAdd = {k:v for k,v in merged.items() if int(k) < start or int(k) > end}
            toAdd = dict(sorted(toAdd.items(), key=lambda x: int(x[0])))
            
            with open(f'{newPath}/{start}.{end}.json', 'w') as f:
                json.dump(toAdd, f, indent=4)
            start = end+1
            merged = notAdd
            start = None
    if len(merged) > 0:
        toAdd = {k:v for k,v in merged.items() if int(k) >= start and int(k) <= end}
        notAdd = {k:v for k,v in merged.items() if int(k) < start or int(k) > end}
        toAdd = dict(sorted(toAdd.items(), key=lambda x: int(x[0])))
        with open(f'{newPath}/{start}.{end}.json', 'w') as f:
            json.dump(toAdd, f, indent=4)
    if len(notAdd) >0:
        with open('./tmp/noHome.json', 'w') as f:
                json.dump(merged, f)
    
    
def addFiltered(data, to, _contracts, maxBlock = None):
    leftover = {}
    addTo = to
    for block_number, transactions in data.items():
        if maxBlock is not None and int(block_number) > maxBlock:
            addTo = leftover
        for tx_hash, contracts in transactions.items():
            for contract, events in contracts.items():
                if contract in _contracts:
                        if block_number not in addTo:
                            addTo[block_number] = {}
                        if tx_hash not in addTo[block_number]:
                            addTo[block_number][tx_hash] = {}
                        if contract not in addTo[block_number][tx_hash]:
                            addTo[block_number][tx_hash][contract] = events
    return leftover
def filterContracts(data, _contracts):
    """
    Filter the data to include only events from specified contract addresses.

    Args:
        data (dict): Input data dictionary.
        contract_addresses (set): Set of contract addresses to filter.

    Returns:
        dict: Filtered data containing only specified contract addresses.
    """
    if not _contracts:
        return data  # No filtering if contract_addresses is empty

    filtered_data = {}
    for block_number, transactions in data.items():
        for tx_hash, contracts in transactions.items():
            for contract, events in contracts.items():
                if contract in _contracts:
                        if block_number not in filtered_data:
                            filtered_data[block_number] = {}
                        if tx_hash not in filtered_data[block_number]:
                            filtered_data[block_number][tx_hash] = {}
                        if contract not in filtered_data[block_number][tx_hash]:
                            filtered_data[block_number][tx_hash][contract] = events
    return filtered_data
def checkRanges(path):
    files = getAndSortFiles(path)
    files.sort(key=lambda x: x[0])
    os.makedirs('./tmp/', exist_ok=True)
    try:
        with open('./tmp/noHome.json', 'r') as f:
            noHome = json.load(f)
    except:
        noHome = {}
    while len(files)>0:
        file = files.pop(0)
        print(f'checking {file}')
        startBlock, file_path, endBlock = file
        with open(file_path, 'r') as f:
            data = json.load(f)
    found = {k:v for k, v in data.items() if k <startBlock or k >endBlock}
    if len(found)> 0:
        print(found)
        noHome.update(found)
        with open('./tmp/noHome.json', 'w') as f:
            json.dump(noHome, f)
        for block in noHome:
            del data[block]
        with open(file_path, 'w') as f:
            json.dump(data, f)

def addData(self, fileFrom, pathTo):
    with open(fileFrom, 'r') as f:
        newData = json.load(f)
    files = getAndSortFiles(pathTo)
    files.sort(key=lambda x: x[0])
    while len(newData)>0:
        block = list(newData.keys())[0]
        for file in files:
            startBlock, file_path, endBlock = file
            if startBlock <= block and endBlock>=block:
                toAdd = {k:v for k,v in newData.items() if k >=startBlock and k <= endBlock}
            with open(file_path, 'rw') as f:
                data = json.load(f)
                data = deep_update(data, toAdd)
                json.dump(data, f, indent = 4)

            

from dataManager import dm

if __name__ == "__main__":
    contracts = ['0x7f670f78B17dEC44d5Ef68a48740b6f8849cc2e6','0xE642657E4F43e6DcF0bd73Ef24008394574Dee28', '0x8351616F224a035Aa5ee6b9f74A68659701af3e9', '0x0AA3E62f4d97C404012352E881a2D0f2712c24A2', '0x445DeEbc5863a8Ae9e2Bdf7adceD6202509c5d5A']
    outPath = './recordData'
    # checkRanges(path)
    addToDb('./settings/base/data', './settings/base2/data', dm.tables['RecordData'])
    # addToDb('./settings/base2/data', recordTable)
    # delete(outPath)
    # mergeData('./settings/base/data', './settings/base2/data', outPath,contracts)
    # sort('./recordData')
    # check('./settings/base/data', './settings/base2/data', './recordData',contracts)