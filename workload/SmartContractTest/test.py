"""调用入口合约"""

from web3 import Web3
import multiprocessing
import datetime
import zipfianGenerator

true = True
false = False

#w3 = Web3(Web3.IPCProvider("/home/xiandibo/goworkspace/src/segment/go-ethereum/poadata/signer1/data/geth.ipc"))
w3 = Web3(Web3.HTTPProvider("http://10.20.36.229:5409"))
# w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/go-ethereum/poadata/signer1/data/geth.ipc"))
#w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/2PL/go-ethereum/poadata/signer1/data/geth.ipc"))
# w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/OCC/go-ethereum/poadata/signer1/data/geth.ipc"))

# 参数
# 总的被访问数据量（关系到冲突率）
TotalRecords = 100000
# 冲突率
conflicRate = 0

# 使用合约数量（分区）
partitions = 16
# 发送交易数量
TransactionCount = 1024

num = zipfianGenerator.randZipf(TotalRecords, conflicRate, TransactionCount * 10)
# print(num)

config = {
    "abi": [
        {
            "inputs": [
                {
                    "components": [
                        {
                            "internalType": "address",
                            "name": "addr1",
                            "type": "address"
                        },
                        {
                            "internalType": "string",
                            "name": "key1",
                            "type": "string"
                        },
                        {
                            "internalType": "address",
                            "name": "addr2",
                            "type": "address"
                        },
                        {
                            "internalType": "string",
                            "name": "key2",
                            "type": "string"
                        }
                    ],
                    "internalType": "struct Kvs.memaddr",
                    "name": "arg1",
                    "type": "tuple"
                },
                {
                    "components": [
                        {
                            "internalType": "address",
                            "name": "addr1",
                            "type": "address"
                        },
                        {
                            "internalType": "string",
                            "name": "key1",
                            "type": "string"
                        },
                        {
                            "internalType": "address",
                            "name": "addr2",
                            "type": "address"
                        },
                        {
                            "internalType": "string",
                            "name": "key2",
                            "type": "string"
                        }
                    ],
                    "internalType": "struct Kvs.memaddr",
                    "name": "arg2",
                    "type": "tuple"
                },
                {
                    "components": [
                        {
                            "internalType": "address",
                            "name": "addr1",
                            "type": "address"
                        },
                        {
                            "internalType": "string",
                            "name": "key1",
                            "type": "string"
                        },
                        {
                            "internalType": "address",
                            "name": "addr2",
                            "type": "address"
                        },
                        {
                            "internalType": "string",
                            "name": "key2",
                            "type": "string"
                        }
                    ],
                    "internalType": "struct Kvs.memaddr",
                    "name": "arg3",
                    "type": "tuple"
                },
                {
                    "components": [
                        {
                            "internalType": "address",
                            "name": "addr1",
                            "type": "address"
                        },
                        {
                            "internalType": "string",
                            "name": "key1",
                            "type": "string"
                        },
                        {
                            "internalType": "address",
                            "name": "addr2",
                            "type": "address"
                        },
                        {
                            "internalType": "string",
                            "name": "key2",
                            "type": "string"
                        }
                    ],
                    "internalType": "struct Kvs.memaddr",
                    "name": "arg4",
                    "type": "tuple"
                },
                {
                    "components": [
                        {
                            "internalType": "address",
                            "name": "addr1",
                            "type": "address"
                        },
                        {
                            "internalType": "string",
                            "name": "key1",
                            "type": "string"
                        },
                        {
                            "internalType": "address",
                            "name": "addr2",
                            "type": "address"
                        },
                        {
                            "internalType": "string",
                            "name": "key2",
                            "type": "string"
                        }
                    ],
                    "internalType": "struct Kvs.memaddr",
                    "name": "arg5",
                    "type": "tuple"
                }
            ],
            "name": "test",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        }
    ],
    "address": "0x1f45395c8769A931Cd0E7A2a7133CEc492BD1e9c"  # 合约地址
}


def sendTxn(txn):
    # signed_txn = w3.eth.account.signTransaction(txn, private_key='123456')
    # res = w3.eth.sendRawTransaction(signed_txn.rawTransaction).hex()
    res = w3.eth.sendTransaction(txn).hex()
    # print("sendTransaction")
    # txn_receipt = w3.eth.waitForTransactionReceipt(res)

    # print("receipt hash: ")
    # print(res)
    # print(txn_receipt)
    # return txn_receipt


def buildTx(contract,
            address1, key1, address2, key2, address3, key3, address4, key4, address5, key5,
            address6, key6, address7, key7, address8, key8, address9, key9, address10, key10
            ):
    txn = contract.functions.test((address1, key1, address2, key2), (address3, key3, address4, key4),
                                  (address5, key5, address6, key6), (address7, key7, address8, key8),
                                  (address9, key9, address10, key10)
                                  ).buildTransaction(
        {
            'from': w3.eth.coinbase,
            'gasPrice': w3.eth.gasPrice,
            'gas': 3000000,
            'concurrencyInformation': {
                'commitPoint': 0,
                'callInformation': [
                    {
                        'readset': [
                            {
                                'varname': "store",
                                'elemkey': key1
                            }
                        ],
                        'address': address1,
                        'isAbortable': true
                    },
                    {
                        'writeset': [
                            {
                                'varname': "store",
                                'elemkey': key1
                            }
                        ],
                        'address': address1,
                        'isAbortable': true
                    },
                    {
                        'readset': [
                            {
                                'varname': "store",
                                'elemkey': key2
                            }
                        ],
                        'address': address2,
                        'isAbortable': true
                    },
                    {
                        'writeset': [
                            {
                                'varname': "store",
                                'elemkey': key2
                            }
                        ],
                        'address': address2,
                        'isAbortable': true
                    },
                    {
                        'readset': [
                            {
                                'varname': "store",
                                'elemkey': key3
                            }
                        ],
                        'address': address3,
                        'isAbortable': true
                    },
                    {
                        'writeset': [
                            {
                                'varname': "store",
                                'elemkey': key3
                            }
                        ],
                        'address': address3,
                        'sAbortable': true
                    },
                    {
                        'readset': [
                            {
                                'varname': "store",
                                'elemkey': key4
                            }
                        ],
                        'address': address4,
                        'isAbortable': true
                    },
                    {
                        'writeset': [
                            {
                                'varname': "store",
                                'elemkey': key4
                            }
                        ],
                        'address': address4,
                        'isAbortable': true
                    },
                    {
                        'readset': [
                            {
                                'varname': "store",
                                'elemkey': key5
                            }
                        ],
                        'address': address5,
                        'isAbortable': true
                    },
                    {
                        'writeset': [
                            {
                                'varname': "store",
                                'elemkey': key5
                            }
                        ],
                        'address': address5,
                        'isAbortable': true
                    },
                    {
                        'readset': [
                            {
                                'varname': "store",
                                'elemkey': key6
                            }
                        ],
                        'address': address6,
                        'isAbortable': true
                    },
                    {
                        'writeset': [
                            {
                                'varname': "store",
                                'elemkey': key6
                            }
                        ],
                        'address': address6,
                        'isAbortable': true
                    },
                    {
                        'readset': [
                            {
                                'varname': "store",
                                'elemkey': key7
                            }
                        ],
                        'address': address7,
                        'isAbortable': true
                    },
                    {
                        'writeset': [
                            {
                                'varname': "store",
                                'elemkey': key7
                            }
                        ],
                        'address': address7,
                        'isAbortable': true
                    },
                    {
                        'readset': [
                            {
                                'varname': "store",
                                'elemkey': key8
                            }
                        ],
                        'address': address8,
                        'isAbortable': true
                    },
                    {
                        'writeset': [
                            {
                                'varname': "store",
                                'elemkey': key8
                            }
                        ],
                        'address': address8,
                        'isAbortable': true
                    },
                    {
                        'readset': [
                            {
                                'varname': "store",
                                'elemkey': key9
                            }
                        ],
                        'address': address9,
                        'isAbortable': true
                    },
                    {
                        'writeset': [
                            {
                                'varname': "store",
                                'elemkey': key9
                            }
                        ],
                        'address': address9,
                        'isAbortable': true
                    },
                    {
                        'readset': [
                            {
                                'varname': "store",
                                'elemkey': key10
                            }
                        ],
                        'address': address10,
                        'isAbortable': true
                    },
                    {
                        'writeset': [
                            {
                                'varname': "store",
                                'elemkey': key10
                            }
                        ],
                        'address': address10,
                        'isAbortable': true
                    }
                ]
            }
        }
    )
    return txn


#w3 = Web3(Web3.IPCProvider("~/goworkspace/src/go-ethereum/poadata/signer1/data/geth.ipc"))
#w3 = Web3(Web3.HTTPProvider("http://10.20.36.229:5409"))
# w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/go-ethereum/poadata/signer1/data/geth.ipc"))
#w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/2PL/go-ethereum/poadata/signer1/data/geth.ipc"))
# w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/OCC/go-ethereum/poadata/signer1/data/geth.ipc"))

# kvtest合约 必须保证前36个
kvTestContractList = [
    '0x2F94181eE94a979a8aA2338B0D7e517232109F1F',
    '0xBAE42d4D68159E8D2e5044510b12bB06e5b2Db28',
    '0x0a8ECecA62C85e86F8a998A96dE53252B86b4Bbc',
    '0xd8A1363a7b141906bDa3AEC46ad56E7818998964',
    '0xcaE6752689E943D076A5b28BaB57d010e25A3D2F',
    '0x527cD95AFB34c528fDFFb3dC37849F89cb646c92',
    '0xE25790BDA44D3779E7D1783a25566Be6f708D3EF',
    '0xC71E314339c4C5d69F86823c21aab345471F5154',
    '0xAC5639f6377c8BA2c13824f027e9abcc30f289De',
    '0x2fe532ee67895A5D593e2441C07b25AfDc3E0E85',
    '0x7C02A8256130dD902262473A35DA840fb356cEE9',
    '0xE9C6204775206282BAEE3997eE6Ecf326292F36E',

    '0x72F7aB4002D4DeD975C6Bf9F6025A121Ef41Ad4b',
    '0x77cA4c6d53f0cE5DB0e69F9D152FaaB270eeD074',
    '0xd426269A1382f55F1CE62051dECB0A4dB7826f22',
    '0x17A6A674E351a75112C083A0A69e1D26FFA2E881',
    '0x28B1c37D32eBa8d93D46F4B20b6CDA0f0EfD4F10',
    '0x51850A079Bf423d8AC2c8cBEE914E03CEC5cEF8b',
    '0xb9fB09D9d0A00581A701D373b267aD5A8c35F233',
    '0x2507e4CE2F1872D5D114C1545AB2a1D831169a17',
    '0x65e884473A876816eece3c0d030B776012867BF0',
    '0x8a0E92f028E0D505F78e1FeB2B99bE49Ba7eB295',
    '0x86450a31A5DAdA866b80ec7760846E459aA7C378',
    '0x94a1b15A10D73098bc103B447Ea26Fb4f5714458',

    '0x9e7Cd02161d9cD547c35fB7778EB6F1C3286ce09',
    '0x887750bB2584192a9379770E384D580854532744',
    '0x1404e6d9B6c1C53545Ca19d5679eb88E16A0Ac58',
    '0x8398fd7f7bf958284AEC992Adbe73fe8456FA2c4',
    '0x8636602Cf3Ce7fA087900775D5B53892774879a8',
    '0x73AfD5Fc1304F86E993dDAA2a4C621Fc903DD8ED',
    '0xA3Ab92D6Cb804F795edfb2C5da5bb8203190813a',
    '0x8A446bc02E1e498D3Ae4551050B4C09e28C254F6',
    '0x2D48B558A66Cf71D1c78280aafFb8d6a84Dd55B8',
    '0x4d075d165c0Fc62648e1Ed7d1CC2d2c64d9b363E',
    '0xA61158B482F296c188A71975a625011ED88397Bd',
    '0xDee005bB0Cd7040024d10718a45134EC07cBA95A',

]

kvStoreContractList = [
    '0x355DA74B9f1B14BD4BF3c549B8d39aA1F36c3bD7',
    '0x6569eb12C8B72B4a25ee244Dae4c41Bb09Ceb06f',
    '0x7dB5492cafd3F8144e51fC83Bb1A40Bb681e6548',
    '0x161b7288A79925D5796AFE600982DbDD0Cbd6804',
    '0xF1389D8F30a4aDFB94fbCEEDa05313864A0e8B29',
    '0xbfb44733729E61Ab6c69c09DCAe3F55D304634fC',
    '0x6Ba20658267c858EB4fEa689549bC13785E4E7a9',
    '0xcacD5fF8e009a4245f5102E850A7CA84a0800033',
    '0xc5da1D39d3199897263c70247cb84D95EDCF4A80',
    '0x796F2cb11c541B8bcc5B049D57Af0eCBD5638cb3',
    '0xc3A9BdFf218A5138D2053792Bcdf535976E8b261',
    '0x4113bfB5F867Eaeeb123520b855C7277d27d6FCd',

    '0x4416cC79196CB8F1e9ab3CF087E8D3AFAA8b2D5B',
    '0xcAA886B87F8859c9efb650BafA7fCeA363E26BDa',
    '0x92a3Af08fc3226357B71B378775e6FFf53134cF1',
    '0x3d20Cd108a1152763bAc2148D7E12E7d833D1D2F',
    '0xBD13C85ed91bD0c172bAfb323074195c720F51e3',
    '0xDA2845fE56eF1318E7434ea22b3Cf286EfB99933',
    '0xAABC9Eb146490Be8745F37d6d569fE4d19c53A97',
    '0x569D5e7FaE553E9bE1f512A8DD8370d1f4ac54eC',
    '0xa94109269877762356a50E2832BC7eAcC0379A63',
    '0x81134E1802b6260C5459b043BE96Bfae6eA8fa8c',
    '0xAC106bD5D1d3eF61aBF3e9CCaeF123Ba70FEa38F',
    '0x7432ca8A9cD69D829932568394731E1db7D1b4C6',

    '0x786ddF434f038dfCeBd11410D1AF265a89435396',
    '0xB29BaCbD7a3B361e51127597491B314f77CbaA1d',
    '0xcC5502D1C176679c7C747fFf94C8324221c4E29c',
    '0x69091d08dbcd8Ec4074ACCd3d16D847910ba97fF',
    '0x6aA4bd7358cB8D9a15327bEAFe975f3b9f611055',
    '0x6e0CD9C1871b18637857Db73071a85f31FCA33db',
    '0x4e28De49e8e4FdaBdF1e4830a93C8674aC406a8d',
    '0x346665618B5E38a8ab4B24A3217f79Bc8dd3bDE9',
    '0x45DEA09C53E9D80547463B7FDd8FaDa9B4BD82A1',
    '0x8F2835E89F49dB4242DA43EFe6D214475A981485',
    '0x8017229Cd0396D9b4f138A67FbAB1bc4aFc435E3',
    '0xf21D29b0143FacE73699991A84b62156ADF36c55',
]

contractInstance = []
for i in kvTestContractList:
    contract_instance = w3.eth.contract(address=i, abi=config['abi'])
    contractInstance.append(contract_instance)


def sendTxs(txs):
    for elem in txs:
        sendTxn(elem)


def sendTransactions(start, end):
    for index in range(start, end, 1):
        partitionIndex = index % partitions
        # print(partitionIndex)
        keylist = num[index * 10: (index + 1) * 10]
        # print(keylist)

        tx = buildTx(contractInstance[partitionIndex],
                     kvStoreContractList[keylist[0] % partitions], str(keylist[0]),
                     kvStoreContractList[keylist[1] % partitions], str(keylist[1]),
                     kvStoreContractList[keylist[2] % partitions], str(keylist[2]),
                     kvStoreContractList[keylist[3] % partitions], str(keylist[3]),
                     kvStoreContractList[keylist[4] % partitions], str(keylist[4]),
                     kvStoreContractList[keylist[5] % partitions], str(keylist[5]),
                     kvStoreContractList[keylist[6] % partitions], str(keylist[6]),
                     kvStoreContractList[keylist[7] % partitions], str(keylist[7]),
                     kvStoreContractList[keylist[8] % partitions], str(keylist[8]),
                     kvStoreContractList[keylist[9] % partitions], str(keylist[9]),
                     )
        sendTxn(tx)




def main():
    for i in range(3):
        num = zipfianGenerator.randZipf(TotalRecords, conflicRate, TransactionCount * 10)
        start = datetime.datetime.now()
        processCount = 8
        processList = []
        for i in range(processCount):
            p = multiprocessing.Process(target=sendTransactions,
                                        args=(i * int(TransactionCount // processCount),
                                              (i + 1) * int(TransactionCount // processCount))
                                        , )
            processList.append(p)
            p.start()

        for p in processList:
            p.join()

        end = datetime.datetime.now()
        print((end - start).seconds)

    w3.geth.miner.stop()
    w3.geth.miner.start(1)


if __name__ == '__main__':
    main()


'''kvTestContractList = [
    #'0xc5a44222A47681b58A662A29BE5da90a5a66a32D',
    '0x8a0E92f028E0D505F78e1FeB2B99bE49Ba7eB295',
    '0x2F94181eE94a979a8aA2338B0D7e517232109F1F',
    '0xA3Ab92D6Cb804F795edfb2C5da5bb8203190813a',
    '0x2D48B558A66Cf71D1c78280aafFb8d6a84Dd55B8',
    '0x4d075d165c0Fc62648e1Ed7d1CC2d2c64d9b363E',
    '0x86450a31A5DAdA866b80ec7760846E459aA7C378',
    '0x8636602Cf3Ce7fA087900775D5B53892774879a8',
    '0x2507e4CE2F1872D5D114C1545AB2a1D831169a17',

    '0x887750bB2584192a9379770E384D580854532744',
    '0x7C02A8256130dD902262473A35DA840fb356cEE9',
    '0x527cD95AFB34c528fDFFb3dC37849F89cb646c92',
    '0xA61158B482F296c188A71975a625011ED88397Bd',
    '0xd426269A1382f55F1CE62051dECB0A4dB7826f22',
    '0x0a8ECecA62C85e86F8a998A96dE53252B86b4Bbc',
    '0x94a1b15A10D73098bc103B447Ea26Fb4f5714458',
    '0xDee005bB0Cd7040024d10718a45134EC07cBA95A',
]

kvStoreContractList = [
    #'0x8631b4Dad12f75a51910aE9b39b09e92846Dcfc0',
    '0x161b7288A79925D5796AFE600982DbDD0Cbd6804',
    '0x51850A079Bf423d8AC2c8cBEE914E03CEC5cEF8b',
    '0x1404e6d9B6c1C53545Ca19d5679eb88E16A0Ac58',
    '0x77cA4c6d53f0cE5DB0e69F9D152FaaB270eeD074',
    '0x9e7Cd02161d9cD547c35fB7778EB6F1C3286ce09',
    '0x17A6A674E351a75112C083A0A69e1D26FFA2E881',
    '0x2fe532ee67895A5D593e2441C07b25AfDc3E0E85',
    '0xE25790BDA44D3779E7D1783a25566Be6f708D3EF',

    '0x92a3Af08fc3226357B71B378775e6FFf53134cF1',
    '0xAC5639f6377c8BA2c13824f027e9abcc30f289De',
    '0xC71E314339c4C5d69F86823c21aab345471F5154',
    '0x73AfD5Fc1304F86E993dDAA2a4C621Fc903DD8ED',
    '0xE9C6204775206282BAEE3997eE6Ecf326292F36E',
    '0x28B1c37D32eBa8d93D46F4B20b6CDA0f0EfD4F10',
    '0x69091d08dbcd8Ec4074ACCd3d16D847910ba97fF',
    '0xAABC9Eb146490Be8745F37d6d569fE4d19c53A97',
]

contractInstance = []
for i in kvTestContractList:
    contract_instance = w3.eth.contract(address=i, abi=config['abi'])
    contractInstance.append(contract_instance)


def sendTxs(txs):
    for elem in txs:
        sendTxn(elem)


# 发送不能被整除的交易
TotalRecords = 100000
partitions = 12
TransactionCount = 400
num = zipfianGenerator.randZipf(TotalRecords, 0, TransactionCount * 10)


def sendTransactions(start, end):
    for index in range(start, end, 1):
        partitionIndex = index % (partitions)
        print(partitionIndex)
        keylist = num[index * 10: (index + 1) * 10]
        print(keylist)
        tx = buildTx(contractInstance[partitionIndex],
                     kvStoreContractList[keylist[0] % partitions], str(keylist[0]),
                     kvStoreContractList[keylist[1] % partitions], str(keylist[1]),
                     kvStoreContractList[keylist[2] % partitions], str(keylist[2]),
                     kvStoreContractList[keylist[3] % partitions], str(keylist[3]),
                     kvStoreContractList[keylist[4] % partitions], str(keylist[4]),
                     kvStoreContractList[keylist[5] % partitions], str(keylist[5]),
                     kvStoreContractList[keylist[6] % partitions], str(keylist[6]),
                     kvStoreContractList[keylist[7] % partitions], str(keylist[7]),
                     kvStoreContractList[keylist[8] % partitions], str(keylist[8]),
                     kvStoreContractList[keylist[9] % partitions], str(keylist[9]),

                     )
        sendTxn(tx)


start = datetime.datetime.now()
processCount = 8
processList = []
for i in range(processCount):
    # p = multiprocessing.Process(target=sendTxs, args=(8, 125,))
    p = multiprocessing.Process(target=sendTransactions,
                                args=(i * int(TransactionCount // processCount),
                                      (i + 1) * int(TransactionCount // processCount))
                                , )
    processList.append(p)
    p.start()

for p in processList:
    p.join()

end = datetime.datetime.now()
print((end - start).seconds)'''

'''kvTestContractList = [
    #'0xc5a44222A47681b58A662A29BE5da90a5a66a32D',
    '0x5BA991F9bFAbb021a45AE0194593621a4A77543A',
    '0x5bFe2aDBE2eCA307F7574F163217D35f70716DD2',
    '0xF351729EC5543C2EB18F39C26Bdd89D74657e866',
    '0xDdd60314Eb2ee6840Ef21EFb6B3A551328CFd372',
    '0x08851d3eF17c13bC1CC5EdfEa9580972f84edf14',
    '0x434A97A2c89a1bdAE999608a5C713Ea20028d142',
    '0xD61b674a835a4AA458Dc8fbc7b1a272ACd957bBa',
    '0x007f90A1B8F3396ad87C3B0700477c46A582937E',
]

kvStoreContractList = [
    #'0x8631b4Dad12f75a51910aE9b39b09e92846Dcfc0',
    '0xcc698ae6D9A807897Ff093f78cf35D842182bd35',
    '0x8f8BB1011b50CF232955677F4cE8262E488f9eD9',
    '0xe3491dE8f4FbA2A9995336E40bFF7b28385aC392',
    '0x1D3133683384C6b538A90851FB9Fd8c5FC94C5dc',
    '0x95f5E53C39506F55A5d8EC6521Af9DeEe68e82Ea',
    '0x98Ad049192E42E26204816Cda9aa2170703E7a0f',
    '0x61D9ecc3C1451320742622C44f7240459358453D',
    '0xDA6524C4B40600A2617E85BeaF551E00dcAc4F0D',
]'''
