import json
from web3 import Web3
from solc import compile_standard
from concurrent.futures import ThreadPoolExecutor
import KVtest
import KVstore

#  web3.py provider
w3 = Web3(Web3.HTTPProvider("http://10.20.36.229:5409"))
# w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/go-ethereum/poadata/signer1/data/geth.ipc"))
# w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/gethtest/go-ethereum/poadata/signer1/data/geth.ipc"))



def sendTxn(txn):
    # signed_txn = w3.eth.account.signTransaction(txn, private_key='123456')
    # res = w3.eth.sendRawTransaction(signed_txn.rawTransaction).hex()
    res = w3.eth.sendTransaction(txn).hex()
    # txn_receipt = w3.eth.waitForTransactionReceipt(res)
    # print(txn_receipt)
    print(res)


def main():
    # 设定合约创建者
    w3.eth.defaultAccount = w3.eth.accounts[0]

    # 编译后的合约字节码和ABI
    bytecode = KVtest.compiled_sol_kvstore['contracts']['kvstore.sol']['Kvs']['evm']['bytecode']['object']
    abi = json.loads(KVtest.compiled_sol_kvstore['contracts']['kvstore.sol']['Kvs']['metadata'])['output']['abi']
    # 生成合约对象
    kvtest = w3.eth.contract(abi=abi, bytecode=bytecode)

    # 初始化交易
    transaction = {
        'from': w3.eth.accounts[0],
        'gasPrice': w3.eth.gasPrice,
    }

    # 合约的创建数据写入交易
    contract_data = kvtest.constructor().buildTransaction(
        transaction
    )

    # 发送36个交易
    for i in range(0, 36):
        sendTxn(contract_data)

    # 创建kvstore合约 编译后的合约字节码和ABI
    bytecode = KVstore.compiled_sol_kvstore['contracts']['kvstore.sol']['kvstore']['evm']['bytecode']['object']
    abi = json.loads(KVstore.compiled_sol_kvstore['contracts']['kvstore.sol']['kvstore']['metadata'])['output']['abi']
    kvstore = w3.eth.contract(abi=abi, bytecode=bytecode)

    # 初始化交易
    transaction = {
        'from': w3.eth.accounts[0],
        'gasPrice': w3.eth.gasPrice,
    }

    # 合约的创建数据写入交易
    contract_data = kvstore.constructor().buildTransaction(
        transaction
    )

    # 发送36个交易
    for i in range(0, 36):
        sendTxn(contract_data)

    w3.geth.miner.stop()
    w3.geth.miner.start(1)


if __name__ == '__main__':
    main()




