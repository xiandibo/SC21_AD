"""部署kvtest合约"""

import json
from web3 import Web3
from solc import compile_standard
from concurrent.futures import ThreadPoolExecutor


'''def sendTxn(txn):
    # signed_txn = w3.eth.account.signTransaction(txn, private_key='123456')
    # res = w3.eth.sendRawTransaction(signed_txn.rawTransaction).hex()
    res = w3.eth.sendTransaction(txn).hex()
    txn_receipt = w3.eth.waitForTransactionReceipt(res)
    print(txn_receipt)'''


# w3 = Web3(Web3.HTTPProvider("http://10.20.36.229:5409"))
# w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/go-ethereum/poadata/signer1/data/geth.ipc"))
# w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/gethtest/go-ethereum/poadata/signer1/data/geth.ipc"))

compiled_sol_kvstore = compile_standard({
    "language": "Solidity",
    "sources": {
        "kvstore.sol": {
            "content": '''
                pragma experimental ABIEncoderV2;
pragma solidity >=0.5.0 <0.7.0;

interface kvstore {
    function get(string calldata key) external returns(string memory);
    function set(string calldata key, string calldata value) external;

}

contract Kvs{

     struct memaddr{

        address addr1;
        string key1;
        address addr2;
        string key2;

    }

    function test(
                memaddr memory arg1,
                memaddr memory arg2,
                memaddr memory arg3,
                memaddr memory arg4,
                memaddr memory arg5
                ) public{



        string memory result;
        kvstore kvst;
        uint temp = 30;

        kvst = kvstore(arg1.addr1);
        result = kvst.get(arg1.key1);
        sort(temp);
        kvst.set(arg1.key1,arg1.key2);
        //sort(temp);

        kvst = kvstore(arg1.addr2);
        result = kvst.get(arg1.key2);
        sort(temp);
        kvst.set(arg1.key2,arg1.key1);
        //sort(temp);

        kvst = kvstore(arg2.addr1);
        result = kvst.get(arg2.key1);
        sort(temp);
        kvst.set(arg2.key1,arg2.key2);
        //sort(temp);

        kvst = kvstore(arg2.addr2);
        result = kvst.get(arg2.key2);
        sort(temp);
        kvst.set(arg2.key2,arg2.key1);
        //sort(temp);

        kvst = kvstore(arg3.addr1);
        result = kvst.get(arg3.key1);
        sort(temp);
        kvst.set(arg3.key1,arg3.key2);
        //sort(temp);

        kvst = kvstore(arg3.addr2);
        result = kvst.get(arg3.key2);
        sort(temp);
        kvst.set(arg3.key2,arg3.key1);
        //sort(temp);

        kvst = kvstore(arg4.addr1);
        result = kvst.get(arg4.key1);
        sort(temp);
        kvst.set(arg4.key1,arg4.key2);
        //sort(temp);

        kvst = kvstore(arg4.addr2);
        result = kvst.get(arg4.key2);
        sort(temp);
        kvst.set(arg4.key2,arg4.key1);
        //sort(temp);

        kvst = kvstore(arg5.addr1);
        result = kvst.get(arg5.key1);
        sort(temp);
        kvst.set(arg5.key1,arg5.key2);
        //sort(temp);

        kvst = kvstore(arg5.addr2);
        result = kvst.get(arg5.key2);
        sort(temp);
        kvst.set(arg5.key2,arg5.key1);
        //sort(temp);



    }

    function sort(uint size) public{
        uint[] memory data = new uint[](size);
        for (uint x = 0; x < data.length; x++) {
            data[x] = size-x;
        }
        quickSort(data, int(0), int(data.length - 1));
        //quickSort(data, 0, data.length - 1);
    }


    function quickSort(uint[] memory arr, int left, int right) internal{
        int i = left;
        int j = right;
        if(i==j) return;
        uint pivot = arr[uint(left + (right - left) / 2)];
        while (i <= j) {
            while (arr[uint(i)] < pivot) i++;
            while (pivot < arr[uint(j)]) j--;
            if (i <= j) {
                (arr[uint(i)], arr[uint(j)]) = (arr[uint(j)], arr[uint(i)]);
                i++;
                j--;
            }
        }
        if (left < j)
            quickSort(arr, left, j);
        if (i < right)
            quickSort(arr, i, right);
    }


}


             '''
        }
    },
    "settings":
        {
            "outputSelection": {
                "*": {
                    "*": [
                        "metadata", "evm.bytecode", "evm.bytecode.sourceMap"
                    ]
                }
            }
        }
})

'''w3.eth.defaultAccount = w3.eth.accounts[0]

bytecode = compiled_sol_kvstore['contracts']['kvstore.sol']['Kvs']['evm']['bytecode']['object']
abi = json.loads(compiled_sol_kvstore['contracts']['kvstore.sol']['Kvs']['metadata'])['output']['abi']
kvstore = w3.eth.contract(abi=abi, bytecode=bytecode)

transaction = {
    'from': w3.eth.accounts[0],
    'gasPrice': w3.eth.gasPrice,
}

contract_data = kvstore.constructor().buildTransaction(
    transaction
)

tx_hash = w3.eth.sendTransaction(contract_data)

tx_receipt = w3.eth.waitForTransactionReceipt(tx_hash)

print(tx_receipt)

pool = ThreadPoolExecutor(36)
for i in range(0, 36):
    pool.submit(sendTxn, contract_data)'''



