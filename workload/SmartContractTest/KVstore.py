"""
部署复杂的kvstore合约
读写同时，还附加一个快速排序操作
"""


from solc import compile_standard





# w3 = Web3(Web3.HTTPProvider("http://10.20.36.229:5409"))
# w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/go-ethereum/poadata/signer1/data/geth.ipc"))
# w3 = Web3(Web3.IPCProvider("/home/xiandibo/goWorkSpace/src/gethtest/go-ethereum/poadata/signer1/data/geth.ipc"))

compiled_sol_kvstore = compile_standard({
    "language": "Solidity",
    "sources": {
        "kvstore.sol": {
            "content": '''
                pragma solidity >=0.4.0 <0.7.0;

contract kvstore {

  mapping(string=>string) store;
  mapping(string=>uint) s;




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




  function get(string memory key) public returns(string memory) {
    //uint temp = parseInt(key, 0);
    //uint temp = 10;
    //sort(temp);
    return store[key];
  }
  function set(string memory key, string memory value) public {
    //uint temp = parseInt(key, 0);
    //uint temp = 10;
    //sort(temp);
    store[key] = value;
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
                        "metadata", "evm.bytecode"
                        , "evm.bytecode.sourceMap"
                    ]
                }
            }
        }
})


