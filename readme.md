# Introduction

This project consists of two parts: system source code `./code`, experimental test code `./workload`

The source code directory contains the following four algorithms:

| File path      | Descrition                                       |
| :------------- | :----------------------------------------------- |
| ./code/segment | Our concurrency control algorithm SC-Chef        |
| ./code/serial  | Ethereum's original serial method                |
| ./code/occ     | Classic optimistic concurrency control algorithm |
| ./code/tpl     | Classic two-phase lock algorithm                 |

Before performing the following steps, make sure to switch the current working directory to `./code/xxx/go-ethereum`

# Building the source code

Our SC-Chef and the three comparison methods OCC, 2PL and Serial are all implemented based on the classic Ethereum client go-ethereum v1.9.17.

Building the source code requires both a Go (version 1.14) and a C compiler.

Run the following command to build the source code.

```
$ make geth
```

# Run

Here is how to run a blockchain node and build a blockchain system.

## Run a blockchain node

1. Initialize node data

   We have prepared 4 folders in `./poadata/` to store node data: signer1, node1, node2, node3.

   Take the initialization of the signer1 node as an example：

   First delete the old node data:

   ```
   $ rm -rf ./poadata/signer1/data/geth/
   ```

   Initialize node data:

   ```
   $ ./build/bin/geth --datadir poadata/signer1/data init poadata/poatest.json
   ```

2. Run node

   If this is a mining node:

   ```
   $ ./build/bin/geth --datadir ./poadata/signer1/data --rpc --rpcaddr "0.0.0.0" --rpcport 5409 --rpcapi="eth,net,web3,personal,miner" --networkid 540980442 --port 9002 --allow-insecure-unlock --unlock 0xF886F1916926Dc60F9C75941dBEa1856f997FDe1 --password ./poadata/signer1/data/password  console
   ```

   Then use

   ```
   $ miner.start(1)
   ```

   to start mining and start generating blocks.

   

   If this is a validation node:

   ```
   $ ./build/bin/geth --datadir ./poadata/node1/data --rpc --rpcaddr "0.0.0.0" --rpcport 5409 --rpcapi="eth,net,web3,personal,miner" --networkid 540980442 --port 9002 --allow-insecure-unlock  console
   ```

Note: If you run multiple blockchain nodes on the same physical node, be careful not to use conflicting port numbers `--port port number`

## Build a blockchain system

If you want to build a blockchain system, you must first run multiple blockchain nodes. In this project, we have prepared four nodes: signer1 as a mining node, node1, node2 and node3 as verification nodes.

To build a blockchain system, three verification nodes need to be connected to the mining node.

In the console of the mining node:

```
$ admin.nodeInfo.enode
>"enode://ac6e7ea07632938ede95a368eb8d4b062472ac17aa24bd0e32e7d442a829847dc64bd6b4a337b973d1c52d0e44c0b64c5735ade383526923907d34b0e4979fa6@127.0.0.1:9002"
```

In the console of the verification node:

```
$admin.addPeer("enode://ac6e7ea07632938ede95a368eb8d4b062472ac17aa24bd0e32e7d442a829847dc64bd6b4a337b973d1c52d0e44c0b64c5735ade383526923907d34b0e4979fa6@127.0.0.1:9002")
>true
```



# Experiment 

We use python3 to implement the experimental code. The code directory is `./workload/SmartContractTest`

Dependent libraries needed to run the experiment: `web3 5.13.0` and `py-solc 3.2.0`.



First run `Deploy.py` to deploy smart contracts.

Then run `test.py` to send transactions.

In `test.py`,  `conflicRate` represents the contention rate of the generated workload.

