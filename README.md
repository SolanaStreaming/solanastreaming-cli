# solanastreaming-cli

Command line tools for interacting with SolanaStreaming. This repo provides the `ss-cli` command which has various subcommands to assist with dealing with SolanaStreaming data.

## Installation

Download the latest package from github releases. Ensure to get the package that aligns with your system architecture. 
Precompiled binaries are available for osx, windows and linux. 
You can also compile from source instead if you need to.

## Usage
`ss-cli <subcommand> [subcommand options]`

## Command Overview

**simulate**
This command replicates the SolanaStreaming websocket server but with archive data. This means you can configure this server and connect to it as if it was production. 

**download**
The download command is used to help with downloading the multiple files available when purchasing archive data. The files can be large and numerous and this command manages the download and tracks the progress.

**reduce**
When downloading archive data, the files can be very large. The reduce command creates a copy of this data but reduced according to your filter specifications. E.g limit the data set to a specific list of tokens or wallets. The output reduced data set can then also be used with the simulate command.

## Simulate
This command replicates the SolanaStreaming websocket server but with archive data. This means you can configure this server and connect to it as if it was production. 

**Input Params**
- `data-dir` Defaults to `out`. The local directory containing the archive data you want to run in the simulation. 
- `port` Defaults to `8000`. The port the simulate websocket server will bind to on your local machine.

Once the server is running, send your subscribe messages to setup your subscriptions as normal. Once ready, to start the simulation send:
```
{"method":"startSimulation"}
```
to trigger the simulation to run. The server will then send events from your archive data just as it would on api.solanastreaming.com.
Once the simulation is finished, it will disconnect the client. 

**Notes**
- `latestBlockSubscribe` is not available on the simulation server.
- Swap filters do not function on the simulation server. You will receive all the data. If you need a subset, consider using the `reduce` command to pre-filter your dataset.
- If gaps exist in your `data-dir` files (e.g. from purchasing different non-consecutive days of you have a file missing), the server will stream the data regardless without checking for gaps.
- Since the archive data can be a large amount of data, the simulate command unzips each file only when it needs it to keep the memory and disk footprint down. It should also delete unzipped files after its finished so unnessisary disk smace is feed up.


## Download
The download command is used to help with downloading the multiple files available when purchasing archive data. The files can be large and numerous and this command manages the download and tracks the progress.

You can download your archive data order without this command if you need to from the dashboard.

**Input Params**
- `key` **required**. Your API key. 
- `order-id` **required**. The id of the order you want to download. This can be obtained from the orders section of the dashboard.
- `output-dir` Defaults to `out`. The directory of where to save the archive data it downloads. 
- `concurrency` Defaults to 1. This is how many concurrent connections to open to download the data. Its best to leave this at 1 unless you're using a high bandwidth internet connection. Max: `4`

Once your download is started, the command will estimate how long it will take to download the full set based on your current connection speed. 

## Reduce

**Input Params**
- `in-data-dir` Defaults to `out`. The data dir to read from.
- `out-data-dir` Defaults to `out-reduced`. The data dir to output to.
- `amm` A csv list of base58 encoded strings of the amm field include in the output data set.
- `baseTokenMint` A csv list of base58 encoded strings of the baseTokenMint field include in the output data set.
- `wallet` A csv list of base58 encoded strings of the wallet field include in the output data set.
- `concurrency` Defaults to `10`. How many files to process at once. The higher the number the faster it will complete but the more cpu it will use. If you want to restrict the process to 1 core only, set to `1`.