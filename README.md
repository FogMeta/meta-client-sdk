# go-mc-sdk

[![Made by FogMeta](https://img.shields.io/badge/made%20by-FogMeta-green.svg)](https://en.fogmeta.com/)
[![Twitter Follow](https://img.shields.io/twitter/follow/FogMeta)](https://twitter.com/FogMeta)
[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg)](https://github.com/RichardLitt/standard-readme)

A Golang SDK for the MetaArk product, providing an easy interface for developers to deal with the Filecoin network. It streamlines the process of securely storing, retrieving and recovering data on the IPFS and Filecoin network. 

MetaArk is a Web3 data service that securely stores data backups and enables data recovery. It automatically records data storage information and stores data on both the IPFS gateway and Filecoin network, providing fast retrieval and permanent backup.

## Features

`go-mc-sdk` provides the following features:

- Upload files or directories to the IPFS gateway
- Report data information to the Meta-Client server 
    - MetaArk will automatically complete data processing(split or merge file and generate CAR files)
    - Store the CAR file in the IPFS gateway
    - Send CAR files to the storage providers in the Filecoin network
- Download files or directories to the local machine
- Query the dataset list and details by the dataset name
- Get a source file information by the IPFS CID
- Acquire the storage information and status of the dataset.

## Prerequisites

Before using `go-mc-sdk`, you need to install the following services:

- Aria2 service (used to download file)

```
sudo apt install aria2 
```
- [IPFS service](https://docs.ipfs.tech/install/command-line/#install-official-binary-distributions)
- [Go](https://golang.org/dl/) (1.20 or later)

## Installation

To install `go-mc-sdk`, run the following command:

```
go get github.com/FogMeta/go-mc-sdk
```


## Usage

### [New client](document/api.md#newclient)

```
    key := "V0schjjl_bxCtSNwBYXXXX"
    token := "fca72014744019a949248874610fXXXX"
    metaClient := client.NewClient(key, token, &client.MetaConf{
        MetaServer: "", // client server
        IpfsApi:"",     // for upload
        IpfsGateway:"", // for download
        Aria2Conf:&client.Aria2Conf{ // for download
            Host:"",
            Port:"",
            Secret:""
        }
    })
```
>`key` : Swan API key. Acquire from [Swan Platform](https://console.filswan.com/#/dashboard) -> "My Profile"->"Developer Settings". 

>`token`: Swan API access token. Acquire from [Swan Platform](https://console.filswan.com/#/dashboard) -> "My Profile"->"Developer Settings". 

### [Upload](document/api.md#upload) 

`UploadFile` uploads file to the IPFS server, support file & directory


```
    ipfsData, err := metaClient.Upload("./testdata")
```

### [Backup](document/api.md#backup)

`BackupFile` backups uploaded files with the given `datasetName`

```
    err = metaClient.Backup("dataset-name", ipfsData)
```

### [Download](document/api.md#download)

`Download` downloads files related with ipfsCid to `outPath`, support download specific url to `outPath`

```
    err := metaClient.Download(ipfsCid, outPath)              // download all files related with ipfsCid to outPath
    err := metaClient.Download(ipfsCid, outPath, downloadUrl) // download specific url to outPath
```

### [List](document/api.md#list)

`List` lists files related with the `backup` `datasetName`

```
    pageNum := 0 // start from 0
    pageSize := 10
    datasetListPager, err := metaClient.List("dataset-name", pageNum, pageSize)
```

### [ListStatus](document/api.md#liststatus)

`ListStatus` lists the status of files related with the `backup` `datasetName` & `ipfsCid`

```
    pageNum := 0 // start from 0
    pageSize := 10
    datasetListPager, err := metaClient.ListStatus("dataset-name", ipfsCid, pageNum, pageSize)
```

### [Rebuild](document/api.md#rebuild)

`Rebuild` rebuilds the backup dataset files

```
    list, err := metaClient.Rebuild(datasetId, ipfsCids...)
```

## API Documentation

For more details, please check out the [API Documentation](document/api.md ':include').

## Contributing

Contributions to go-mc-sdk are welcome! If you find any errors or want to add new features, please submit an [Issue](https://github.com/FogMeta/go-mc-sdk/issues), or initiate a [Pull Request](https://github.com/FogMeta/go-mc-sdk/pulls).

## License

go-mc-sdk is licensed under the MIT License.
