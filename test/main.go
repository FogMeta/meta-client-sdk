package main

import (
	"log"

	metacli "github.com/FogMeta/go-mc-sdk/client"
)

func main() {
	// Swan API key. Acquire from [Swan Platform](https://console.filswan.com/#/dashboard) -> "My Profile"->"Developer Settings". It can be ignored if `[sender].offline_swan=true`.
	key := "V0schjjl_bxCtSNwBYXXXX"
	// Swan API access token. Acquire from [Swan Platform](https://console.filswan.com/#/dashboard) -> "My Profile"->"Developer Settings". It can be ignored if `[sender].offline_swan=true`.
	token := "fca72014744019a949248874610fXXXX"
	metaUrl := "http://{ip}:8099/rpc/v0"
	apiUrl := "http://127.0.0.1:5001"
	ipfsGateway := "http://127.0.0.1:8080"
	aria2conf := &metacli.Aria2Conf{Host: "127.0.0.1", Port: 6800, Secret: "my_aria2_secret"}
	metaClient := metacli.NewClient(key, token, &metacli.MetaConf{
		MetaServer:  metaUrl,
		IpfsApi:     apiUrl,
		IpfsGateway: ipfsGateway,
		Aria2Conf:   aria2conf,
	})

	// update file(s) in testdata to IPFS server
	inputPath := "./testdata"
	ipfsData, err := metaClient.Upload(inputPath)
	if err != nil {
		log.Println("upload failed:", err)
		return
	}
	log.Println("upload success, and ipfs data: ", ipfsData)

	// report ipfs cid to meta server
	datasetName := "dataset-name"
	datasetId, err := metaClient.Backup(datasetName, ipfsData)
	if err != nil {
		log.Println("report meta client server  failed:", err)
		return
	}
	log.Println("report meta client server success")

	// download file(s) from IPFS server
	outPath := "./output"
	downloadUrl := "http://127.0.0.1:8080/ipfs/QmQgM2tGEduvYmgYy54jZaZ9D7qtsNETcog8EHR8XoeyEp"

	err = metaClient.Download(ipfsData.IpfsCid, outPath, downloadUrl)
	if err != nil {
		log.Println("download failed:", err)
		return
	}
	log.Println("download success")

	// get dataset list from meta server
	datasetListPager, err := metaClient.List(datasetName, 0, 10)
	if err != nil {
		log.Println("get dataset list failed:", err)
		return
	}
	log.Printf("get dataset list success: %+v\n", datasetListPager)

	// get source file information
	ipfsDataDetail, err := metaClient.SourceFileInfo(ipfsData.IpfsCid)
	if err != nil {
		log.Println("get source file information failed:", err)
		return
	}
	log.Printf("get source file information success: %+v\n", ipfsDataDetail)

	// get source file status
	sourceFileStatusPager, err := metaClient.ListStatus(datasetName, ipfsData.IpfsCid, 0, 10)
	if err != nil {
		log.Println("get source file status failed:", err)
		return
	}
	log.Printf("get source file status success: %+v\n", sourceFileStatusPager)

	// rebuild dataset
	data, err := metaClient.Rebuild(datasetId)
	if err != nil {
		log.Println("get source file status failed:", err)
		return
	}
	log.Printf("rebuild dataset success: %+v\n", data)
}
