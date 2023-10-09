# APIs

- [APIs](#apis)
  - [NewClient](#newclient)
  - [Upload](#upload)
  - [Backup](#backup)
  - [Download](#download)
  - [List](#list)
  - [ListStatus](#liststatus)
  - [SourceFileInfo](#sourcefileinfo)
  - [Rebuild](#rebuild)

## NewClient

Definition:
Creates a Meta Client instance.

```shell
func NewClient(key, token, conf *MetaConf) *MetaClient
```
Inputs:

| name        | type       | description              |
| ----------- | ---------- | ------------------------ |
| key         | string     | Swan API key             |
| token       | string     | Swan API token           |
| conf        | *MetaConf  | meta conf                |
| MetaServer  | string     | meta server url          |
| IpfsApi     | string     | ipfs api url             |
| IpfsGateway | string     | ipfs gateway url         |
| Aria2Conf   | *Aria2Conf | aria2 conf, for download |
| Host        | string     | aria2 host               |
| Port        | int        | aria2 port               |
| Secret      | string     | aria2 secret             |


**note**:
>key                    # Swan API key. Acquire from [Swan Platform](https://console.filswan.com/#/dashboard) -> "My Profile"->"Developer Settings". 

>token                  # Swan API access token. Acquire from [Swan Platform](https://console.filswan.com/#/dashboard) -> "My Profile"->"Developer Settings". 

Outputs:

```shell
*MetaClient            # Created Meta Client instance.
```

## Upload

`Upload` uploads file or directory to ipfs

```go
func (m *MetaClient) Upload(inputPath string) (ipfsData *IpfsData, err error) 
```

Inputs:

| name      | type   | description            |
| --------- | ------ | ---------------------- |
| inputPath | string | file or direction path |


Outputs:

**About `IpfsData`:**

```go
type IpfsData struct {
    IpfsCid     string `json:"ipfs_cid"`
    SourceName  string `json:"source_name"`
    DataSize    int64  `json:"data_size"`
    IsDirectory bool   `json:"is_directory"`
    DownloadUrl string `json:"download_url"`
}
```

| name        | type   | description                                                                                   |
| ----------- | ------ | --------------------------------------------------------------------------------------------- |
| IpfsCid     | string | The CID (Content Identifier) of the data in IPFS, which is used to uniquely identify the data |
| SourceName  | string | The name of the data source                                                                   |
| DataSize    | int64  | The size of the data in bytes                                                                 |
| IsDirectory | bool   | The type of data, used to differentiate whether it is a directory or not                      |
| DownloadUrl | string | The download link for the data, used to download the data file from IPFS                      |

## Backup

`Backup` backups the uploaded files with the datasetName,support multiple IpfsData

```go
func (m *MetaClient) Backup(datasetName string, ipfsData ...*IpfsData)   error 
```

Inputs:

| name        | type        | description                                     |
| ----------- | ----------- | ----------------------------------------------- |
| datasetName | string      | backup dataset name                             |
| ipfsData    | []*IpfsData | ipfs data list , refer to the `IpfsData` struct |


## Download

`Download` downloads all the files related with the specified ipfsCid default,and downloads specific files with the specified downloadUrl

```go
func (m *MetaClient) Download(ipfsCid string, outPath string, downloadUrl ...string) error
```

Inputs:

| name        | type   | description                                                                   |
| ----------- | ------ | ----------------------------------------------------------------------------- |
| ipfsCid     | string | ipfs cid in `IpfsData`                                                        |
| outPath     | string | download path                                                                 |
| downloadUrl | string | download url, if not given, just download all relevant files with the ipfsCid |


## List

`List` lists the backup files with the given datasetName

```go
func (m *MetaClient) List(datasetName string, pageNum, size int) (*ListPager, error) 
```

Inputs:

```shell
datasetName            # The dataset name.
pageNum                # Page number which to be queried
size                   # Size of records per page
```

Outputs:

```shell
*ListPager   # The pointer to the dataset descriptions, refer to the `Structs`.
error                  # error or nil
```

**About `ListPager`:**
```go
type ListPager struct {
    Total       int64            `json:"total"`
    PageCount   int64            `json:"pageCount"`
    DatasetList []*DatasetDetail `json:"dataset_list"`
}
```
| name        | type             | description                                                           |
| ----------- | ---------------- | --------------------------------------------------------------------- |
| Total       | int64            | The total number of datasets                                          |
| PageCount   | int64            | The total number of pages                                             |
| DatasetList | []*DatasetDetail | A list of dataset details, which contains the details of each dataset |

```
type DatasetDetail struct {
    DataSetName   string            `json:"source_name"`
    DealFile      string            `json:"deal_file"`
    TaskName      string            `json:"task_name"`
    DatasetStatus string            `json:"dataset_status"`
    IpfsList      []*IpfsDataDetail `json:"ipfs_list"`
}

```
| name          | type              | description                                                              |
| ------------- | ----------------- | ------------------------------------------------------------------------ |
| DataSetName   | string            | The name of the dataset                                                  |
| DealFile      | string            | The file name of the dataset                                             |
| TaskName      | string            | The name of the task associated with the dataset                         |
| DatasetStatus | string            | The status of the dataset, indicating downloading, downloaded, or others |
| IpfsList      | []*IpfsDataDetail | A list of data info associated with the dataset                          |


```
type IpfsDataDetail struct {
    DatasetName string `json:"dataset_name"`
    IpfsCid     string `json:"ipfs_cid"`
    DataSize    int64  `json:"data_size"`
    IsDirectory bool   `json:"is_directory"`
    DownloadUrl string `json:"download_url"`
}
```
| name        | type   | description                                                                                |
| ----------- | ------ | ------------------------------------------------------------------------------------------ |
| DatasetName | string | The name of the dataset                                                                    |
| IpfsCid     | string | The CID (Content Identifier) of the IPFS data, which is used to uniquely identify the data |
| DataSize    | int64  | The size of the IPFS data in bytes                                                         |
| IsDirectory | bool   | The type of data, used to differentiate whether it is a directory or not                   |
| DownloadUrl | string | The download url for the IPFS data, used to download the data file from the IPFS gateway   |

## ListStatus

`ListStatus` lists the status of backup files

```go
func (m *MetaClient) ListStatus(datasetName, ipfsCid string, pageNum, size int) (*ListStatusPager, error)
```

Inputs:

```shell
datasetName            # The dataset name.
ipfsCid                # IPFS cid to be queried
pageNum                # Which page to query
size                   # Size of records per page
```

Outputs:

```shell
*ListStatusPager  # The pointer to source file status corresponding to the queried IPFS CID
error                      # error or nil
```


**About `ListStatusPager`:**
```go
type ListStatusPager struct {
    Total     int64              `json:"total"`
    PageCount int64              `json:"pageCount"`
    CarList   []*SplitFileDetail `json:"car_list"`
}
```
| name      | type               | description                                                                 |
| --------- | ------------------ | --------------------------------------------------------------------------- |
| Total     | int64              | The total number of split files                                             |
| PageCount | int64              | The total number of pages in the split file list                            |
| CarList   | []*SplitFileDetail | A list of split file details, which contains the details of each split file |


```
type SplitFileDetail struct {
    FileName         string            `json:"file_name"`
    DataCid          string            `json:"data_cid"`
    FileSize         int64             `json:"file_size"`
    PieceCid         string            `json:"piece_cid"`
    DownloadUrl      string            `json:"download_url"`
    StorageProviders []StorageProvider `json:"storage_providers"`
}
```
| name             | type              | description                                                                   |
| ---------------- | ----------------- | ----------------------------------------------------------------------------- |
| FileName         | string            | The name of the split file                                                    |
| DataCid          | string            | The CID (Content Identifier) of the data associated with the split file       |
| FileSize         | int64             | The size of the split file in bytes                                           |
| PieceCid         | string            | The CID (Content Identifier) of the piece associated with the split file      |
| DownloadUrl      | string            | The download url for the CAR of the split file, used to download the CAR file |
| StorageProviders | []StorageProvider | A list of storage providers that have stored the split file                   |


```
type StorageProvider struct {
    StorageProviderId string `json:"storage_provider_id"`
    StorageStatus     string `json:"storage_status"`
    DealId            int64  `json:"deal_id"`
    DealCid           string `json:"deal_cid"`
    StartEpoch        int64  `json:"start_epoch"`
    EndEpoch          int64  `json:"end_epoch"`
    StartTime         string `json:"start_time"`
    EndTime           string `json:"end_time"`       
}
```
| name              | type   | description                                           |
| ----------------- | ------ | ----------------------------------------------------- |
| StorageProviderId | string | The ID of the storage provider                        |
| StorageStatus     | string | The status of the deal in the Filecoin network        |
| DealId            | int64  | The dealID in the filecoin network                    |
| DealCid           | string | The proposal CID or deal UUID in the filecoin network |
| StartEpoch        | int64  | The start epoch of deal in the filecoin network       |
| EndEpoch          | int64  | The end epoch of deal in the filecoin network         |
| StartTime         | string | The start UTC time of deal in the filecoin network    |
| EndTime           | string | The end UTC time of deal in the filecoin network      |

## SourceFileInfo
 
Definition:
Get the source file information by the IPFS CID from the Meta Server.

```go
func (m *MetaClient) SourceFileInfo(ipfsCid string) ([]IpfsDataDetail, error)
```

Inputs:

```shell
ipfsCid               # IPFS cid to be queried
```

Outputs:

```shell
[]IpfsDataDetail       # List of IPFS data details corresponding to the queried IPFS CID
error                  # error or nil
```

Structs:
```go
type IpfsDataDetail struct {
    DatasetName string `json:"dataset_name"`
    IpfsCid     string `json:"ipfs_cid"`
    DataSize    int64  `json:"data_size"`
    IsDirectory bool   `json:"is_directory"`
    DownloadUrl string `json:"download_url"`
}
```
| name        | type   | description                                                                                |
| ----------- | ------ | ------------------------------------------------------------------------------------------ |
| DatasetName | string | The name of the dataset                                                                    |
| IpfsCid     | string | The CID (Content Identifier) of the IPFS data, which is used to uniquely identify the data |
| DataSize    | int64  | The size of the IPFS data in bytes                                                         |
| IsDirectory | bool   | The type of data, used to differentiate whether it is a directory or not                   |
| DownloadUrl | string | The download link for the IPFS data, used to download the data file from the IPFS gateway  |

## Rebuild
 
Definition:

Rebuild the backup file when the local file is missed

```go
func (m *MetaClient) Rebuild(datasetId int64, ipfsCids ...string) ([]*RebuildData, error)
```

Inputs:

```shell
datasetId            # The dataset id.
ipfsCids             # IPFS cids to be queried,if not provided, means rebuild all dataset
```

Outputs:

```shell
[]*RebuildData       # List of rebuild data details
error                # error or nil
```

Structs:
```go
type RebuildData struct {
	Status     int      `json:"status"`
	PayloadCID string   `json:"payload_cid"`
	PayloadURL string   `json:"payload_url"`
	Providers  []string `json:"providers"`
        IsDir      bool     `json:"is_dir"`
	Size       int64    `json:"size"`
	DueAt      int64    `json:"due_at"`
	CreatedAt  int64    `json:"created_at"`
}
```
| name        | type     | description                                                                                |
| ----------- | -------- | ------------------------------------------------------------------------------------------ |
| Status      | int      | rebuild status                                                                             |
| payload_cid | string   | The CID (Content Identifier) of the IPFS data, which is used to uniquely identify the data |
| payload_url | string   | download url for the ipfs cid                                                              |
| providers   | []string | the storage providers for the ipfs cid                                                     |
| is_dir      | bool     | whether is directory                                                                       |
| size        | int64    | file size                                                                                  |
| due_at      | int64    | storage expired time unix, unit second                                                     |
| created_at  | int64    | created time unix, unit second                                                             |
