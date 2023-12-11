package client

type MetaConf struct {
	MetaServer  string
	IpfsApi     string     // for upload
	IpfsGateway string     // for download
	Aria2Conf   *Aria2Conf // for download
}

type Aria2Conf struct {
	Host   string `toml:"host"`
	Port   int    `toml:"port"`
	Secret string `toml:"secret"`
}

type JsonRpcParams struct {
	JsonRpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	Id      int           `json:"id"`
}

type IpfsCidInfo struct {
	IpfsCid     string `json:"ipfs_cid"`
	DataSize    int64  `json:"data_size"`
	IsDirectory bool   `json:"is_directory"`
}

// StoreSourceFile
// StoreSourceFile(ctx context.Context, datasetName string, ipfsData []IpfsData) APIResp

type IpfsData struct {
	IpfsCid     string `json:"ipfs_cid"`
	SourceName  string `json:"source_name"`
	DataSize    int64  `json:"data_size"`
	IsDirectory bool   `json:"is_directory"`
	DownloadUrl string `json:"download_url"`
}

type StoreSourceFileResponse struct {
	JsonRpc string `json:"jsonrpc"`
	Result  struct {
		Code    string `json:"code"`
		Message string `json:"message,omitempty"`
		Data    int64  `json:"data,omitempty"`
	} `json:"result"`
	Id int `json:"id"`
}

// GetDatasetList
// GetDatasetList(ctx context.Context, req GetDatasetListReq) APIResp

type DatasetListReq struct {
	DatasetName string `json:"dataset_name"`
	PageNum     int    `json:"page_num"`
	Size        int    `json:"size"`
}

type DatasetListResponse struct {
	JsonRpc string `json:"jsonrpc"`
	Result  struct {
		Code    string           `json:"code"`
		Message string           `json:"message,omitempty"`
		Data    DatasetListPager `json:"data,omitempty"`
	} `json:"result"`
	Id int `json:"id"`
}

type DatasetListPager struct {
	Total       int64            `json:"total"`
	PageCount   int64            `json:"pageCount"`
	DatasetList []*DatasetDetail `json:"dataset_list"`
}

type DatasetDetail struct {
	DataSetName   string            `json:"source_name"`
	DealFile      string            `json:"deal_file"`
	TaskName      string            `json:"task_name"`
	DatasetStatus string            `json:"dataset_status"`
	IpfsList      []*IpfsDataDetail `json:"ipfs_list"`
}

type IpfsDataDetail struct {
	DatasetName string `json:"dataset_name"`
	IpfsCid     string `json:"ipfs_cid"`
	DataSize    int64  `json:"data_size"`
	IsDirectory bool   `json:"is_directory"`
	DownloadUrl string `json:"download_url"`
}

// GetSourceFileInfo
// func (api *ApiImpl) GetSourceFileInfo(ctx context.Context, ipfsCid string) APIResp

type SourceFileInfoResponse struct {
	JsonRpc string `json:"jsonrpc"`
	Result  struct {
		Code    string            `json:"code"`
		Message string            `json:"message,omitempty"`
		Data    []*IpfsDataDetail `json:"data,omitempty"`
	} `json:"result"`
	Id int `json:"id"`
}

// GetSourceFileStatus
// func (api *ApiImpl) GetSourceFileStatus(ctx context.Context, req GetSourceFileStatusReq) APIResp

type SourceFileStatusReq struct {
	DatasetName string `json:"dataset_name"`
	IpfsCid     string `json:"ipfs_cid"`
	PageNum     int    `json:"page_num"`
	Size        int    `json:"size"`
}

type SourceFileStatusResponse struct {
	JsonRpc string `json:"jsonrpc"`
	Result  struct {
		Code    string                `json:"code"`
		Message string                `json:"message,omitempty"`
		Data    SourceFileStatusPager `json:"data,omitempty"`
	} `json:"result"`
	Id int `json:"id"`
}

type SourceFileStatusPager struct {
	Total     int64              `json:"total"`
	PageCount int64              `json:"pageCount"`
	CarList   []*SplitFileDetail `json:"car_list"`
}

type SplitFileDetail struct {
	FileName         string            `json:"file_name"`
	DataCid          string            `json:"data_cid"`
	FileSize         int64             `json:"file_size"`
	PieceCid         string            `json:"piece_cid"`
	DownloadUrl      string            `json:"download_url"`
	StorageProviders []StorageProvider `json:"storage_providers"`
}

type StorageProvider struct {
	StorageProviderId string `json:"storage_provider_id"`
	StorageStatus     string `json:"storage_status"`
	DealId            int64  `json:"deal_id"`
	DealCid           string `json:"deal_cid"` // proposal cid or uuid
	StartEpoch        int64  `json:"start_epoch"`
	EndEpoch          int64  `json:"end_epoch"`
	StartTime         string `json:"start_time"`
	EndTime           string `json:"end_time"`
}

// GetDownloadFileInfoByIpfsCid
// func (api *ApiImpl) GetDownloadFileInfoByIpfsCid(ctx context.Context, ipfsCid string) APIResp

type DownloadFileInfoResponse struct {
	JsonRpc string `json:"jsonrpc"`
	Result  struct {
		Code    string              `json:"code"`
		Message string              `json:"message,omitempty"`
		Data    []*DownloadFileInfo `json:"data,omitempty"`
	} `json:"result"`
	Id int `json:"id"`
}

type DownloadFileInfo struct {
	SourceName  string `json:"source_name"`
	DownloadUrl string `json:"download_url"`
	IsDirectory bool   `json:"is_directory"`
}

type RebuildReq struct {
	DatasetID   int64    `json:"dataset_id"`
	Object      string   `json:"object"`
	PayloadCIDs []string `json:"payload_cids"`
}

type RebuildData struct {
	Status     int      `json:"status"`
	PayloadCID string   `json:"payload_cid"`
	PayloadURL string   `json:"payload_url"`
	Providers  []string `json:"providers"`
	IsDir      bool     `json:"is_dir"`
	Size       int64    `json:"size"`
	DueAt      int64    `json:"due_at"`
}

type JsonRpcResp struct {
	JsonRpc string `json:"jsonrpc"`
	Result  struct {
		Code    string      `json:"code"`
		Message string      `json:"message,omitempty"`
		Data    interface{} `json:"data,omitempty"`
	} `json:"result"`
	Id int `json:"id"`
}

// list option
type listOption struct {
	ShowCar bool
}

type ListOption interface {
	apply(*listOption)
}

type funcOption struct {
	f func(*listOption)
}

func (fdo *funcOption) apply(do *listOption) {
	fdo.f(do)
}

func showStorageOption(f func(*listOption)) *funcOption {
	return &funcOption{
		f: f,
	}
}

func WithShowCar(show bool) ListOption {
	return showStorageOption(func(o *listOption) {
		o.ShowCar = show
	})
}
func defaultOptions() listOption {
	return listOption{
		ShowCar: false,
	}
}
