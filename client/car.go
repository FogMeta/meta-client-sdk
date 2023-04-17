package client

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/codingsince1985/checksum"
	"github.com/filedrive-team/go-graphsplit"
	"github.com/filswan/go-swan-lib/logs"
	"github.com/filswan/go-swan-lib/utils"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	DIR_NAME_INPUT  = "input"
	DIR_NAME_OUTPUT = "output"

	JSON_FILE_NAME_CAR_UPLOAD = "car.json"
	CSV_FILE_NAME_CAR_UPLOAD  = "car.csv"
)

type CmdGoCar struct {
	OutputDir          string   //required
	InputDir           []string //required
	GenerateMd5        bool     //required
	GocarFileSizeLimit int64    //required
	GocarFolderBased   bool     //required
	Parallel           int
}

type FileDesc struct {
	Uuid           string
	SourceFileName string
	SourceFilePath string
	SourceFileMd5  string
	SourceFileSize int64
	CarFileName    string
	CarFilePath    string
	CarFileMd5     string
	CarFileUrl     string
	CarFileSize    int64
	PayloadCid     string
	PieceCid       string
	StartEpoch     *int64
	SourceId       *int
}

func GetCmdGoCar(inputDir []string, outputDir *string, parallel int, carFileSizeLimit int64, carFolderBased, genMd5Flag bool) *CmdGoCar {
	cmdGoCar := &CmdGoCar{
		InputDir:           inputDir,
		GocarFileSizeLimit: carFileSizeLimit,
		GenerateMd5:        genMd5Flag,
		GocarFolderBased:   carFolderBased,
		Parallel:           parallel,
	}

	if !utils.IsStrEmpty(outputDir) {
		cmdGoCar.OutputDir = *outputDir
	} else {
		cmdGoCar.OutputDir = filepath.Join(*outputDir, time.Now().Format("2006-01-02_15:04:05")) + "_" + uuid.NewString()
	}

	return cmdGoCar
}

func CreateGoCarFilesByConfig(group Group, outputDir *string, parallel int, carFileSizeLimit int64, carFolderBased bool) ([]*FileDesc, error) {

	var inputs []string
	for _, fileInfo := range group.Items {
		inputs = append(inputs, fileInfo.Name)
	}

	cmdGoCar := GetCmdGoCar(inputs, outputDir, parallel, carFileSizeLimit, carFolderBased, false)
	fileDescs, err := cmdGoCar.CreateGoCarFiles()
	if err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	return fileDescs, nil
}

func RestoreCarFilesByConfig(inputDir []string, outputDir *string, parallel int) error {
	cmdGoCar := GetCmdGoCar(inputDir, outputDir, parallel, 0, false, false)
	err := cmdGoCar.RestoreCarToFiles()
	if err != nil {
		logs.GetLogger().Error(err)
		return err
	}
	return nil
}

func (cmdGoCar *CmdGoCar) CreateGoCarFiles() ([]*FileDesc, error) {
	//err := utils.CheckDirExists(cmdGoCar.InputDir, DIR_NAME_INPUT)
	//if err != nil {
	//	logs.GetLogger().Error(err)
	//	return nil, err
	//}

	err := utils.CreateDirIfNotExists(cmdGoCar.OutputDir, DIR_NAME_OUTPUT)
	if err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	sliceSize := cmdGoCar.GocarFileSizeLimit
	if sliceSize <= 0 {
		err := fmt.Errorf("gocar file size limit is too smal")
		logs.GetLogger().Error(err)
		return nil, err
	}

	srcFiles, err := ioutil.ReadDir(cmdGoCar.InputDir)
	if err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	carDir := cmdGoCar.OutputDir
	Emptyctx := context.Background()
	cb := graphsplit.CommPCallback(carDir, false, false)

	if cmdGoCar.GocarFolderBased {
		parentPath := cmdGoCar.InputDir
		targetPath := parentPath
		graphName := filepath.Base(parentPath)

		logs.GetLogger().Info("Creating car file for ", parentPath)
		err = graphsplit.ChunkMulti(Emptyctx, sliceSize, parentPath, []string{targetPath}, carDir, graphName, cmdGoCar.Parallel, cb)
		if err != nil {
			logs.GetLogger().Error(err)
			return nil, err
		}
		logs.GetLogger().Info("Car file for ", parentPath, " created")
	} else {
		for _, srcFile := range srcFiles {
			parentPath := filepath.Join(cmdGoCar.InputDir, srcFile.Name())
			targetPath := parentPath
			graphName := srcFile.Name()

			logs.GetLogger().Info("Creating car file for ", parentPath)
			err = graphsplit.ChunkMulti(Emptyctx, sliceSize, parentPath, []string{targetPath}, carDir, graphName, cmdGoCar.Parallel, cb)
			if err != nil {
				logs.GetLogger().Error(err)
				return nil, err
			}
			logs.GetLogger().Info("Car file for ", parentPath, " created")
		}
	}
	fileDescs, err := cmdGoCar.createFilesDescFromManifest()
	if err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	logs.GetLogger().Info(len(fileDescs), " car files have been created to directory:", carDir)
	logs.GetLogger().Info("Please upload car files to web server or ipfs server.")

	return fileDescs, nil
}

func (cmdGoCar *CmdGoCar) RestoreCarToFiles() error {
	//err := utils.CheckDirExists(cmdGoCar.InputDir, DIR_NAME_INPUT)
	//if err != nil {
	//	logs.GetLogger().Error(err)
	//	return err
	//}

	err := utils.CreateDirIfNotExists(cmdGoCar.OutputDir, DIR_NAME_OUTPUT)
	if err != nil {
		logs.GetLogger().Error(err)
		return err
	}

	graphsplit.CarTo(cmdGoCar.InputDir, cmdGoCar.OutputDir, cmdGoCar.Parallel)
	graphsplit.Merge(cmdGoCar.OutputDir, cmdGoCar.Parallel)

	logs.GetLogger().Info("car files have been restored to directory:", cmdGoCar.OutputDir)
	return nil
}

type ManifestDetail struct {
	Name string
	Hash string
	Size int
	Link []struct {
		Name string
		Hash string
		Size int64
	}
}

func (cmdGoCar *CmdGoCar) createFilesDescFromManifest() ([]*FileDesc, error) {
	manifestFilename := "manifest.csv"
	lines, err := utils.ReadAllLines(cmdGoCar.OutputDir, manifestFilename)
	if err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	fileDescs := []*FileDesc{}
	for i, line := range lines {
		if i == 0 {
			continue
		}

		fields := strings.Split(line, ",")
		if len(fields) < 5 {
			err := fmt.Errorf("not enough fields in %s", manifestFilename)
			logs.GetLogger().Error(err)
			return nil, err
		}

		fileDesc := FileDesc{}
		fileDesc.PayloadCid = fields[0]
		fileDesc.CarFileName = fileDesc.PayloadCid + ".car"
		fileDesc.CarFileUrl = fileDesc.CarFileName
		fileDesc.CarFilePath = filepath.Join(cmdGoCar.OutputDir, fileDesc.CarFileName)
		fileDesc.PieceCid = fields[2]
		fileDesc.CarFileSize = utils.GetInt64FromStr(fields[3])

		carFileDetail := fields[4]
		for i := 5; i < len(fields); i++ {
			carFileDetail = carFileDetail + "," + fields[i]
		}

		manifestDetail := ManifestDetail{}
		err = json.Unmarshal([]byte(carFileDetail), &manifestDetail)
		if err != nil {
			logs.GetLogger().Error("Failed to parse: ", carFileDetail)
			return nil, err
		}

		if cmdGoCar.GocarFolderBased {
			fileDesc.SourceFileName = filepath.Base(cmdGoCar.InputDir)
			fileDesc.SourceFilePath = cmdGoCar.InputDir
			for _, link := range manifestDetail.Link {
				fileDesc.SourceFileSize = fileDesc.SourceFileSize + link.Size
			}
		} else {
			fileDesc.SourceFileName = manifestDetail.Link[0].Name
			fileDesc.SourceFilePath = filepath.Join(cmdGoCar.InputDir, fileDesc.SourceFileName)
			fileDesc.SourceFileSize = int64(manifestDetail.Link[0].Size)
		}

		if cmdGoCar.GenerateMd5 {
			if utils.IsFileExistsFullPath(fileDesc.SourceFilePath) {
				srcFileMd5, err := checksum.MD5sum(fileDesc.SourceFilePath)
				if err != nil {
					logs.GetLogger().Error(err)
					return nil, err
				}
				fileDesc.SourceFileMd5 = srcFileMd5
			}

			carFileMd5, err := checksum.MD5sum(fileDesc.CarFilePath)
			if err != nil {
				logs.GetLogger().Error(err)
				return nil, err
			}
			fileDesc.CarFileMd5 = carFileMd5
		}

		fileDescs = append(fileDescs, &fileDesc)
	}

	_, err = WriteCarFilesToFiles(fileDescs, cmdGoCar.OutputDir, JSON_FILE_NAME_CAR_UPLOAD, CSV_FILE_NAME_CAR_UPLOAD)
	if err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	return fileDescs, nil
}

func WriteCarFilesToFiles(carFiles []*FileDesc, outputDir, jsonFilename, csvFileName string) (*string, error) {
	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	jsonFilePath, err := WriteFileDescsToJsonFile(carFiles, outputDir, jsonFilename)
	if err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	err = WriteCarFilesToCsvFile(carFiles, outputDir, csvFileName)
	if err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	return jsonFilePath, nil
}

func WriteFileDescsToJsonFile(fileDescs []*FileDesc, outputDir, jsonFileName string) (*string, error) {
	jsonFilePath := filepath.Join(outputDir, jsonFileName)
	content, err := json.MarshalIndent(fileDescs, "", " ")
	if err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	err = ioutil.WriteFile(jsonFilePath, content, 0644)
	if err != nil {
		logs.GetLogger().Error(err)
		return nil, err
	}

	logs.GetLogger().Info("Metadata json file generated: ", jsonFilePath)
	return &jsonFilePath, nil
}

func WriteCarFilesToCsvFile(carFiles []*FileDesc, outDir, csvFileName string) error {
	csvFilePath := filepath.Join(outDir, csvFileName)
	var headers []string
	headers = append(headers, "uuid")
	headers = append(headers, "source_file_name")
	headers = append(headers, "source_file_path")
	headers = append(headers, "source_file_md5")
	headers = append(headers, "source_file_size")
	headers = append(headers, "car_file_name")
	headers = append(headers, "car_file_path")
	headers = append(headers, "car_file_md5")
	headers = append(headers, "car_file_url")
	headers = append(headers, "car_file_size")
	headers = append(headers, "pay_load_cid")
	headers = append(headers, "piece_cid")
	headers = append(headers, "start_epoch")
	headers = append(headers, "source_id")
	headers = append(headers, "deals")

	file, err := os.Create(csvFilePath)
	if err != nil {
		logs.GetLogger().Error(err)
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write(headers)
	if err != nil {
		logs.GetLogger().Error(err)
		return err
	}

	for _, carFile := range carFiles {
		var columns []string
		columns = append(columns, carFile.Uuid)
		columns = append(columns, carFile.SourceFileName)
		columns = append(columns, carFile.SourceFilePath)
		columns = append(columns, carFile.SourceFileMd5)
		columns = append(columns, strconv.FormatInt(carFile.SourceFileSize, 10))
		columns = append(columns, carFile.CarFileName)
		columns = append(columns, carFile.CarFilePath)
		columns = append(columns, carFile.CarFileMd5)
		columns = append(columns, carFile.CarFileUrl)
		columns = append(columns, strconv.FormatInt(carFile.CarFileSize, 10))
		columns = append(columns, carFile.PayloadCid)
		columns = append(columns, carFile.PieceCid)

		if carFile.StartEpoch != nil {
			columns = append(columns, strconv.FormatInt(*carFile.StartEpoch, 10))
		} else {
			columns = append(columns, "")
		}

		if carFile.SourceId != nil {
			columns = append(columns, strconv.Itoa(*carFile.SourceId))
		} else {
			columns = append(columns, "")
		}

		// no deals
		columns = append(columns, "")

		err = writer.Write(columns)
		if err != nil {
			logs.GetLogger().Error(err)
			return err
		}
	}

	logs.GetLogger().Info("Metadata csv generated: ", csvFilePath)

	return nil
}
