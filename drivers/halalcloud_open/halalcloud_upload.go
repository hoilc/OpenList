package halalcloudopen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	sdkUserFile "github.com/halalcloud/golang-sdk-lite/halalcloud/services/userfile"
	"github.com/ipfs/go-cid"
)

func (d *HalalCloudOpen) put(ctx context.Context, dstDir model.Obj, fileStream model.FileStreamer, up driver.UpdateProgress) (model.Obj, error) {

	newPath := path.Join(dstDir.GetPath(), fileStream.GetName())
	fmt.Printf("开始上传文件: %s, 大小: %d\n", newPath, fileStream.GetSize())

	uploadTask, err := d.sdkUserFileService.CreateUploadTask(ctx, &sdkUserFile.File{
		Path: newPath,
		Size: fileStream.GetSize(),
	})
	if err != nil {
		fmt.Printf("创建上传任务失败: %v\n", err)
		return nil, err
	}
	fmt.Printf("创建上传任务成功: TaskID=%s, BlockSize=%d\n", uploadTask.Task, uploadTask.BlockSize)

	if uploadTask.Created {
		fmt.Println("文件已存在，无需上传")
		return nil, nil
	}

	slicesList := make([]string, 0)
	codec := uint64(0x55)
	if uploadTask.BlockCodec > 0 {
		codec = uint64(uploadTask.BlockCodec)
	}
	blockHashType := uploadTask.BlockHashType
	mhType := uint64(0x12)
	if blockHashType > 0 {
		mhType = uint64(blockHashType)
	}
	prefix := cid.Prefix{
		Codec:    codec,
		MhLength: -1,
		MhType:   mhType,
		Version:  1,
	}
	blockSize := uploadTask.BlockSize

	fmt.Printf("开始读取文件并分片上传: BlockSize=%d\n", blockSize)

	// Not sure whether FileStream supports concurrent read and write operations, so currently using single-threaded upload to ensure safety.
	// read file
	reader := driver.NewLimitedUploadStream(ctx, fileStream)
	progressReader := driver.NewProgress(fileStream.GetSize(), up)
	teeReader := io.TeeReader(reader, progressReader)

	totalRead := int64(0)
	chunkIndex := 0
	for totalRead < fileStream.GetSize() {
		// 计算当前块应该读取的大小
		remaining := fileStream.GetSize() - totalRead
		currentBlockSize := int64(blockSize)
		if remaining < currentBlockSize {
			currentBlockSize = remaining
		}

		fmt.Printf("准备读取第 %d 个块: 应读取 %d 字节, 剩余 %d 字节\n", chunkIndex, currentBlockSize, remaining)

		// 创建适当大小的缓冲区
		buffer := make([]byte, currentBlockSize)

		// 读取完整的一块数据
		n, err := io.ReadFull(teeReader, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			fmt.Printf("读取文件块失败: %v\n", err)
			return nil, err
		}
		fmt.Printf("成功读取第 %d 个块: 实际读取 %d 字节\n", chunkIndex, n)

		if n > 0 {
			data := buffer[:n]
			fmt.Printf("开始上传第 %d 个块: 大小 %d 字节\n", chunkIndex, len(data))
			uploadCid, err := postFileSlice(ctx, data, uploadTask.Task, uploadTask.UploadAddress, prefix, retryTimes)
			if err != nil {
				fmt.Printf("上传第 %d 个块失败: %v\n", chunkIndex, err)
				return nil, err
			}
			slicesList = append(slicesList, uploadCid.String())
			fmt.Printf("第 %d 个块上传成功: CID=%s\n", chunkIndex, uploadCid.String())
			totalRead += int64(n)
			chunkIndex++
		}

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			fmt.Println("文件读取完成")
			break
		}
	}

	fmt.Printf("所有块上传完成，开始创建文件: 共 %d 个块\n", len(slicesList))
	newFile, err := makeFile(ctx, slicesList, uploadTask.Task, uploadTask.UploadAddress, retryTimes)
	if err != nil {
		fmt.Printf("创建文件失败: %v\n", err)
		return nil, err
	}
	fmt.Println("文件创建成功")

	return NewObjFile(newFile), nil

}

func makeFile(ctx context.Context, fileSlice []string, taskID string, uploadAddress string, retry int) (*sdkUserFile.File, error) {
	var lastError error = nil
	for i := 0; i < retry; i++ {
		fmt.Printf("开始创建文件 (尝试 %d/%d): TaskID=%s\n", i+1, retry, taskID)
		newFile, err := doMakeFile(fileSlice, taskID, uploadAddress)
		if err == nil {
			fmt.Printf("文件创建成功: %+v\n", newFile)
			return newFile, nil
		}
		fmt.Printf("文件创建失败 (尝试 %d/%d): %v\n", i+1, retry, err)
		if ctx.Err() != nil {
			return nil, err
		}
		if strings.Contains(err.Error(), "not found") {
			return nil, err
		}
		lastError = err
		time.Sleep(slicePostErrorRetryInterval)
	}
	return nil, fmt.Errorf("mk file slice failed after %d times, error: %s", retry, lastError.Error())
}

func doMakeFile(fileSlice []string, taskID string, uploadAddress string) (*sdkUserFile.File, error) {
	accessUrl := uploadAddress + "/" + taskID
	getTimeOut := time.Minute * 2
	u, err := url.Parse(accessUrl)
	if err != nil {
		return nil, err
	}
	n, _ := json.Marshal(fileSlice)
	fmt.Printf("发送创建文件请求: URL=%s, Slice数量=%d\n", accessUrl, len(fileSlice))
	httpRequest := http.Request{
		Method: http.MethodPost,
		URL:    u,
		Header: map[string][]string{
			"Accept":       {"application/json"},
			"Content-Type": {"application/json"},
			//"Content-Length": {strconv.Itoa(len(n))},
		},
		Body: io.NopCloser(bytes.NewReader(n)),
	}
	httpClient := http.Client{
		Timeout: getTimeOut,
	}
	httpResponse, err := httpClient.Do(&httpRequest)
	if err != nil {
		fmt.Printf("创建文件请求失败: %v\n", err)
		return nil, err
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusOK && httpResponse.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(httpResponse.Body)
		message := string(b)
		fmt.Printf("创建文件响应错误: StatusCode=%d, Message=%s\n", httpResponse.StatusCode, message)
		return nil, fmt.Errorf("mk file slice failed, status code: %d, message: %s", httpResponse.StatusCode, message)
	}
	fmt.Printf("创建文件响应成功: StatusCode=%d\n", httpResponse.StatusCode)
	b, _ := io.ReadAll(httpResponse.Body)
	// 打印响应的JSON内容，便于调试
	fmt.Printf("创建文件响应JSON: %s\n", string(b))
	var result *sdkUserFile.File
	err = json.Unmarshal(b, &result)
	if err != nil {
		fmt.Printf("解析创建文件响应失败: %v\n", err)
		return nil, err
	}
	return result, nil
}

func postFileSlice(ctx context.Context, fileSlice []byte, taskID string, uploadAddress string, preix cid.Prefix, retry int) (cid.Cid, error) {
	var lastError error = nil
	for i := 0; i < retry; i++ {
		fmt.Printf("开始上传文件块 (尝试 %d/%d): TaskID=%s, 块大小=%d\n", i+1, retry, taskID, len(fileSlice))
		newCid, err := doPostFileSlice(fileSlice, taskID, uploadAddress, preix)
		if err == nil {
			fmt.Printf("文件块上传成功 (尝试 %d/%d): CID=%s\n", i+1, retry, newCid.String())
			return newCid, nil
		}
		fmt.Printf("文件块上传失败 (尝试 %d/%d): %v\n", i+1, retry, err)
		if ctx.Err() != nil {
			return cid.Undef, err
		}
		time.Sleep(slicePostErrorRetryInterval)
		lastError = err
	}
	return cid.Undef, fmt.Errorf("upload file slice failed after %d times, error: %s", retry, lastError.Error())
}

func doPostFileSlice(fileSlice []byte, taskID string, uploadAddress string, preix cid.Prefix) (cid.Cid, error) {
	// 1. sum file slice
	fmt.Printf("计算文件块CID: 块大小=%d\n", len(fileSlice))
	newCid, err := preix.Sum(fileSlice)
	if err != nil {
		fmt.Printf("计算文件块CID失败: %v\n", err)
		return cid.Undef, err
	}
	// 2. post file slice
	sliceCidString := newCid.String()
	// /{taskID}/{sliceID}
	accessUrl := uploadAddress + "/" + taskID + "/" + sliceCidString
	getTimeOut := time.Second * 30
	// get {accessUrl} in {getTimeOut}
	u, err := url.Parse(accessUrl)
	if err != nil {
		fmt.Printf("解析上传URL失败: %v\n", err)
		return cid.Undef, err
	}
	fmt.Printf("发送检查文件块请求: URL=%s\n", accessUrl)
	// header: accept: application/json
	// header: content-type: application/octet-stream
	// header: content-length: {fileSlice.length}
	// header: x-content-cid: {sliceCidString}
	// header: x-task-id: {taskID}
	httpRequest := http.Request{
		Method: http.MethodGet,
		URL:    u,
		Header: map[string][]string{
			"Accept": {"application/json"},
		},
	}
	httpClient := http.Client{
		Timeout: getTimeOut,
	}
	httpResponse, err := httpClient.Do(&httpRequest)
	if err != nil {
		fmt.Printf("检查文件块请求失败: %v\n", err)
		return cid.Undef, err
	}
	fmt.Printf("检查文件块响应: StatusCode=%d\n", httpResponse.StatusCode)
	if httpResponse.StatusCode != http.StatusOK {
		fmt.Printf("检查文件块响应错误: StatusCode=%d\n", httpResponse.StatusCode)
		return cid.Undef, fmt.Errorf("upload file slice failed, status code: %d", httpResponse.StatusCode)
	}
	var result bool
	b, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		fmt.Printf("读取检查文件块响应失败: %v\n", err)
		return cid.Undef, err
	}
	err = json.Unmarshal(b, &result)
	if err != nil {
		fmt.Printf("解析检查文件块响应失败: %v\n", err)
		return cid.Undef, err
	}
	if result {
		fmt.Printf("文件块已存在，无需上传: CID=%s\n", newCid.String())
		return newCid, nil
	}

	fmt.Printf("开始上传文件块: URL=%s, 块大小=%d\n", accessUrl, len(fileSlice))
	httpRequest = http.Request{
		Method: http.MethodPost,
		URL:    u,
		Header: map[string][]string{
			"Accept":       {"application/json"},
			"Content-Type": {"application/octet-stream"},
			// "Content-Length": {strconv.Itoa(len(fileSlice))},
		},
		Body: io.NopCloser(bytes.NewReader(fileSlice)),
	}
	httpResponse, err = httpClient.Do(&httpRequest)
	if err != nil {
		fmt.Printf("上传文件块请求失败: %v\n", err)
		return cid.Undef, err
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusOK && httpResponse.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(httpResponse.Body)
		message := string(b)
		fmt.Printf("上传文件块响应错误: StatusCode=%d, Message=%s\n", httpResponse.StatusCode, message)
		return cid.Undef, fmt.Errorf("upload file slice failed, status code: %d, message: %s", httpResponse.StatusCode, message)
	}
	fmt.Printf("上传文件块成功: StatusCode=%d\n", httpResponse.StatusCode)

	return newCid, nil
}
