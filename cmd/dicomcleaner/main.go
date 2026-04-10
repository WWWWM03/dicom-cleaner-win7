package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

type config struct {
	root                 string
	jobs                 int
	dryRun               bool
	includeDotUnderscore bool
	incremental          bool
	statePath            string
}

type fileState struct {
	Size    int64  `json:"size"`
	MtimeNS int64  `json:"mtime_ns"`
	Status  string `json:"status"`
}

type stateStore struct {
	Files map[string]fileState `json:"files"`
}

type job struct {
	path    string
	relPath string
	size    int64
	mtimeNS int64
}

type result struct {
	status string
	path   string
	size   int64
	mtime  int64
	err    error
}

type walkSummary struct {
	scannedFiles int
	queuedFiles  int
	skipped      int
	errorsSeen   []string
	errorCount   int
}

type transferSyntax struct {
	explicitVR bool
	little     bool
}

type elementHeader struct {
	group     uint16
	element   uint16
	headerLen int
	valueLen  uint32
	raw       []byte
}

func parseFlags() config {
	defaultJobs := runtime.NumCPU()
	if defaultJobs < 1 {
		defaultJobs = 1
	}
	if defaultJobs > 2 {
		defaultJobs = 2
	}

	cfg := config{}
	flag.IntVar(&cfg.jobs, "j", defaultJobs, "Number of worker threads.")
	flag.IntVar(&cfg.jobs, "jobs", defaultJobs, "Number of worker threads.")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "Only report matching DICOM files without modifying them.")
	flag.BoolVar(&cfg.includeDotUnderscore, "include-dot-underscore", false, "Also inspect macOS ._* files.")
	flag.BoolVar(&cfg.incremental, "incremental", false, "Skip files whose path, size, and mtime match the previous run.")
	flag.StringVar(&cfg.statePath, "state", "", "Path to the incremental state JSON file.")
	flag.Parse()

	if flag.NArg() > 0 {
		cfg.root = flag.Arg(0)
	} else {
		cfg.root = "."
	}
	return cfg
}

func loadState(path string) (*stateStore, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &stateStore{Files: map[string]fileState{}}, nil
		}
		return nil, err
	}

	var store stateStore
	if err := json.Unmarshal(data, &store); err != nil {
		return nil, err
	}
	if store.Files == nil {
		store.Files = map[string]fileState{}
	}
	return &store, nil
}

func saveState(path string, store *stateStore) error {
	if store == nil {
		return nil
	}

	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func shouldSkipIncremental(store *stateStore, relPath string, size, mtimeNS int64) bool {
	if store == nil {
		return false
	}
	state, ok := store.Files[relPath]
	if !ok {
		return false
	}
	return state.Size == size && state.MtimeNS == mtimeNS
}

func hasDICMPreamble(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()

	header := make([]byte, 132)
	n, err := io.ReadFull(file, header)
	if err != nil || n < 132 {
		return false
	}
	return string(header[128:132]) == "DICM"
}

func isLongVR(vr string) bool {
	switch vr {
	case "OB", "OD", "OF", "OL", "OV", "OW", "SQ", "UC", "UR", "UT", "UN":
		return true
	default:
		return false
	}
}

func readExactly(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func readElementHeader(r io.Reader, syntax transferSyntax) (elementHeader, error) {
	base, err := readExactly(r, 8)
	if err != nil {
		return elementHeader{}, err
	}

	h := elementHeader{headerLen: 8, raw: base}
	if syntax.little {
		h.group = binary.LittleEndian.Uint16(base[0:2])
		h.element = binary.LittleEndian.Uint16(base[2:4])
	} else {
		h.group = binary.BigEndian.Uint16(base[0:2])
		h.element = binary.BigEndian.Uint16(base[2:4])
	}

	if syntax.explicitVR {
		vr := string(base[4:6])
		if isLongVR(vr) {
			extra, extraErr := readExactly(r, 4)
			if extraErr != nil {
				return elementHeader{}, extraErr
			}
			h.headerLen = 12
			h.raw = append(h.raw, extra...)
			if syntax.little {
				h.valueLen = binary.LittleEndian.Uint32(extra)
			} else {
				h.valueLen = binary.BigEndian.Uint32(extra)
			}
			return h, nil
		}
		if syntax.little {
			h.valueLen = uint32(binary.LittleEndian.Uint16(base[6:8]))
		} else {
			h.valueLen = uint32(binary.BigEndian.Uint16(base[6:8]))
		}
		return h, nil
	}

	if syntax.little {
		h.valueLen = binary.LittleEndian.Uint32(base[4:8])
	} else {
		h.valueLen = binary.BigEndian.Uint32(base[4:8])
	}
	return h, nil
}

func determineSyntaxFromUID(value []byte) transferSyntax {
	uid := strings.TrimRight(string(value), "\x00 ")
	switch uid {
	case "1.2.840.10008.1.2":
		return transferSyntax{explicitVR: false, little: true}
	case "1.2.840.10008.1.2.2":
		return transferSyntax{explicitVR: true, little: false}
	default:
		return transferSyntax{explicitVR: true, little: true}
	}
}

func formatErrorMessage(err error) string {
	if err == nil {
		return ""
	}

	msg := err.Error()
	if strings.Contains(msg, "VR mismatch for tag (7fe0,0010)") {
		return "像素数据标签 PixelData(7FE0,0010) 的 VR 与该 Go DICOM 库的写回规则不兼容，文件未修改。通常说明这是压缩图像或一种库兼容性较差的 DICOM。"
	}
	if strings.Contains(msg, "Access is denied") {
		return "文件无法写入，可能被占用或没有权限。"
	}
	return msg
}

func scanForSpecificCharacterSet(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()

	preamble, err := readExactly(file, 132)
	if err != nil {
		return false, nil
	}
	if string(preamble[128:132]) != "DICM" {
		return false, nil
	}

	syntax := transferSyntax{explicitVR: true, little: true}
	for {
		h, headerErr := readElementHeader(file, syntax)
		if errors.Is(headerErr, io.EOF) || errors.Is(headerErr, io.ErrUnexpectedEOF) {
			return false, nil
		}
		if headerErr != nil {
			return false, headerErr
		}

		if h.group == 0x7FE0 && h.element == 0x0010 {
			return false, nil
		}

		if h.valueLen > 64*1024*1024 {
			return false, fmt.Errorf("发现超大标签值(%d字节)，为避免32位内存溢出，停止处理该文件", h.valueLen)
		}

		value, valueErr := readExactly(file, int(h.valueLen))
		if valueErr != nil {
			return false, valueErr
		}

		if h.group == 0x0002 && h.element == 0x0010 {
			syntax = determineSyntaxFromUID(value)
		}
		if h.group == 0x0008 && h.element == 0x0005 {
			return true, nil
		}
	}
}

func rewriteWithoutSpecificCharacterSet(path string) error {
	src, err := os.Open(path)
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	dst, err := os.Create(tmpPath)
	if err != nil {
		_ = src.Close()
		return err
	}

	cleanup := func(e error) error {
		_ = src.Close()
		_ = dst.Close()
		_ = os.Remove(tmpPath)
		return e
	}

	preamble, err := readExactly(src, 132)
	if err != nil {
		return cleanup(err)
	}
	if _, err := dst.Write(preamble); err != nil {
		return cleanup(err)
	}

	syntax := transferSyntax{explicitVR: true, little: true}
	removed := false

	for {
		h, headerErr := readElementHeader(src, syntax)
		if errors.Is(headerErr, io.EOF) {
			break
		}
		if headerErr != nil {
			return cleanup(headerErr)
		}

		if h.group == 0x7FE0 && h.element == 0x0010 {
			if _, err := dst.Write(h.raw); err != nil {
				return cleanup(err)
			}
			if _, err := io.Copy(dst, src); err != nil {
				return cleanup(err)
			}
			break
		}

		if h.valueLen > 64*1024*1024 {
			return cleanup(fmt.Errorf("发现超大标签值(%d字节)，为避免32位内存溢出，停止处理该文件", h.valueLen))
		}

		value, valueErr := readExactly(src, int(h.valueLen))
		if valueErr != nil {
			return cleanup(valueErr)
		}

		if h.group == 0x0002 && h.element == 0x0010 {
			syntax = determineSyntaxFromUID(value)
		}

		if h.group == 0x0008 && h.element == 0x0005 {
			removed = true
			continue
		}

		if _, err := dst.Write(h.raw); err != nil {
			return cleanup(err)
		}
		if _, err := dst.Write(value); err != nil {
			return cleanup(err)
		}
	}

	if err := dst.Close(); err != nil {
		return cleanup(err)
	}
	if !removed {
		_ = src.Close()
		_ = os.Remove(tmpPath)
		return nil
	}

	if err := src.Close(); err != nil {
		return cleanup(err)
	}

	if err := os.Remove(path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func removeSpecificCharacterSet(path string, dryRun bool) (string, error) {
	if !hasDICMPreamble(path) {
		return "non_dicom", nil
	}

	found, err := scanForSpecificCharacterSet(path)
	if err != nil {
		return "error", err
	}
	if !found {
		return "unchanged", nil
	}
	if dryRun {
		return "would_update", nil
	}
	if err := rewriteWithoutSpecificCharacterSet(path); err != nil {
		return "error", err
	}
	return "updated", nil
}

func worker(jobs <-chan job, results chan<- result, dryRun bool) {
	for item := range jobs {
		status, err := removeSpecificCharacterSet(item.path, dryRun)
		results <- result{
			status: status,
			path:   item.relPath,
			size:   item.size,
			mtime:  item.mtimeNS,
			err:    err,
		}
	}
}

func main() {
	cfg := parseFlags()
	root, err := filepath.Abs(cfg.root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Resolve root failed: %v\n", err)
		os.Exit(2)
	}
	if cfg.jobs < 1 {
		cfg.jobs = 1
	}

	info, err := os.Stat(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Root path error: %v\n", err)
		os.Exit(2)
	}
	if !info.IsDir() {
		fmt.Fprintf(os.Stderr, "Root path is not a directory: %s\n", root)
		os.Exit(2)
	}

	statePath := cfg.statePath
	if statePath == "" {
		statePath = filepath.Join(root, ".dicom_cleanup_state.json")
	}
	if !filepath.IsAbs(statePath) {
		statePath = filepath.Join(root, statePath)
	}

	var store *stateStore
	if cfg.incremental {
		store, err = loadState(statePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Load state failed: %v\n", err)
			os.Exit(2)
		}
	}

	jobs := make(chan job, cfg.jobs*4)
	results := make(chan result, cfg.jobs*4)

	var workerWG sync.WaitGroup
	for i := 0; i < cfg.jobs; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			worker(jobs, results, cfg.dryRun)
		}()
	}

	go func() {
		workerWG.Wait()
		close(results)
	}()

	counts := map[string]int{
		"updated":             0,
		"would_update":        0,
		"unchanged":           0,
		"non_dicom":           0,
		"error":               0,
		"skipped_incremental": 0,
	}
	var errorsSeen []string
	walkDone := make(chan walkSummary, 1)
	go func() {
		summary := walkSummary{}
		walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				summary.errorCount++
				summary.errorsSeen = append(summary.errorsSeen, fmt.Sprintf("%s: %v", path, err))
				return nil
			}
			if d.IsDir() {
				return nil
			}
			if !cfg.includeDotUnderscore && strings.HasPrefix(d.Name(), "._") {
				return nil
			}
			if cfg.incremental && sameFilePath(path, statePath) {
				return nil
			}

			info, statErr := d.Info()
			if statErr != nil {
				summary.errorCount++
				summary.errorsSeen = append(summary.errorsSeen, fmt.Sprintf("%s: %v", path, statErr))
				return nil
			}

			relPath, relErr := filepath.Rel(root, path)
			if relErr != nil {
				summary.errorCount++
				summary.errorsSeen = append(summary.errorsSeen, fmt.Sprintf("%s: %v", path, relErr))
				return nil
			}
			relPath = filepath.ToSlash(relPath)

			summary.scannedFiles++
			if shouldSkipIncremental(store, relPath, info.Size(), info.ModTime().UnixNano()) {
				summary.skipped++
				return nil
			}

			summary.queuedFiles++
			jobs <- job{
				path:    path,
				relPath: relPath,
				size:    info.Size(),
				mtimeNS: info.ModTime().UnixNano(),
			}
			return nil
		})
		close(jobs)

		if walkErr != nil {
			summary.errorCount++
			summary.errorsSeen = append(summary.errorsSeen, walkErr.Error())
		}
		walkDone <- summary
	}()

	for res := range results {
		counts[res.status]++
		if res.status == "updated" {
			fmt.Printf("已更新 %s\n", res.path)
		}
		if res.status == "would_update" {
			fmt.Printf("将更新 %s\n", res.path)
		}
		if res.err != nil {
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s: %s", res.path, formatErrorMessage(res.err)))
		}
		if store != nil && !cfg.dryRun && res.status != "error" {
			store.Files[res.path] = fileState{
				Size:    res.size,
				MtimeNS: res.mtime,
				Status:  res.status,
			}
		}
	}

	summary := <-walkDone
	counts["error"] += summary.errorCount
	counts["skipped_incremental"] += summary.skipped
	errorsSeen = append(errorsSeen, summary.errorsSeen...)

	if store != nil && !cfg.dryRun {
		if err := saveState(statePath, store); err != nil {
			counts["error"]++
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s: %s", statePath, formatErrorMessage(err)))
		}
	}

	fmt.Println()
	fmt.Printf("根目录: %s\n", root)
	fmt.Printf("扫描文件数: %d\n", summary.scannedFiles)
	fmt.Printf("进入处理队列数: %d\n", summary.queuedFiles)
	fmt.Printf("因增量缓存跳过: %d\n", counts["skipped_incremental"])
	fmt.Printf("已更新: %d\n", counts["updated"])
	fmt.Printf("仅预览将更新: %d\n", counts["would_update"])
	fmt.Printf("无需修改的 DICOM: %d\n", counts["unchanged"])
	fmt.Printf("非 DICOM 文件: %d\n", counts["non_dicom"])
	fmt.Printf("错误数: %d\n", counts["error"])

	if len(errorsSeen) > 0 {
		fmt.Println()
		for _, line := range errorsSeen {
			fmt.Fprintf(os.Stderr, "错误 %s\n", line)
		}
		os.Exit(1)
	}
}

func sameFilePath(a, b string) bool {
	cleanA := filepath.Clean(a)
	cleanB := filepath.Clean(b)
	return strings.EqualFold(cleanA, cleanB)
}
