package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/pkg/logger/message/audit"
)

type LogEntry interface{}

// Logger represents the logger
type Logger[T LogEntry] struct {
	dirs []string
	// log buffers
	logBuffer []T
	logMu     sync.Mutex

	// current index
	currentDirIndex       int
	currentFile           string
	currentCompressedSize int64
	currentFileEntryCount int
	maxEntriesPerFile     int

	batchSize int
	maxSize   int64

	logChan chan T
	wg      sync.WaitGroup
	workers int
	sync.RWMutex
}

// NewLogger returns a logger instance
func NewLogger[T LogEntry](dirs []string, batchSize int, maxSize int64, flushInterval time.Duration, workers int) *Logger[T] {
	var currentDirIndex int
	var foundLock bool
	var currentCompressedSize int64
	var currentFile string
	// check for dir.lock file inside the directories
	// if present get the compressed size of the directory
	for index, dir := range dirs {
		lockFilePath := filepath.Join(dir, "dir.lock")
		if _, err := os.Stat(lockFilePath); err == nil {
			currentDirIndex = index
			foundLock = true

			currentCompressedSize, currentFile, err = getCurrentDirInfo(dir)
			if err != nil {
				panic(err)
			}
			break
		}
	}

	l := &Logger[T]{
		dirs:      dirs,
		batchSize: batchSize,
		maxSize:   maxSize,
		// we need the buffer to be 50%, so that
		// we can cache the logs during the flush time
		logChan: make(chan T, batchSize/2),
		workers: workers,
		// use double the batchSize because we commit even if the
		// log buffer is half full
		logBuffer:             make([]T, 0, batchSize*2),
		maxEntriesPerFile:     100,
		currentDirIndex:       currentDirIndex,
		currentCompressedSize: currentCompressedSize,
		currentFile:           currentFile,
	}

	// if dir.lock is not found, prepare the first directory
	if !foundLock {
		if err := prepareDirectory(l.dirs[l.currentDirIndex]); err != nil {
			panic(err)
		}
	}

	// flush every "flushInterval" duration
	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				l.logMu.Lock()
				if len(l.logBuffer) > 0 {
					l.flush(&l.logBuffer)
				}
				l.logMu.Unlock()
			}
		}
	}()

	// spawn log workers
	for i := 0; i < workers; i++ {
		go l.logWorker()
	}
	return l
}

// WriteLog appends to the buffered logChan
func (l *Logger[T]) WriteLog(log T) {
	l.logChan <- log
}

func (l *Logger[T]) logWorker() {
	l.wg.Add(1)
	defer l.wg.Done()

	for {
		select {
		case log, ok := <-l.logChan:
			if !ok {
				return
			}
			l.logMu.Lock()
			l.logBuffer = append(l.logBuffer, log)
			// flush if the buffer is atleast half full
			if len(l.logBuffer) >= l.batchSize/2 {
				l.flush(&l.logBuffer)
			}
			l.logMu.Unlock()
		}
	}
}

// flush the buffer to the logfile
func (l *Logger[T]) flush(logBuffer *[]T) {
	l.Lock()
	defer l.Unlock()

	if len(l.logBuffer) == 0 {
		return
	}

	currentDir := l.dirs[l.currentDirIndex]
	if l.currentFile == "" {
		l.currentFile = filepath.Join(currentDir, fmt.Sprintf("%d.log", time.Now().UnixNano()))
		l.currentFileEntryCount = 0
	}

	func() {
		f, err := os.OpenFile(l.currentFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println("Error creating log file:", err)
			return
		}
		defer f.Close()

		encoder := json.NewEncoder(f)
		for _, log := range *logBuffer {
			if err := encoder.Encode(log); err != nil {
				fmt.Println("Error writing log:", err)
				return
			}
			l.currentFileEntryCount++
		}
	}()

	*logBuffer = (*logBuffer)[:0]

	// if the log file exceeds max entries, compress and close the current file
	if l.currentFileEntryCount >= l.maxEntriesPerFile {
		if err := l.compressAndCloseCurrentFile(); err != nil {
			return
		}
	}
}

// compress and close current file
// if the current dir size exceeds, go to the next
// directory and prepare it
func (l *Logger[T]) compressAndCloseCurrentFile() error {
	input, err := os.ReadFile(l.currentFile)
	if err != nil {
		return err
	}

	compressedFile := l.currentFile + ".zst"
	output, err := os.Create(compressedFile)
	if err != nil {
		return err
	}
	defer output.Close()

	writer, err := zstd.NewWriter(output)
	if err != nil {
		return err
	}

	if _, err := writer.Write(input); err != nil {
		writer.Close()
		return err
	}
	writer.Close()

	compressedSize := getFileSize(compressedFile)
	os.Remove(l.currentFile)
	l.currentFile = ""
	l.currentFileEntryCount = 0

	l.currentCompressedSize += compressedSize
	if l.currentCompressedSize >= l.maxSize {
		closeDirectory(l.dirs[l.currentDirIndex])
		l.currentDirIndex = (l.currentDirIndex + 1) % len(l.dirs)
		l.currentCompressedSize = 0
		prepareDirectory(l.dirs[l.currentDirIndex])
	}

	return nil
}

// ReadLogs reads the logs in descending order of the directory
// sorted in reverse by filename (timestamp)
func (l *Logger[T]) ReadLogs(limit int) ([]T, error) {
	l.Lock()
	defer l.Unlock()

	var logs []T

	currentDirIndex := l.currentDirIndex
	dirs := make([]string, len(l.dirs))
	copy(dirs, l.dirs)

	for i := 0; i < len(dirs); i++ {
		dir := dirs[(currentDirIndex-i+len(dirs))%len(dirs)]
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		sort.Sort(sort.Reverse(byName(entries)))

		for _, entry := range entries {
			filePath := filepath.Join(dir, entry.Name())
			var fileLogs []T

			if err := readLogFile(filePath, &fileLogs, limit-len(logs)); err == nil {
				logs = append(logs, fileLogs...)
			}

			if len(logs) >= limit {
				return logs[:limit], nil
			}
		}
	}
	return logs, nil
}

type byName []os.DirEntry

func (b byName) Len() int           { return len(b) }
func (b byName) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byName) Less(i, j int) bool { return b[i].Name() > b[j].Name() }

// readLogFile will read the logs from the file
// if the file is compressed, it will decompress, else it will
// read raw from the file uncompressed
func readLogFile[T LogEntry](filePath string, logs *[]T, limit int) error {
	var reader io.Reader
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if filepath.Ext(filePath) == ".zst" {
		zstdReader, err := zstd.NewReader(file)
		if err != nil {
			return err
		}
		defer zstdReader.Close()
		reader = zstdReader
	} else {
		reader = file
	}

	decoder := json.NewDecoder(reader)
	for decoder.More() {
		if len(*logs) >= limit {
			break
		}
		var log T
		if err := decoder.Decode(&log); err != nil {
			return err
		}
		*logs = append(*logs, log)
	}
	return nil
}

// removes the lock file
func closeDirectory(dir string) {
	os.Remove(filepath.Join(dir, "dir.lock"))
}

// prepareDirectory prepares the directory by removing the
// dentries and creates the lock file
func prepareDirectory(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		os.Remove(filepath.Join(dir, entry.Name()))
	}
	lockFilePath := filepath.Join(dir, "dir.lock")
	// Create the lock file using os.O_CREATE | os.O_EXCL to prevent overwrites
	lockFile, err := os.OpenFile(lockFilePath, os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	lockFile.Close()
	return nil
}

// getCurrentDirInfo will stat the dentries and returns the size
// of the compressed sizes in the directory and also returns the current file
func getCurrentDirInfo(dir string) (size int64, currentFile string, err error) {
	var entries []os.DirEntry
	entries, err = os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if filepath.Ext(path) == ".zst" {
			size += getFileSize(filepath.Join(dir, entry.Name()))
		} else {
			currentFile = path
		}
	}
	return
}

// getFileSize gets the current file size
func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// Stop closes the logChan and waits for go routines to settle
func (l *Logger[T]) Stop() {
	close(l.logChan)
	l.wg.Wait()
}

func main() {
	dirs := []string{"/tmp/data1", "/tmp/data2", "/tmp/data3"}
	for _, dir := range dirs {
		os.MkdirAll(dir, 0755)
	}

	logger := NewLogger[audit.Entry](dirs, 50, 100*1024*1024, 15*time.Minute, 4) // 4 log workers

	http.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		var auditLog audit.Entry
		if err := json.NewDecoder(r.Body).Decode(&auditLog); err != nil {
			http.Error(w, "Failed to parse request body", http.StatusBadRequest)
			return
		}

		logger.WriteLog(auditLog)

		// Respond with success
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		logs, err := logger.ReadLogs(120)
		if err != nil {
			http.Error(w, "Failed to read logs", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(logs)
	})

	port := 8080
	log.Printf("Starting webhook server on :%d...\n", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}

	logger.Stop()
}
