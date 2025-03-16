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

type Logger[T LogEntry] struct {
	dirs []string
	// log buffers
	logBuffer []T
	logMu     sync.Mutex

	// current index
	currentDir            int
	currentFile           string
	currentCompressedSize int64
	currentFileEntryCount int
	maxEntriesPerFile     int

	batchSize     int
	maxSize       int64
	writeInterval time.Duration

	logChan      chan T
	wg           sync.WaitGroup
	writeWorkers int
	sync.RWMutex
}

func NewLogger[T LogEntry](dirs []string, batchSize int, maxSize int64, writeInterval time.Duration, writeWorkers int) *Logger[T] {
	var currentDir int
	var foundLock bool
	var currentCompressedSize int64
	for index, dir := range dirs {
		lockFilePath := filepath.Join(dir, "dir.lock")
		if _, err := os.Stat(lockFilePath); err == nil {
			currentDir = index
			foundLock = true
			currentCompressedSize, err = getCompressedSize(dir)
			if err != nil {
				panic(err)
			}
			break
		}
	}

	l := &Logger[T]{
		dirs:                  dirs,
		batchSize:             batchSize,
		maxSize:               maxSize,
		writeInterval:         writeInterval,
		logChan:               make(chan T, batchSize/2),
		writeWorkers:          writeWorkers,
		logBuffer:             make([]T, 0, batchSize*2),
		maxEntriesPerFile:     100,
		currentDir:            currentDir,
		currentCompressedSize: currentCompressedSize,
	}

	if !foundLock {
		if err := prepareDirectory(l.dirs[l.currentDir]); err != nil {
			panic(err)
		}
	}

	go func() {
		ticker := time.NewTicker(l.writeInterval)
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

	for i := 0; i < writeWorkers; i++ {
		go l.logWorker()
	}
	return l
}

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
			// flush if the buffer is half full
			if len(l.logBuffer) >= l.batchSize/2 {
				l.flush(&l.logBuffer)
			}
			l.logMu.Unlock()
		}
	}
}

func (l *Logger[T]) flush(logBuffer *[]T) {
	l.Lock()
	defer l.Unlock()

	if len(l.logBuffer) == 0 {
		return
	}

	currentDir := l.dirs[l.currentDir]
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

	if l.currentFileEntryCount >= l.maxEntriesPerFile {
		if err := l.compressAndCloseCurrentFile(); err != nil {
			return
		}
	}
}

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
		closeDirectory(l.dirs[l.currentDir])
		l.currentDir = (l.currentDir + 1) % len(l.dirs)
		l.currentCompressedSize = 0
		prepareDirectory(l.dirs[l.currentDir])
	}

	return nil
}

func (l *Logger[T]) ReadLogs(limit int) ([]T, error) {
	l.Lock()
	defer l.Unlock()

	var logs []T

	currentDir := l.currentDir
	dirs := make([]string, len(l.dirs))
	copy(dirs, l.dirs)

	for i := 0; i < len(dirs); i++ {
		dir := dirs[(currentDir-i+len(dirs))%len(dirs)]
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

func closeDirectory(dir string) {
	os.Remove(filepath.Join(dir, "dir.lock"))
}

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

func getCompressedSize(dir string) (size int64, err error) {
	var entries []os.DirEntry
	entries, err = os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		size += getFileSize(filepath.Join(dir, entry.Name()))
	}
	return
}

func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

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
