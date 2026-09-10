package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"os/signal" // NEW: For catching K8s termination signals
	"sync"      // NEW: For the WaitGroup
        "syscall" // NEW: For SIGTERM types
	"net/http"
	"time" 

	"github.com/segmentio/kafka-go"
	_ "github.com/lib/pq"
)

type SubmissionRequest struct {
	ID       string `json:"id"`
	Language string `json:"language"`
	Code     string `json:"code"`
	WebhookURL string `json:"webhook_url"`
}

type ExecutionResult struct {
	ID     string
	Time   string
	Memory string
	Status string
}

type LanguageConfig struct {
	Extension   string
	IsCompiled  bool
	CompileCmd  []string
	RunCmd      []string
}

var SupportedLanguages = map[string]LanguageConfig{
	"python": {
		Extension:  ".py",
		IsCompiled: false,
		RunCmd:     []string{"/usr/bin/python3", "solution.py"},
	},
	"c": {
		Extension:  ".c",
		IsCompiled: true,
		CompileCmd: []string{"/usr/bin/gcc", "-O2", "solution.c", "-o", "solution"},
		RunCmd:     []string{"./solution"},
	},
	"cpp": {
		Extension:  ".cpp",
		IsCompiled: true,
		CompileCmd: []string{"/usr/bin/g++", "-O2", "solution.cpp", "-o", "solution"},
		RunCmd:     []string{"./solution"},
	},
	"java": {
		Extension:  ".java",
		IsCompiled: true,
		CompileCmd: []string{"/usr/bin/javac", "-J-Xms64m", "-J-Xmx256m", "Solution.java"},
		RunCmd:     []string{"/usr/bin/java", "-Xms64m", "-Xmx256m", "Solution"},
	},
}

func main() {
	// Connect to PostgreSQL
	connStr := "postgres://engine_user:secretpassword@postgres:5432/engine_db?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS submissions (
		id VARCHAR(50) PRIMARY KEY,
		time VARCHAR(20),
		memory VARCHAR(20),
		status VARCHAR(50),
		webhook_url TEXT
	);`
	if _, err := db.Exec(createTableQuery); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Println("Connected to PostgreSQL successfully.")

	// Connect to Kafka
	brokerUrl := os.Getenv("KAFKA_BROKER")
	if brokerUrl == "" {
    		brokerUrl = "kafka:9092" // Fallback if env variable is missing
	}
	
	// Connect to Kafka
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerUrl},
		GroupID:  "isolate-workers",
		Topic:    "code-submissions",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	// =====================================================================
	// KERNEL PATCH: The Cgroups V2 "No Internal Processes" Rule Fix
	// =====================================================================
	fmt.Println("Worker Booted. Applying advanced cgroup v2 routing...")
	
	// 1. Move our Go process out of the root cgroup so the kernel allows propagation
	os.MkdirAll("/sys/fs/cgroup/init", 0755)
	os.WriteFile("/sys/fs/cgroup/init/cgroup.procs", []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644)

	// 2. Create the isolate parent cgroup and the sticky note
	os.MkdirAll("/sys/fs/cgroup/isolate", 0755)
	os.MkdirAll("/run/isolate", 0755)
	os.WriteFile("/run/isolate/cgroup", []byte("/sys/fs/cgroup/isolate\n"), 0644)

	// 3. Now that root is empty, safely propagate controllers downwards
	enableStr := "+cpu +memory +pids\n"
	
	err1 := os.WriteFile("/sys/fs/cgroup/cgroup.subtree_control", []byte(enableStr), 0644)
	if err1 != nil {
		fmt.Printf("Kernel Warning (root subtree): %v\n", err1)
	}
	
	err2 := os.WriteFile("/sys/fs/cgroup/isolate/cgroup.subtree_control", []byte(enableStr), 0644)
	if err2 != nil {
		fmt.Printf("Kernel Warning (isolate subtree): %v\n", err2)
	}

	// 4. Force-clear any zombie kernel locks
	exec.Command("isolate", "--cg", "--cleanup").Run()
	// ===================================================================


	fmt.Println("Listening for submissions...")

	// =====================================================================
        // NEW: GRACEFUL SHUTDOWN SETUP
        // =====================================================================
        var wg sync.WaitGroup
        quit := make(chan os.Signal, 1)
        // Listen for Kubernetes SIGTERM (scale-down) and manual Ctrl+C (SIGINT)
        signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

        // NEW: Move the entire Kafka processing loop into a background Goroutine
        // so the main thread can block on the 'quit' channel below.
        go func() {
                for {
                        msg, err := reader.ReadMessage(context.Background())
                        if err != nil {
                                log.Printf("Error reading from Kafka: %v", err)
                                continue
                        }

                        var req SubmissionRequest
                        json.Unmarshal(msg.Value, &req)

                        fmt.Printf("\n--- Processing Submission: %s (%s) ---\n", req.ID, req.Language)
                        
                        result := processSubmission(req)
                        
                        // NEW: Update query to save the webhook URL state in the DB
                        insertQuery := `
                        INSERT INTO submissions (id, time, memory, status, webhook_url) 
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (id) DO UPDATE 
                        SET time = EXCLUDED.time, memory = EXCLUDED.memory, status = EXCLUDED.status, webhook_url = EXCLUDED.webhook_url;`
                        
                        _, err = db.Exec(insertQuery, result.ID, result.Time, result.Memory, result.Status, req.WebhookURL)
                        if err != nil {
                                log.Printf("Failed to save to DB: %v", err)
                        } else {
                                fmt.Printf("Saved to DB: %+v\n", result)
                        }

                        // =========================================================
                        // NEW: DISPATCH WEBHOOK ASYNCHRONOUSLY
                        // =========================================================
                        if req.WebhookURL != "" {
                                wg.Add(1) // Tell the WaitGroup we have 1 active webhook
                                
                                // Pass result string you want to send back (e.g., Status or raw Output)
                                payload := fmt.Sprintf(`{"id":"%s", "status":"%s"}`, result.ID, result.Status)
                                
                                go dispatchWebhook(result.ID, req.WebhookURL, payload, db, &wg)
                        }
                }
        }()

        // =====================================================================
        // NEW: GRACEFUL SHUTDOWN BLOCKER
        // =====================================================================
        <-quit // The main thread freezes here until Kubernetes sends a Kill signal
        
        fmt.Println("\n[SIGTERM RECEIVED] KEDA is scaling down the worker.")
        fmt.Println("Waiting for active background webhooks to finish dispatching...")
        
        wg.Wait() // The main thread freezes here until the WaitGroup counter hits 0
        
        fmt.Println("All webhooks safely delivered. Worker shutting down.")
}

func processSubmission(req SubmissionRequest) ExecutionResult {
	lang, exists := SupportedLanguages[strings.ToLower(req.Language)]
	if !exists {
		return ExecutionResult{ID: req.ID, Status: "Unsupported Language"}
	}

	boxPathRaw, err := initSandbox()
	if err != nil {
		log.Printf("CRITICAL ERROR: Failed to init isolate: %v\n", err)
		return ExecutionResult{ID: req.ID, Status: "Init Error"}
	}
	defer cleanupSandbox()

	boxPath := strings.TrimSpace(boxPathRaw)
	if !strings.HasSuffix(boxPath, "box") {
		boxPath = boxPath + "/box"
	}

	fileName := "solution" + lang.Extension
	if strings.ToLower(req.Language) == "java" {
		fileName = "Solution.java" 
	}
	
	codePath := boxPath + "/" + fileName
	err = os.WriteFile(codePath, []byte(req.Code), 0777)
	if err != nil {
		log.Printf("CRITICAL ERROR: Failed to write file: %v\n", err)
		return ExecutionResult{ID: req.ID, Status: "Write Error"}
	}
	os.Chmod(codePath, 0777)
	fmt.Printf("File written: %s\n", fileName)

	if lang.IsCompiled {
		fmt.Println("Status: Compiling...")
		err := runIsolate(lang.CompileCmd, 10.0, 500000, "compile_meta.txt")
		if err != nil {
			log.Printf("Compiler exited with error: %v\n", err)
		}
		
		compileResult := parseMetaFile("compile_meta.txt")
		if compileResult.Status != "AC" {
			return ExecutionResult{ID: req.ID, Status: "Compile Error"}
		}
	}

	fmt.Println("Status: Running...")
	runIsolate(lang.RunCmd, 2.0, 256000, "meta.txt")
	
	result := parseMetaFile("meta.txt")
	result.ID = req.ID
	
	return result
}

func initSandbox() (string, error) {
	cmd := exec.Command("isolate", "--cg", "--init")
	var out bytes.Buffer
	var stderrBuf bytes.Buffer 
	
	cmd.Stdout = &out
	cmd.Stderr = &stderrBuf 
	
	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("%v - %s", err, stderrBuf.String()) 
	}
	return out.String(), nil
}

func runIsolate(command []string, timeLimit float64, memLimit int, metaFile string) error {
	isolateArgs := []string{
		"--cg",
		fmt.Sprintf("--time=%f", timeLimit),
		fmt.Sprintf("--wall-time=%f", timeLimit+2.0),
		fmt.Sprintf("--cg-mem=%d", memLimit),
		fmt.Sprintf("--meta=%s", metaFile),
		"--processes=100", // NEW: Allow GCC and Java to fork sub-processes/threads
		"--dir=/etc",  // NEW: Allow Isolate to read /etc/alternatives symlinks for Java
		"--env=PATH", 
		"--run",
		"--",
	}
	
	isolateArgs = append(isolateArgs, command...)

	cmd := exec.Command("isolate", isolateArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func parseMetaFile(filename string) ExecutionResult {
	file, err := os.Open(filename)
	if err != nil {
		return ExecutionResult{Status: "Internal Error"}
	}
	defer file.Close()

	result := ExecutionResult{Status: "AC"}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ":", 2)
		if len(parts) != 2 { continue }
		key, val := parts[0], parts[1]

		switch key {
		case "time": result.Time = val
		case "max-rss": result.Memory = val
		case "status":
			if val == "TO" { result.Status = "TLE" }
			if val == "SG" { result.Status = "MLE / RE" }
			if val == "RE" { result.Status = "RE" }
		}
	}
	return result
}

func cleanupSandbox() {
	exec.Command("isolate", "--cg", "--cleanup").Run()
}

func dispatchWebhook(id, url, result string, db *sql.DB, wg *sync.WaitGroup) {
    defer wg.Done()
    backoff := 2 * time.Second

    for attempt := 1; attempt <= 5; attempt++ {
        db.Exec("UPDATE submissions SET webhook_attempts = $1 WHERE id = $2", attempt, id)
        
        resp, err := http.Post(url, "text/plain", bytes.NewBufferString(result))
        if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
            db.Exec("UPDATE submissions SET webhook_status = 'DELIVERED' WHERE id = $1", id)
            return
        }

        time.Sleep(backoff)
        backoff *= 2
    }
    db.Exec("UPDATE submissions SET webhook_status = 'FAILED' WHERE id = $1", id)
}
