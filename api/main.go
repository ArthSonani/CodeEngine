package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

// SubmissionRequest defines the JSON structure we expect from the frontend

type SubmissionRequest struct {
    ID         string `json:"id"`
    Language   string `json:"language"`
    Code       string `json:"code"`
    WebhookURL string `json:"webhook_url"` // Add this field
}

func main() {
	// 1. Configure the Kafka Producer (Writer)
	// We connect to the localhost port exposed by your Docker container
	writer := &kafka.Writer{
		Addr:                   kafka.TCP("kafka:9092"),
		Topic:                  "code-submissions",
		Balancer:               &kafka.Hash{}, // Ensures same IDs go to the same partition
		WriteTimeout:           10 * time.Second,
		AllowAutoTopicCreation: false, // client-side auto creation
	}
	defer writer.Close()

	// 2. Initialize the Gin HTTP router
	// We use Gin's default mode which includes a logger and crash recovery
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	// 3. Define the /submit endpoint
	router.POST("/submit", func(c *gin.Context) {
		var req SubmissionRequest
		
		// Validate that the incoming request matches our struct
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON or missing fields"})
			return
		}

		// Convert the validated struct back to raw bytes for Kafka
		payloadBytes, err := json.Marshal(req)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize payload"})
			return
		}

		// 4. Construct the Kafka Message
		// The Key is used by the Hash balancer to pick which of the 10 partitions to use.
		msg := kafka.Message{
			Key:   []byte(req.ID),
			Value: payloadBytes,
		}

		// 5. Push to the Queue
		err = writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Printf("Failed to write to Kafka: %v\n", err)
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Queue unavailable. Try again later."})
			return
		}

		log.Printf("Successfully queued submission %s to Kafka", req.ID)

		// 6. Return immediately (Asynchronous)
		// 202 Accepted means "We received the request and will process it in the background"
		c.JSON(http.StatusAccepted, gin.H{
			"status":  "queued",
			"message": "Your code is waiting for an available worker.",
			"id":      req.ID,
		})
	})

	// Start the server on port 8080
	log.Println("API Gateway starting on http://localhost:8080")
	if err := router.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
