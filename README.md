# Concurrent Terminal-Based Chat System (Go)

A high-performance, thread-safe concurrent chat system built in Go, demonstrating real-time message broadcasting using Goroutines, Channels, and Mutexes.

## Features
- Concurrent Server Core: Handles multiple client state transitions non-blockingly.
- Thread-Safety: Utilizes sync.Mutex to prevent race conditions during state reads and writes.
- Asynchronous Broadcasting: Non-blocking message dispatch via buffered Go channels.
- Graceful Shutdown: Handles system interrupts (`SIGINT`/`SIGTERM`) to clean up client channels and WaitGroups safely.

## Architecture & Go Concepts Used
- Goroutines: Background event loops for the server and asynchronous notification workers for each client.
- Channels: Unbuffered control channels for state actions (`joinChan`, leaveChan, `msgChan`) and buffered channels for individual client message queues.
- Sync Package: sync.Mutex for map synchronization and sync.WaitGroup to track active client loops during termination.

## How to Run

1. Clone the repository:
   ```bash
   git clone [https://github.com/YOUR_USERNAME/chat-app.git](https://github.com/YOUR_USERNAME/chat-app.git)
   cd chat-app