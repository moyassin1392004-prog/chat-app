package main

import (
 "bufio"
 "fmt"
 "os"
 "os/signal"
 "strings"
 "sync"
 "syscall"
)

type Client struct {
 Username string
 Incoming chan string
 Done     chan struct{}
}

type Message struct {
 Sender  string
 Content string
}

type Server struct {
 mu        sync.Mutex
 clients   map[string]*Client
 joinChan  chan *Client
 leaveChan chan *Client
 msgChan   chan Message
 shutdown  chan struct{}
 wg        sync.WaitGroup
}

func NewServer() *Server {
 return &Server{
  clients:   make(map[string]*Client),
  joinChan:  make(chan *Client),
  leaveChan: make(chan *Client),
  msgChan:   make(chan Message),
  shutdown:  make(chan struct{}),
 }
}

func (s *Server) Run() {
 for {
  select {
  case client := <-s.joinChan:
   s.mu.Lock()]
   s.clients[client.Username] = client
   s.mu.Unlock()
   s.broadcast(fmt.Sprintf("User %s joined the chat.", client.Username), client.Username)

  case client := <-s.leaveChan:
   s.mu.Lock()
   if _, exists := s.clients[client.Username]; exists {
    delete(s.clients, client.Username)
    close(client.Done)
    s.broadcast(fmt.Sprintf("User %s left the chat.", client.Username), client.Username)
   }
   s.mu.Unlock()

  case msg := <-s.msgChan:
   formattedMsg := fmt.Sprintf("[%s]: %s", msg.Sender, msg.Content)
   s.broadcast(formattedMsg, msg.Sender)

  case <-s.shutdown:
   s.mu.Lock()
   for name, client := range s.clients {
    delete(s.clients, name)
    close(client.Done)
   }
   s.mu.Unlock()
   return
  }
 }
}

func (s *Server) broadcast(msg string, excludeUser string) {
 s.mu.Lock()
 defer s.mu.Unlock()

 for name, client := range s.clients {
  if name != excludeUser {
   select {
   case client.Incoming <- msg:
   default:
   }
  }
 }
}

func (s *Server) AddClient(username string) error {
 s.mu.Lock()
 if _, exists := s.clients[username]; exists {
  s.mu.Unlock()
  return fmt.Errorf("username '%s' is already taken", username)
 }

 client := &Client{
  Username: username,
  Incoming: make(chan string, 10),
  Done:     make(chan struct{}),
 }
 s.mu.Unlock()

 s.wg.Add(1)
 go func(c *Client) {
  defer s.wg.Done()
  for {
   select {
   case msg := <-c.Incoming:
    fmt.Printf("\n[NOTIFICATION for %s] %s\n> ", c.Username, msg)
   case <-c.Done:
    return
   }
  }
 }(client)

 s.joinChan <- client
 return nil
}

func (s *Server) RemoveClient(username string) error {
 s.mu.Lock()
 client, exists := s.clients[username]
 s.mu.Unlock()

 if !exists {
  return fmt.Errorf("user '%s' not found", username)
 }

 s.leaveChan <- client
 return nil
}

func (s *Server) SendMessage(sender, content string) error {
 s.mu.Lock()
 _, exists := s.clients[sender]
 s.mu.Unlock()

 if !exists {
  return fmt.Errorf("active user '%s' is not connected", sender)
 }

 s.msgChan <- Message{Sender: sender, Content: content}
 return nil
}

func (s *Server) ListUsers() []string {
 s.mu.Lock()
 defer s.mu.Unlock()

 users := make([]string, 0, len(s.clients))
 for name := range s.clients {
  users = append(users, name)
 }
 return users
}

func (s *Server) Stop() {
 close(s.shutdown)
 s.wg.Wait()
}

func main() {
 server := NewServer()
 go server.Run()

 sigChan := make(chan os.Signal, 1)
 signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
 go func() {
  <-sigChan
  fmt.Println("\nShutting down server...")
  server.Stop()
  os.Exit(0)
 }()

 scanner := bufio.NewScanner(os.Stdin)
 var currentUser string

 for {
  fmt.Println("\n=== CONCURRENT CHAT MENU ===")
  if currentUser != "" {
   fmt.Printf("Active User: [%s]\n", currentUser)
  } else {
   fmt.Println("Active User: [None Selected]")
  }
  fmt.Println("1. Create User")
  fmt.Println("2. List Connected Users")
  fmt.Println("3. Select Active User")
  fmt.Println("4. Send Message")
  fmt.Println("5. Remove User")
  fmt.Println("6. Exit")
  fmt.Print("> Select an option: ")

  if !scanner.Scan() {
   break
  }
  choice := strings.TrimSpace(scanner.Text())

  switch choice {
  case "1":
   fmt.Print("Enter username: ")
   if scanner.Scan() {
    username := strings.TrimSpace(scanner.Text())
    if username == "" {
     fmt.Println("Username cannot be empty.")
 continue
    }
    err := server.AddClient(username)
    if err != nil {
     fmt.Printf("Error: %v\n", err)
    } else {
     fmt.Printf("User '%s' created successfully.\n", username)
     if currentUser == "" {
      currentUser = username
     }
    }
   }

  case "2":
   users := server.ListUsers()
   fmt.Println("\nConnected Users:")
   if len(users) == 0 {
    fmt.Println(" - No users online.")
   } else {
    for _, u := range users {
     fmt.Printf(" - %s\n", u)
    }
   }

  case "3":
   users := server.ListUsers()
   if len(users) == 0 {
    fmt.Println("No users available to select.")
    continue
   }
   fmt.Print("Enter username to select: ")
   if scanner.Scan() {
    selected := strings.TrimSpace(scanner.Text())
    server.mu.Lock()
    _, exists := server.clients[selected]
    server.mu.Unlock()

    if exists {
     currentUser = selected
     fmt.Printf("Active user set to '%s'.\n", currentUser)
    } else {
     fmt.Println("User not found.")
    }
   }

  case "4":
   if currentUser == "" {
    fmt.Println("Please select or create an active user first.")
    continue
   }
   fmt.Printf("Enter message as '%s': ", currentUser)
   if scanner.Scan() {
    content := strings.TrimSpace(scanner.Text())
    if content != "" {
     err := server.SendMessage(currentUser, content)
     if err != nil {
      fmt.Printf("Error: %v\n", err)
     }
    }
   }

  case "5":
   fmt.Print("Enter username to remove: ")
   if scanner.Scan() {
    toRemove := strings.TrimSpace(scanner.Text())
    err := server.RemoveClient(toRemove)
    if err != nil {
     fmt.Printf("Error: %v\n", err)
    } else {
     fmt.Printf("User '%s' removed.\n", toRemove)
     if currentUser == toRemove {
      currentUser = ""
     }
    }
   }

  case "6":
   fmt.Println("Exiting...")
   server.Stop()
   return

  default:
   fmt.Println("Invalid choice, please try again.")
  }
 }
}