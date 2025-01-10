#include "api.h"

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>

// int req_pipe_fd = -1;
// int resp_pipe_fd = -1;
// int notif_pipe_fd = -1;

char *saved_req_pipe_path = NULL;
char *saved_resp_pipe_path = NULL;
char *saved_notif_pipe_path = NULL;

// Helper function to safely delete existing pipes
int remove_if_exists(const char *pipe_path)
{
  if (access(pipe_path, F_OK) == 0)
  {
    if (unlink(pipe_path) != 0)
    {
      perror("Failed to delete existing pipe");
      return -1;
    }
  }
  return 0;
}

// Helper function to create a named pipe
int create_pipe(const char *pipe_path)
{
  if (mkfifo(pipe_path, 0666) != 0)
  {
    perror("Failed to create pipe");
    return -1;
  }

  return 0;
}

// Helper function to check pipe file descriptors
int check_pipe_fd(int fd)
{
  if (fd == -1)
  {
    perror("Failed to open pipe");
    return -1;
  }
  return 0;
}

// Helper function to handle with requests

// Helper function to handle with responses

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path, char const *notif_pipe_path, char const *server_pipe_path)
{
  // create pipes and connect
  printf("Connecting to server\n");

  // check if pipes already exist
  printf("Check if pipes already exist\n");
  if (remove_if_exists(req_pipe_path) != 0 || remove_if_exists(resp_pipe_path) != 0 || remove_if_exists(notif_pipe_path) != 0)
  {
    return 1;
  }

  // open pipes using mkfifo
  printf("Create pipes using mkfifo\n");
  if (create_pipe(req_pipe_path) != 0 || create_pipe(resp_pipe_path) != 0 || create_pipe(notif_pipe_path) != 0)
  {
    return 1;
  }

  printf("Save file paths\n");
  saved_req_pipe_path = req_pipe_path;
  saved_resp_pipe_path = resp_pipe_path;
  saved_notif_pipe_path = notif_pipe_path;

  // Send connection request to server by server pipe (already initialized)
  printf("Send connection request to server by server pipe: %s\n", server_pipe_path);
  int server_pipe_fd = open(server_pipe_path, O_WRONLY);
  if (server_pipe_fd == -1)
  {
    perror("Failed to open server pipe");
    return 1;
  }

  // Comunicate with server using pipes (Connect to server)
  printf("Communicate with server using pipes\n");
  char buffer[MAX_STRING_SIZE] = "";

  // Pass the fifo path to the server by message buffer
  printf("Pass the fifo path to the server by message pipe\n");
  strcat(buffer, "CONNECT [");
  strcat(buffer, req_pipe_path);
  strcat(buffer, ",");
  strcat(buffer, resp_pipe_path);
  strcat(buffer, ",");
  strcat(buffer, notif_pipe_path);
  strcat(buffer, "]");
  strcat(buffer, "\n");
  buffer[strlen(buffer)] = '\0'; // CONNECT [req_pipe_path,resp_pipe_path,notif_pipe_path]

  printf("End of message buffer\n");
  printf("Message buffer: %s\n", buffer);

  if (write(server_pipe_fd, buffer, strlen(buffer)) < 0)
  {
    perror("Failed to send connection request");
    return 1;
  }

  printf("Connection request sent successfully\n");

  // Open the receive server response to connection request
  printf("Open the receive server response to connection request\n");
  int resp_pipe_fd = open(resp_pipe_path, O_RDONLY);
  if (resp_pipe_fd < 0)
  {
    perror("Failed to open response pipe");
    return 1;
  }

  // Reponse: 0 - Success, 1 - Error
  char response_buffer[MAX_STRING_SIZE] = "";
  printf("Reponse size: %lu\n", sizeof(response_buffer));
  if (read(resp_pipe_fd, &response_buffer, MAX_STRING_SIZE) < 0)
  {
    perror("Failed to read response");
    return 1;
  }

  close(resp_pipe_fd);

  // Check the response from the server
  printf("Check the response from the server\n");
  int response = atoi(response_buffer);
  if (response != 0)
  {
    fprintf(stderr, "Server returned %d for operation: CONNECT\n", response);
    return 1;
  }

  // Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
  printf("Server returned %d for operation: CONNECT\n", response);

  printf("Pipes defined successfully\n");
  return 0;
}

int kvs_disconnect()
{
  printf("Disconnecting from the session\n");
  if (saved_req_pipe_path == NULL || saved_resp_pipe_path == NULL || saved_notif_pipe_path == NULL)
  {
    fprintf(stderr, "Error: Pipes not initialized. Call kvs_connect() first.\n");
    return 1;
  }

  // Open the request pipe for writing
  printf("Opening request pipe for writing\n");
  int req_pipe_fd = open(saved_req_pipe_path, O_WRONLY);
  if (req_pipe_fd < 0)
  {
    perror("Failed to open request pipe");
    return 1;
  }

  printf("Request pipe opened successfully\n");
  // Send the DISCONNECT request
  char request[256];
  snprintf(request, sizeof(request), "DISCONNECT\n");

  printf("Request formatted successfully\n");
  // Write the DISCONNECT request to the request pipe
  ssize_t bytes_written = write(req_pipe_fd, request, strlen(request));
  if (bytes_written < 0)
  {
    perror("Failed to write to request pipe");
    return 1;
  }

  printf("Request written successfully\n");

  // Close the request pipe
  close(req_pipe_fd);
  printf("Request pipe closed successfully\n");

  // Open the response pipe for reading
  printf("Opening response pipe for reading\n");
  int resp_pipe_fd = open(saved_resp_pipe_path, O_RDONLY);
  if (resp_pipe_fd < 0)
  {
    perror("Failed to open response pipe");
    return 1;
  }

  printf("Response pipe opened successfully\n");

  // Interpret the server's response
  char response_buffer[MAX_STRING_SIZE] = "";
  printf("Response size: %lu\n", sizeof(response_buffer));
  if (read(resp_pipe_fd, &response_buffer, MAX_STRING_SIZE) < 0)
  {
    perror("Failed to read response");
    return 1;
  }

  close(resp_pipe_fd);

  // Check the server's response
  printf("Check the response from the server\n");
  int response_status = atoi(response_buffer);
  if (response_status != 0)
  {
    fprintf(stderr, "Server returned %d for operation: DISCONNECT\n", response_status);
    return 1;
  }

  // Unlink (delete) the client's named pipes
  printf("Deleting client named pipes\n");
  if (unlink(saved_req_pipe_path) < 0)
  {
    perror("Failed to delete request pipe");
    return 1;
  }

  if (unlink(saved_resp_pipe_path) < 0)
  {
    perror("Failed to delete response pipe");
    return 1;
  }

  if (unlink(saved_notif_pipe_path) < 0)
  {
    perror("Failed to delete notification pipe");
    return 1;
  }

  // Reset saved paths
  saved_req_pipe_path = NULL;
  saved_resp_pipe_path = NULL;
  saved_notif_pipe_path = NULL;

  printf("Successfully disconnected and cleaned up\n");
  return 0;
}


int kvs_subscribe(const char *key)
{
  printf("Subscribing to key: %s\n", key);
  if (saved_req_pipe_path == NULL || saved_resp_pipe_path == NULL || saved_notif_pipe_path == NULL)
  {
    fprintf(stderr, "Error: Pipes not initialized. Call kvs_connect() first.\n");
    return 1;
  }

  // Open the request pipe for writing
  printf("Opening request pipe for writing\n");
  int req_pipe_fd = open(saved_req_pipe_path, O_WRONLY);
  if (req_pipe_fd < 0)
  {
    perror("Failed to open request pipe");
    return 1;
  }

  printf("Request pipe opened successfully\n");
  // Format the subscription request with a newline
  char request[256];
  snprintf(request, sizeof(request), "SUBSCRIBE %s\n", key);

  printf("Request formatted successfully\n");
  // Write the subscription request to the request pipe
  ssize_t bytes_written = write(req_pipe_fd, request, strlen(request));
  if (bytes_written < 0)
  {
    perror("Failed to write to request pipe");
    return 1;
  }

  printf("Request written successfully\n");

  // Close the request pipe
  close(req_pipe_fd);
  printf("Request pipe closed successfully\n");

  // Open the response pipe for reading
  printf("Opening response pipe for reading\n");
  int resp_pipe_fd = open(saved_resp_pipe_path, O_RDONLY);
  if (resp_pipe_fd < 0)
  {
    perror("Failed to open response pipe");
    return 1;
  }

  printf("Response pipe opened successfully\n");

  // Interpret the server's response
  char response_buffer[MAX_STRING_SIZE] = "";
  printf("Reponse size: %lu\n", sizeof(response_buffer));
  if (read(resp_pipe_fd, &response_buffer, MAX_STRING_SIZE) < 0)
  {
    perror("Failed to read response");
    return 1;
  }

  close(resp_pipe_fd);

  // Check the response from the server
  printf("Check the response from the server\n");
  int response_status = atoi(response_buffer);
  if (response_status != 0)
  {
    fprintf(stderr, "Server returned %d for operation: SUBSCRIBE\n", response_status);
    return 1;
  }

  // Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
  printf("Server returned %d for operation: SUBSCRIBE\n", response_status);

  return 0;
}

int kvs_unsubscribe(const char *key)
{
  printf("Unsubscribing from key: %s\n", key);
  if (saved_req_pipe_path == NULL || saved_resp_pipe_path == NULL || saved_notif_pipe_path == NULL)
  {
    fprintf(stderr, "Error: Pipes not initialized. Call kvs_connect() first.\n");
    return 1;
  }

  // Open the request pipe for writing
  printf("Opening request pipe for writing\n");
  int req_pipe_fd = open(saved_req_pipe_path, O_WRONLY);
  if (req_pipe_fd < 0)
  {
    perror("Failed to open request pipe");
    return 1;
  }

  printf("Request pipe opened successfully\n");
  // Format the unsubscribe request with a newline
  char request[256];
  snprintf(request, sizeof(request), "UNSUBSCRIBE %s\n", key);

  printf("Request formatted successfully\n");
  // Write the unsubscribe request to the request pipe
  ssize_t bytes_written = write(req_pipe_fd, request, strlen(request));
  if (bytes_written < 0)
  {
    perror("Failed to write to request pipe");
    return 1;
  }

  printf("Request written successfully\n");

  // Close the request pipe
  close(req_pipe_fd);
  printf("Request pipe closed successfully\n");

  // Open the response pipe for reading
  printf("Opening response pipe for reading\n");
  int resp_pipe_fd = open(saved_resp_pipe_path, O_RDONLY);
  if (resp_pipe_fd < 0)
  {
    perror("Failed to open response pipe");
    return 1;
  }

  printf("Response pipe opened successfully\n");

  // Interpret the server's response
  char response_buffer[MAX_STRING_SIZE] = "";
  printf("Response size: %lu\n", sizeof(response_buffer));
  if (read(resp_pipe_fd, &response_buffer, MAX_STRING_SIZE) < 0)
  {
    perror("Failed to read response");
    return 1;
  }

  close(resp_pipe_fd);

  // Check the response from the server
  printf("Check the response from the server\n");
  int response_status = atoi(response_buffer);
  if (response_status != 0)
  {
    fprintf(stderr, "Server returned %d for operation: UNSUBSCRIBE\n", response_status);
    return 1;
  }

  // Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
  printf("Server returned %d for operation: UNSUBSCRIBE\n", response_status);

  return 0;
}
