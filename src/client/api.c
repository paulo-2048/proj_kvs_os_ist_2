#include "api.h"

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

// int req_pipe_fd = -1;
// int resp_pipe_fd = -1;
// int notif_pipe_fd = -1;

const char *saved_server_pipe_path = NULL;
const char *saved_req_pipe_path = NULL;
const char *saved_resp_pipe_path = NULL;
const char *saved_notif_pipe_path = NULL;

// Helper function to safely delete existing pipes
int remove_if_exists(char *pipe_path)
{
  if (check_pipe_path(pipe_path) == 0)
  {
    if (unlink(pipe_path) != 0)
    {
      perror("Failed to delete existing pipe");
      return -1;
    }
  }
  return 0;
}

int check_pipe_path(char *pipe_path)
{
  if (access(pipe_path, F_OK) != 0)
  {
    return 1;
  }

  return 0;
}

// Helper function to create a named pipe
int create_pipe(char *pipe_path)
{
  if (mkfifo(pipe_path, 0666) != 0)
  {
    perror("Failed to create pipe");
    return -1;
  }

  return 0;
}

// Helper function to print message in console
// Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
int log_message(char op_code, char op_status)
{
  char *operation;

  // OP_CODE=1 - CONNECT
  // OP_CODE=2 - DISCONNECT
  // OP_CODE=3 - SUBSCRIBE
  // OP_CODE=4 - UNSUBSCRIBE

  switch (op_code)
  {
  case '1':
    operation = "CONNECT";
    break;
  case '2':
    operation = "DISCONNECT";
    break;
  case '3':
    operation = "SUBSCRIBE";
    break;
  case '4':
    operation = "UNSUBSCRIBE";
    break;
  default:
    printf("Raw response: '%c' - '%c'\n", op_code, op_status);
    operation = "UNKNOWN";
    break;
  }

  printf("Server returned %c for operation: %s\n", op_status, operation);
  return 0;
}

// *Helpers to handle client-server communication
// Helper function to send requests to the server
// Utility function for padding strings to MAX_STRING_SIZE
// Safely pad a string to a fixed size, ensuring null termination
// Safely pad a string to a fixed size, ensuring null termination
void pad_string(char *dest, const char *src, size_t max_size)
{
  if (!dest || !src || max_size == 0)
  {
    return;
  }

  size_t len = strlen(src);
  if (len >= max_size)
  {
    len = max_size - 1; // Leave room for null terminator
  }

  memcpy(dest, src, len);
  memset(dest + len, ' ', max_size - len); // Pad with spaces instead of nulls
}

// Helper function to send requests to the server
int send_request(int op_code, const char *key)
{
  // Validate inputs
  if (op_code < 0)
  {
    fprintf(stderr, "Invalid op_code: %d\n", op_code);
    return 1;
  }

  if ((op_code == OP_CODE_SUBSCRIBE || op_code == OP_CODE_UNSUBSCRIBE) && !key)
  {
    fprintf(stderr, "Key required for subscribe/unsubscribe operations\n");
    return 1;
  }

  // Determine pipe path
  const char *pipe_path = (op_code == OP_CODE_CONNECT)
                              ? saved_server_pipe_path
                              : saved_req_pipe_path;

  // Prepare buffer
  const size_t max_buffer_size = MAX_STRING_SIZE * 3 + 2;
  char buffer[max_buffer_size];
  memset(buffer, ' ', max_buffer_size); // Initialize with spaces

  buffer[0] = '0' + op_code;
  size_t offset = 1; // Start after op_code

  // Build message based on operation type
  switch (op_code)
  {
  case OP_CODE_CONNECT:
    pad_string(buffer + offset, saved_req_pipe_path, MAX_STRING_SIZE);
    offset += MAX_STRING_SIZE;
    pad_string(buffer + offset, saved_resp_pipe_path, MAX_STRING_SIZE);
    offset += MAX_STRING_SIZE;
    pad_string(buffer + offset, saved_notif_pipe_path, MAX_STRING_SIZE);
    offset += MAX_STRING_SIZE;
    break;

  case OP_CODE_SUBSCRIBE:
  case OP_CODE_UNSUBSCRIBE:
    pad_string(buffer + offset, key, MAX_STRING_SIZE);
    offset += MAX_STRING_SIZE;
    break;

  case OP_CODE_DISCONNECT:
    break;

  default:
    fprintf(stderr, "Unsupported op_code: %d\n", op_code);
    return 1;
  }

  // Check if pipe exists (if not is because server closed)
  if (check_pipe_path(pipe_path) != 0)
  {
    fprintf(stderr, "Pipe not found (closed by server) : %s\n", pipe_path);
    exit(1);
  }

  // Open and write to pipe
  int pipe_fd = open(pipe_path, O_WRONLY);
  if (pipe_fd == -1)
  {
    perror("Failed to open pipe");
    return 1;
  }

  ssize_t bytes_written = write(pipe_fd, buffer, offset);
  int result = 0;

  if (bytes_written < 0 || (size_t)bytes_written != offset)
  {
    perror("Failed to write complete request");
    result = 1;
  }
  else
  {
    // printf("Request sent to server\n");
    // Print raw request with all characters (including spaces)
    // printf("Raw request: '%s\n'", buffer);
  }

  close(pipe_fd);

  return result;
}

// Helper function to receive responses from the server
// Server always return 2 characters response code (OP_CODE + OP_STATUS)
int receive_response()
{
  char res_op_code;
  char res_op_status;

  if (check_pipe_path(saved_resp_pipe_path) != 0)
  {
    fprintf(stderr, "Pipe not found (closed by server) : %s\n", saved_resp_pipe_path);
    exit(1);
  }

  // Open the receive server response to connection request
  // printf("Open the receive server response to connection request\n");
  int resp_pipe_fd = open(saved_resp_pipe_path, O_RDONLY);
  if (resp_pipe_fd < 0)
  {
    perror("Failed to open response pipe");
    return 1;
  }

  // Response buffer for OP_CODE and OP_STATUS (2 characters + null terminator)
  char response_buffer[3] = "";
  ssize_t bytes_read = read(resp_pipe_fd, response_buffer, 2); // Expect 2 bytes
  if (bytes_read < 0)
  {
    perror("Failed to read response");
    close(resp_pipe_fd);
    return 1;
  }
  else if (bytes_read < 2)
  {
    fprintf(stderr, "Incomplete response received. Expected 2 bytes.\n");
    close(resp_pipe_fd);
    return 1;
  }

  close(resp_pipe_fd);

  // Null-terminate the response to ensure safety
  response_buffer[2] = '\0';

  // Check the response from the server
  // printf("Check the response from the server\n");
  // printf("Raw response: %s\n", response_buffer);

  // First character is OP_CODE
  res_op_code = response_buffer[0];
  // Second character is OP_STATUS
  res_op_status = response_buffer[1];

  // printf("OP_CODE: %c\n", res_op_code);
  // printf("OP_STATUS: %c\n", res_op_status);

  // Log the response
  if (log_message(res_op_code, res_op_status) != 0)
  {
    perror("Failed to log server response message");
    return 1;
  }

  return 0;
}

void trim_char(char *str)
{
  size_t str_value = strlen(str);
  while (str_value > 0 && str[str_value - 1] == ' ')
  {
    str_value--;
    str[str_value] = '\0';
  }
}

// Thread function to handle with notifications from the server
void parse_notification(char *message, char *key, char *value)
{

  // Debug message
  // printf("Raw message received: '%s'\n", message);

  // Copia os 40 primeiros caracteres para a chave.
  strncpy(key, message, MAX_STRING_SIZE);
  key[MAX_STRING_SIZE] = '\0'; // Garante a terminação.

  // Trim key
  trim_char(key);

  // Copia os próximos 40 caracteres para o valor.
  strncpy(value, message + MAX_STRING_SIZE, MAX_STRING_SIZE);
  value[MAX_STRING_SIZE] = '\0'; // Garante a terminação.

  // Trim value
  trim_char(value);
}

void *notification_handler(void *arg)
{
  while (1)
  {
    char buffer[2 * (MAX_STRING_SIZE + 1)] = {0};

    if (check_pipe_path(saved_notif_pipe_path) != 0)
    {
      fprintf(stderr, "Pipe not found (closed by server) : %s\n", saved_notif_pipe_path);
      exit(1);
    }

    // printf("Waiting for notification...\n");
    int notif_pipe_fd = open(saved_notif_pipe_path, O_RDONLY);
    if (notif_pipe_fd == -1)
    {
      perror("Failed to open notification pipe");
      return NULL;
    }

    ssize_t bytes_read = read(notif_pipe_fd, buffer, sizeof(buffer) - 1);
    if (bytes_read <= 0)
    {
      if (bytes_read == 0)
      {
        // EOF: O servidor fechou o pipe.
        perror("Notification pipe closed by server.\n");
      }
      else
      {
        perror("Failed to read from notification pipe");
      }
      close(notif_pipe_fd);
      return NULL;
    }

    // Garante que o buffer seja sempre terminado.
    buffer[bytes_read] = '\0';

    // Separa chave e valor.
    char key[MAX_STRING_SIZE + 1] = {0};
    char value[MAX_STRING_SIZE + 1] = {0};
    parse_notification(buffer, key, value);

    // Exibe a notificação formatada.
    // printf("\n------- NOTIFICATION -------\n");
    // printf("Key: '%s'\n", key);
    // printf("Value: '%s'\n", value);
    // printf("---------------------------\n\n");

    // <chave>,<valor>)
    printf("(%s,%s)\n", key, value);

    close(notif_pipe_fd);
  }

  return NULL;
}

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path, char const *notif_pipe_path, char const *server_pipe_path)
{
  // create pipes and connect
  // printf("Connecting to server\n");

  // check if pipes already exist
  // printf("Check if pipes already exist\n");
  if (remove_if_exists(req_pipe_path) != 0 || remove_if_exists(resp_pipe_path) != 0 || remove_if_exists(notif_pipe_path) != 0)
  {
    return 1;
  }

  // open pipes using mkfifo
  // printf("Create pipes using mkfifo\n");
  if (create_pipe(req_pipe_path) != 0 || create_pipe(resp_pipe_path) != 0 || create_pipe(notif_pipe_path) != 0)
  {
    return 1;
  }

  // printf("Save file paths\n");
  saved_server_pipe_path = server_pipe_path;
  saved_req_pipe_path = req_pipe_path;
  saved_resp_pipe_path = resp_pipe_path;
  saved_notif_pipe_path = notif_pipe_path;

  // Send connection request to server by server pipe (already initialized)
  // printf("Send connection request to server by server pipe\n");
  if (send_request(OP_CODE_CONNECT, NULL) != 0)
  {
    perror("Failed to send connection request");
    return 1;
  }

  if (receive_response() != 0)
  {
    perror("Failed to receive response");
    return 1;
  }

  // Open the notification pipe for reading
  // printf("Open the notification pipe for reading\n");
  pthread_t *notification_thread;
  if (pthread_create(&notification_thread, NULL, notification_handler, NULL) != 0)
  {
    perror("Failed to create notification thread");
    return 1;
  }

  // printf("Pipes defined successfully\n");
  return 0;
}

int kvs_disconnect()
{

  if (send_request(OP_CODE_DISCONNECT, NULL) != 0)
  {
    perror("Failed to send disconnect request");
    return 1;
  }

  // Check the server's response
  // printf("Check the response from the server\n");
  if (receive_response() != 0)
  {
    perror("Failed to receive response");
    return 1;
  }

  // Unlink (delete) the client's named pipes
  // printf("Deleting client named pipes\n");
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

  // printf("Successfully disconnected and cleaned up\n");
  return 0;
}

int kvs_subscribe(const char *key)
{
  if (send_request(OP_CODE_SUBSCRIBE, key) != 0)
  {
    perror("Failed to send connection request");
    return 1;
  }

  if (receive_response() != 0)
  {
    perror("Failed to receive response");
    return 1;
  }

  return 0;
}

int kvs_unsubscribe(const char *key)
{
  if (send_request(OP_CODE_UNSUBSCRIBE, key) != 0)
  {
    perror("Failed to send connection request");
    return 1;
  };

  if (receive_response() != 0)
  {
    perror("Failed to receive response");
    return 1;
  }

  return 0;
}

// void sigusr1(int signal)
// {
// printf("Received SIGUSR1\n");

//   if (saved_req_pipe_path == NULL || saved_resp_pipe_path == NULL || saved_notif_pipe_path == NULL)
//   {
//     fprintf(stderr, "Error: Pipes not initialized. Call kvs_connect() first.\n");
//     return;
//   }

//   if (remove_if_exists(saved_req_pipe_path) != 0 ||
//       remove_if_exists(saved_resp_pipe_path) != 0 ||
//       remove_if_exists(saved_notif_pipe_path) != 0)
//   {
//     perror("Failed to delete pipes");
//   }

//   saved_req_pipe_path = NULL;
//   saved_resp_pipe_path = NULL;
//   saved_notif_pipe_path = NULL;

// printf("Successfully disconnected all clients\n");
// }