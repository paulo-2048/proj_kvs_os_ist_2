#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/stat.h>

#include "constants.h"
#include "../common/constants.h"
#include "../common/protocol.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"

struct SharedData
{
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t client_thread_mutex = PTHREAD_MUTEX_INITIALIZER;

volatile sig_atomic_t sigusr1_received = 0;

size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;
char server_pipe_path[256] = "/tmp/server_";

struct client_t
{
  int id;
  char *req_pipe_path;
  char *resp_pipe_path;
  char *notif_pipe_path;
  char *subscriptions[MAX_NUMBER_SUB][MAX_STRING_SIZE]
};

struct client_t clients[MAX_NUMBER_SESSIONS];

int current_client = 0;
int client_id = 0;

struct client_threads
{
  pthread_t thread;
  int id;
  int free;
};

struct client_threads client_threads[MAX_NUMBER_SESSIONS];

void handle_sigusr1(int sig)
{
  printf("Received SIGUSR1 in Handle\n");
  sigusr1_received = 1;
}

int filter_job_files(const struct dirent *entry)
{
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0)
  {
    return 1; // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path)
{
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job"))
  {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE)
  {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

// Notify client about changes in subscribed keys
void format_message(const char *key, const char *value, char *formatted_msg)
{
  // Cria buffers de tamanho fixo para key e value com padding.
  char padded_key[MAX_STRING_SIZE] = {0};
  char padded_value[MAX_STRING_SIZE + 1] = {0};

  // Copia a key e adiciona padding.
  snprintf(padded_key, MAX_STRING_SIZE + 1, "%-40s", key);

  // Copia o value e adiciona padding.
  snprintf(padded_value, MAX_STRING_SIZE + 1, "%-40s", value);

  // Concatena key e value no formato correto.
  snprintf(formatted_msg, 2 * (MAX_STRING_SIZE + 1), "%s%s", padded_key, padded_value);
}

int notify_client(const char *key, const char *value)
{
  char formatted_msg[2 * (MAX_STRING_SIZE + 1)] = {0};
  format_message(key, value, formatted_msg);

  for (int i = 0; i < MAX_NUMBER_SESSIONS; i++)
  {
    if (clients[i].id != -1 && clients[i].subscriptions[0][0] != '\0')
    {
      for (int j = 0; j < MAX_NUMBER_SUB; j++)
      {
        if (clients[i].subscriptions[j][0] != '\0' &&
            strcmp(clients[i].subscriptions[j], key) == 0)
        {
          printf("Notifying client %d about key %s\n", i, key);

          int notif_pipe_fd = open(clients[i].notif_pipe_path, O_WRONLY);
          if (notif_pipe_fd == -1)
          {
            perror("Failed to open notification pipe");
            return 1;
          }
          // Escreve a mensagem formatada no pipe.
          if (write(notif_pipe_fd, formatted_msg, sizeof(formatted_msg)) == -1)
          {
            perror("Failed to write to notification pipe");
            close(notif_pipe_fd);
            return 1;
          }
          close(notif_pipe_fd);
        }
      }
    }
  }
  return 0;
}

static int run_job(int in_fd, int out_fd, char *filename)
{
  size_t file_backups = 0;
  while (1)
  {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd))
    {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0)
      {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values))
      {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }

      // Notify clients about changes in subscribed keys
      for (size_t i = 0; i < num_pairs; i++)
      {
        notify_client(keys[i], values[i]);
      }

      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0)
      {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd))
      {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0)
      {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd))
      {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }

      // Notify clients about delete in subscribed keys
      for (size_t i = 0; i < num_pairs; i++)
      {
        notify_client(keys[i], "DELETED");
      }

      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1)
      {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0)
      {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups)
      {
        wait(NULL);
      }
      else
      {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0)
      {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      }
      else if (aux == 1)
      {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

// frees arguments
static void *get_file(void *arguments)
{
  block_sigusr1();
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0)
  {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL)
  {
    if (entry_files(dir_name, entry, in_path, out_path))
    {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0)
    {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1)
    {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1)
    {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out)
    {
      if (closedir(dir) == -1)
      {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0)
    {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0)
  {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

// Register client
int register_client(char *client_req_pipe_path, char *client_resp_pipe_path, char *client_notif_pipe_path)
{
  pthread_mutex_lock(&client_thread_mutex);
  int allocated_thread = -1;

  // Find a free thread
  while (allocated_thread == -1)
  {
    for (int i = 0; i < MAX_NUMBER_SESSIONS; i++)
    {
      if (client_threads[i].free)
      {
        client_threads[i].free = 0; // Mark thread as allocated
        allocated_thread = i;
        break;
      }
    }

    if (allocated_thread == -1)
    {
      pthread_mutex_unlock(&client_thread_mutex);
      // printf("No free threads available. Waiting...\n");
      sleep(1);
      pthread_mutex_lock(&client_thread_mutex);
    }
  }

  // Register the client
  clients[allocated_thread].id = allocated_thread;
  clients[allocated_thread].req_pipe_path = strdup(client_req_pipe_path);
  clients[allocated_thread].resp_pipe_path = strdup(client_resp_pipe_path);
  clients[allocated_thread].notif_pipe_path = strdup(client_notif_pipe_path);
  if (!clients[allocated_thread].req_pipe_path ||
      !clients[allocated_thread].resp_pipe_path ||
      !clients[allocated_thread].notif_pipe_path)
  {
    printf("Memory allocation failed for pipe paths.\n");
    pthread_mutex_unlock(&client_thread_mutex);
    return 1;
  }

  printf("Client registered successfully on thread %d.\n", allocated_thread);
  printf("Request pipe path: %s\n", clients[allocated_thread].req_pipe_path);
  printf("Response pipe path: %s\n", clients[allocated_thread].resp_pipe_path);
  printf("Notification pipe path: %s\n", clients[allocated_thread].notif_pipe_path);

  if (!clients[allocated_thread].req_pipe_path || !clients[allocated_thread].resp_pipe_path || !clients[allocated_thread].notif_pipe_path)
  {
    printf("Memory allocation failed for pipe paths.\n");
    pthread_mutex_unlock(&client_thread_mutex);
    return 1;
  }

  pthread_mutex_unlock(&client_thread_mutex);
  printf("Client registered successfully on thread %d.\n", allocated_thread);
  return 0; // Success
}

int send_response(const char *pipe_path, char op_code, char status)
{
  if (!pipe_path)
  {
    return -1;
  }

  int fd = open(pipe_path, O_WRONLY);
  if (fd == -1)
  {
    perror("Failed to open response pipe");
    return -1;
  }

  char response[2] = {op_code, status};
  ssize_t bytes_written = write(fd, response, sizeof(response));
  close(fd);

  return (bytes_written == sizeof(response)) ? 0 : -1;
}

static void free_thread(int thread_id)
{
  pthread_mutex_lock(&client_thread_mutex);
  client_threads[thread_id].free = 1; // Mark thread as free
  pthread_mutex_unlock(&client_thread_mutex);
}

void clean_pipes(int thread_id)
{
  pthread_mutex_lock(&client_thread_mutex);

  // Clean pipe paths
  if (clients[thread_id].req_pipe_path)
  {
    unlink(clients[thread_id].req_pipe_path);
    free(clients[thread_id].req_pipe_path);
    clients[thread_id].req_pipe_path = NULL;
  }

  if (clients[thread_id].resp_pipe_path)
  {
    unlink(clients[thread_id].resp_pipe_path);
    free(clients[thread_id].resp_pipe_path);
    clients[thread_id].resp_pipe_path = NULL;
  }

  if (clients[thread_id].notif_pipe_path)
  {
    unlink(clients[thread_id].notif_pipe_path);
    free(clients[thread_id].notif_pipe_path);
    clients[thread_id].notif_pipe_path = NULL;
  }

  pthread_mutex_unlock(&client_thread_mutex);
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

int receive_request(const char *pipe_path, int client_id)
{
  if (!pipe_path)
  {
    fprintf(stderr, "Invalid response pipe path\n");
    return -1;
  }

  // Open the pipe for reading
  int pipe_fd = open(pipe_path, O_RDONLY);
  if (pipe_fd == -1)
  {
    perror("Failed to open pipe");
    return -1;
  }

  char buffer[MAX_STRING_SIZE * 3 + 2] = {0};

  // Read response from the pipe
  ssize_t bytes_read = read(pipe_fd, buffer, sizeof(buffer) - 1);
  if (bytes_read < 0)
  {
    perror("Failed to read response from pipe");
    close(pipe_fd);
    return -1;
  }
  close(pipe_fd);

  buffer[bytes_read] = '\0'; // Null-terminate the buffer
  printf("Raw response: '%s'\n", buffer);

  // Decode and handle the response
  char req_op_code = buffer[0]; // Extract OP_CODE
  int req_op_code_int = req_op_code - '0';

  char args[41] = {0};

  int status = 0;
  char response_status = {0};

  if (client_id == -1) // Hostess request
  {
    req_op_code_int = OP_CODE_CONNECT;

    printf("Request Client: NULL\n");
  }
  else
  {
    printf("Request Client: %d\n", client_id);
  }

  printf("Request OP_CODE: %d\n", req_op_code_int);
  ;

  switch (req_op_code_int)
  {
  case OP_CODE_CONNECT:
    printf("OP_CODE_CONNECT\n");

    char req_pipe_path[PIPE_BUF] = {0};
    char resp_client_pipe_path[PIPE_BUF] = {0};
    char notif_pipe_path[PIPE_BUF] = {0};

    sscanf(buffer, "%*c%40s%40s%40s", req_pipe_path, resp_client_pipe_path, notif_pipe_path);

    // Trim trailing whitespaces
    trim_char(req_pipe_path);
    trim_char(resp_client_pipe_path);
    trim_char(notif_pipe_path);

    printf("Request pipe: %s\n", req_pipe_path);
    printf("Response pipe1: %s\n", resp_client_pipe_path);
    printf("Notification pipe: %s\n", notif_pipe_path);

    // Register the client
    status = register_client(req_pipe_path, resp_client_pipe_path, notif_pipe_path);
    printf("Register client status: %d\n", status);

    // Send a response to the client
    response_status = status == 0 ? '0' : '1';
    if (send_response(resp_client_pipe_path, req_op_code, response_status) == -1)
    {
      printf("Failed to send response to client.\n");
      return 1;
    }
    break;

  case OP_CODE_SUBSCRIBE:
    sscanf(buffer, "%*c%40s", args);
    printf("Key: %s\n", args);
    trim_char(args);
    printf("Subscribing to key: '%s'\n", args);
    if (kvs_check(args) != 0)
    {
      printf("Key %s not exists in KVS table.\n", args);
      status = 1;
    }

    // Check if client subscriptions is full
    int sub_count = 0;
    for (int i = 0; i < MAX_NUMBER_SUB; i++)
    {
      if (clients[client_id].subscriptions[i][0] != '\0')
      {
        sub_count++;
      }
    }

    if (sub_count >= MAX_NUMBER_SUB)
    {
      printf("Client subscriptions is full.\n");
      status = 1;
    }

    // Subscribe to a key
    if (status != 1)
    {
      printf("Subscribing to key: %s\n", args);
      for (int i = 0; i < MAX_NUMBER_SUB; i++)
      {
        if (clients[client_id].subscriptions[i][0] == '\0')
        {
          strcpy(clients[client_id].subscriptions[i], args);
          break;
        }
      }
    }

    // Print the current subscriptions
    printf("Current subscriptions: ");
    for (int i = 0; i < MAX_NUMBER_SUB; i++)
    {
      if (clients[client_id].subscriptions[i][0] != '\0')
      {
        printf("%s ", clients[client_id].subscriptions[i]);
      }
    }
    printf("\n");

    // Send a response to the client
    response_status = status == 0 ? '0' : '1';
    printf("Thread ID: %d\n", client_id);
    printf("Response status: %c\n", response_status);
    printf("Response pipe2: %s\n", clients[client_id].resp_pipe_path);
    if (send_response(clients[client_id].resp_pipe_path, req_op_code, response_status) == -1)
    {
      printf("Failed to send response to client.\n");
      return 1;
    }
    break;

  case OP_CODE_UNSUBSCRIBE:
    // Check if client subscriptions contain the key
    sscanf(buffer, "%*c%40s", args);
    printf("Key: %s\n", args);
    trim_char(args);

    int unsubscribed = 0;
    for (int i = 0; i < MAX_NUMBER_SUB; i++)
    {
      if (strcmp(clients[client_id].subscriptions[i], args) == 0)
      {
        printf("Unsubscribing from key: %s\n", args);
        clients[client_id].subscriptions[i][0] = '\0'; // Clear the subscription
        unsubscribed = 1;
        break;
      }
    }

    if (!unsubscribed)
    {
      printf("Key %s not found in client subscriptions.\n", args);
      status = 1;
    }

    // Print the updated subscriptions
    printf("Updated subscriptions: ");
    for (int i = 0; i < MAX_NUMBER_SUB; i++)
    {
      if (clients[client_id].subscriptions[i][0] != '\0')
      {
        printf("%s ", clients[client_id].subscriptions[i]);
      }
    }
    printf("\n");

    // Send a response to the client
    response_status = status == 0 ? '0' : '1';
    if (send_response(clients[client_id].resp_pipe_path, req_op_code, response_status) == -1)
    {
      printf("Failed to send response to client.\n");
      return 1;
    }
    break;

  case OP_CODE_DISCONNECT:
    // Clean subscriptions
    for (int i = 0; i < MAX_NUMBER_SUB; i++)
    {
      clients[client_id].subscriptions[i][0] = '\0';
    }

    // Send a response to the client
    response_status = '0';
    if (send_response(clients[client_id].resp_pipe_path, req_op_code, response_status) == -1)
    {
      printf("Failed to send response to client.\n");
      return 1;
    }

    // Clean pipes and subscriptions
    printf("Cleaning pipes for client %d\n", client_id);
    printf("Pipes: %s\n", clients[client_id].req_pipe_path);
    clean_pipes(client_id);
    printf("Cleaning pipes successfully for client %d\n", client_id);
    printf("Pipes: %s\n", clients[client_id].req_pipe_path);

    printf("Free manager client %d\n", client_id);
    printf("Free Status: %s\n", client_threads[client_id].free);
    free_thread(client_id);
    printf("Thread manager client %d unregistered\n", client_id);

    break;

  default:
    fprintf(stderr, "Unsupported op_code: %c\n", req_op_code);
    return -1;
  }

  return 0;
}

static void *client_manager_thread(void *arg)
{
  block_sigusr1();

  int thread_id = *(int *)arg;

  while (1)
  {
    // Wait for a client to connect
    while (clients[thread_id].req_pipe_path == NULL)
    {
      sleep(1);
    }

    printf("Thread manager client %d registered\n", thread_id);

    while (clients[thread_id].req_pipe_path)
    {
      printf("Waiting for client request on pipe %s\n", clients[thread_id].req_pipe_path);
      receive_request(clients[thread_id].req_pipe_path, thread_id);
    }
  }

  return NULL;
}

void block_sigusr1(void)
{
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGUSR1);
  if (pthread_sigmask(SIG_BLOCK, &mask, NULL) != 0)
  {
    perror("pthread_sigmask block");
  }
}

static void *hostess_thread(void *arg)
{

  sigset_t mask;
  sigemptyset(&mask);

  printf("Thread hostess pipe started\n");
  printf("Waiting for client connection...\n");
  while (1)
  {
    sigaddset(&mask, SIGUSR1);
    if (pthread_sigmask(SIG_UNBLOCK, &mask, NULL) != 0)
    {
      perror("pthread_sigmask hostess");
      return NULL;
    }

    printf("Waiting for client connection...\n");
    sleep(1);

    printf("sigusr1_received: %d\n", sigusr1_received);
    // Handle SIGUSR1
    if (sigusr1_received)
    {
      printf("SIGUSR1 received in hostess thread\n");

      // Elimina todas as subscrições e encerra os FIFOs
      for (int i = 0; i < MAX_NUMBER_SESSIONS; i++)
      {
        if (client_threads[i].free == 0)
        {
          clean_pipes(i);
          free_thread(i);
        }
      }
      sigusr1_received = 0;
      printf("All clients terminated\n");
    }

    // Receive Request - Register client - Send Response
    receive_request(server_pipe_path, -1);

    printf("Response sent to client\n");
    printf("\n-----------------------\n");
  }

  return NULL;
}

static void dispatch_threads(DIR *dir)
{
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL)
  {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  for (size_t i = 0; i < max_threads; i++)
  {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) !=
        0)
    {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // thread para ler do FIFO de registo (server_pipe_path)
  if (pthread_create(&threads[max_threads], NULL, hostess_thread, NULL) != 0)
  {
    fprintf(stderr, "Failed to create hostess thread\n");
    pthread_mutex_destroy(&thread_data.directory_mutex);
    free(threads);
    return;
  }

  // initiate client manager threads based in MAX_NUMBER_SESSIONS, and pass the to thread id
  for (unsigned int i = 0; i < MAX_NUMBER_SESSIONS; i++)
  {
    client_threads[i].id = i;
    client_threads[i].free = 1;
    if (pthread_create(&client_threads[i].thread, NULL, client_manager_thread, &client_threads[i].id) != 0)
    {
      fprintf(stderr, "Failed to create client manager thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  for (unsigned int i = 0; i < max_threads; i++)
  {
    if (pthread_join(threads[i], NULL) != 0)
    {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0)
  {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}

// initialize the server pipe with mkfifo
int init_server_pipe()
{
  if (access(server_pipe_path, F_OK) == 0)
  {
    if (unlink(server_pipe_path) != 0)
    {
      perror("Failed to delete existing pipe");
      return -1;
    }
  }

  int mkfifo_status = mkfifo(server_pipe_path, 0666);
  if (mkfifo_status == -1)
  {
    perror("mkfifo");
    return 1;
  }
  printf("Server pipe created successfully.\n");

  return 0;
}

int main(int argc, char **argv)
{
  struct sigaction sa;
  sa.sa_handler = handle_sigusr1;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  sigaction(SIGUSR1, &sa, NULL);

  if (argc < 5)
  {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups>");
    write_str(STDERR_FILENO, " <nome_do_server_pipe>\n");
    return 1;
  }

  jobs_directory = argv[1];

  // Absolute path to pipe server: /tmp/server_[pipe]
  strcat(server_pipe_path, argv[4]);

  char *endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0')
  {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0')
  {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0)
  {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0)
  {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  if (kvs_init())
  {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  if (init_server_pipe())
  {
    write_str(STDERR_FILENO, "Failed to initialize server pipe\n");
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  if (dir == NULL)
  {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  dispatch_threads(dir);

  if (closedir(dir) == -1)
  {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0)
  {
    wait(NULL);
    active_backups--;
  }

  // unlink server pipe
  if (unlink(server_pipe_path) != 0)
  {
    perror("Failed to delete server pipe");
    return 1;
  }

  // wait for hostess thread to finish
  pthread_exit(NULL);

  kvs_terminate();

  return 0;
}
