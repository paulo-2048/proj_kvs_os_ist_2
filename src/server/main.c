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
      printf("No free threads available. Waiting...\n");
      sleep(1);
      pthread_mutex_lock(&client_thread_mutex);
    }
  }

  // Register the client
  clients[allocated_thread].id = allocated_thread;
  clients[allocated_thread].req_pipe_path = strdup(client_req_pipe_path);
  clients[allocated_thread].resp_pipe_path = strdup(client_resp_pipe_path);
  clients[allocated_thread].notif_pipe_path = strdup(client_notif_pipe_path);

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

// Handle client requests
// DELAY 5000
// SUBSCRIBE [a]
// SUBSCRIBE [NONE]
// DELAY 10000
// UNSUBSCRIBE [a]
// DISCONNECT

int handle_client_request(int client_id, char *command, char *args)
{
  printf("Client %d: %s %s\n", client_id, command, args);

  if (strcmp(command, "DELAY") == 0)
  {
    int delay = atoi(args);
    if (delay > 0)
    {
      printf("Waiting %d \n", delay);
      // usleep(delay * 1000);
    }
    else
    {
      printf("Invalid delay value\n");
      return 1;
    }

    return 0;
  }
  else if (strcmp(command, "SUBSCRIBE") == 0)
  {
    // Check if the key exists in KVS
    if (kvs_check(args) != 0)
    {
      printf("Key %s not exists in KVS table.\n", args);
      return 1;
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
      return 1;
    }

    // Subscribe to a key
    printf("Subscribing to key: %s\n", args);
    for (int i = 0; i < MAX_NUMBER_SUB; i++)
    {
      if (clients[client_id].subscriptions[i][0] == '\0')
      {
        strcpy(clients[client_id].subscriptions[i], args);
        break;
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

    return 0;
  }
  else if (strcmp(command, "UNSUBSCRIBE") == 0)
  {
    // Check if client subscriptions contain the key
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
      return 1;
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

    return 0;
  }
  else if (strcmp(command, "DISCONNECT") == 0)
  {
    // Clean subscriptions
    for (int i = 0; i < MAX_NUMBER_SUB; i++)
    {
      clients[client_id].subscriptions[i][0] = '\0';
    }

    // Close the client's request pipe
    free(clients[client_id].req_pipe_path);

    clients[client_id].req_pipe_path = NULL;

    return 0;
  }
  else
  {
    printf("Invalid command\n");
    return 1;
  }
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
  free(clients[thread_id].resp_pipe_path);
  free(clients[thread_id].notif_pipe_path);

  clients[thread_id].resp_pipe_path = NULL;
  clients[thread_id].notif_pipe_path = NULL;

  pthread_mutex_unlock(&client_thread_mutex);
}

static void *client_manager_thread(void *arg)
{

  int thread_id = *(int *)arg;

  while (1)
  {
    // Wait for a client to connect
    while (clients[thread_id].req_pipe_path == NULL)
    {
      sleep(1);
    }


    // Wait until clients[thread_id] is not NULL
    while (clients[thread_id].req_pipe_path == NULL)
    {
      sleep(1); // Sleep for 1 second
    }

    printf("Thread manager client %d registered\n", thread_id);

    while (clients[thread_id].req_pipe_path)
    {
      // Open the request pipe for reading
      printf("Thread manager client %d opening request pipe\n", thread_id);
      int req_pipe_fd = open(clients[thread_id].req_pipe_path, O_RDONLY);
      if (req_pipe_fd == -1)
      {
        perror("Failed to open request pipe");
        exit(1);
      }
      printf("Thread manager client %d received request pipe\n", thread_id);

      char buffer[PIPE_BUF] = {0};
      if (read(req_pipe_fd, buffer, PIPE_BUF) == -1)
      {
        perror("Failed to read from request pipe");
        exit(1);
      }

      // Close the request pipe
      close(req_pipe_fd);

      // Get command and arguments from the request pipe
      char command[PIPE_BUF] = {0};
      char args[PIPE_BUF] = {0};

      if (sscanf(buffer, "%s", command) != 1)
      {
        fprintf(stderr, "Failed to parse command\n");
        exit(1);
      }

      // Get the arguments
      char *args_start = strchr(buffer, ' ');
      if (args_start != NULL)
      {
        strcpy(args, args_start + 1);
        // Remove trailing newline if present
        size_t len = strlen(args);
        if (len > 0 && args[len - 1] == '\n')
        {
          args[len - 1] = '\0';
        }
      }
      else
      {
        args[0] = '\0'; // No arguments
      }

      int status = 0;
      // Handle the client request
      if (handle_client_request(clients[thread_id].id, command, args) != 0)
      {
        fprintf(stderr, "Failed to handle client request\n");
        status = 1;
      }

      // Send a response to the client through the response pipe
      int client_resp_fd = open(clients[thread_id].resp_pipe_path, O_WRONLY);
      if (client_resp_fd == -1)
      {
        perror("Failed to open client response pipe");
        exit(1);
      }

      // 0 - Success, 1 - Failure
      char response_status = status == 0 ? '0' : '1';
      if (write(client_resp_fd, &response_status, sizeof(response_status)) == -1)
      {
        perror("Failed to write response");
      }

      close(client_resp_fd);
    }

    // Clean pipes and subscriptions
    clean_pipes(thread_id);

    // Mark thread as free
    free_thread(thread_id);
  }

  return NULL;
}

static void *hostess_thread()
{
  printf("Thread hostess pipe started\n");
  int server_fd = open(server_pipe_path, O_RDONLY);
  if (server_fd == -1)
  {
    perror("Failed to open server pipe");
    exit(1);
  }

  printf("Waiting for client connection...\n");
  while (1)
  {
    int status;
    char buffer[PIPE_BUF] = {0}; // Initialize buffer to zeros

    // Read from the server pipe and check return value
    ssize_t bytes_read = read(server_fd, buffer, PIPE_BUF - 1);
    if (bytes_read <= 0)
    {
      if (bytes_read < 0)
        perror("Error reading from pipe");
      continue;
    }

    buffer[bytes_read] = '\0'; // Null terminate based on actual bytes read

    // Verify the message starts with "CONNECT ["
    if (strncmp(buffer, "CONNECT [", 9) != 0)
    {
      fprintf(stderr, "Invalid message format\n");
      continue;
    }

    // Extract paths from the message
    char req_pipe_path[PIPE_BUF] = {0};
    char resp_pipe_path[PIPE_BUF] = {0};
    char notif_pipe_path[PIPE_BUF] = {0};

    if (sscanf(buffer, "CONNECT [%[^,],%[^,],%[^]]]",
               req_pipe_path, resp_pipe_path, notif_pipe_path) != 3)
    {
      fprintf(stderr, "Failed to parse connection message\n");
      continue;
    }

    printf("Request pipe: %s\n", req_pipe_path);
    printf("Response pipe: %s\n", resp_pipe_path);
    printf("Notification pipe: %s\n", notif_pipe_path);
    printf("Received message from client: %s\n", buffer);

    // Register client
    status = register_client(req_pipe_path, resp_pipe_path, notif_pipe_path);

    // Send a response to the client through the response pipe
    int client_resp_fd = open(resp_pipe_path, O_WRONLY);
    if (client_resp_fd == -1)
    {
      perror("Failed to open client response pipe");
      continue; // Don't exit, just try next connection
    }

    // 0 - Success, 1 - Failure
    char response_status = status == 0 ? '0' : '1';
    if (write(client_resp_fd, &response_status, sizeof(response_status)) == -1)
    {
      perror("Failed to write response");
    }

    close(client_resp_fd);

    printf("Response sent to client\n");
    printf("\n-----------------------\n");
  }

  close(server_fd);
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
