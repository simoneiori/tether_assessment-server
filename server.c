/**
 * @file server.c
 * @brief Multithreaded File Processing Server with Priority Queues
 *
 *
 * Sample usage:
 *   - ./server -threads 4 -port 8080
 *
 * @author Simone Iori
 * @date 14.09.2025
 */

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "jsmn.h"

#define MAX_CONN 128
#define MAX_CLIENTS 1024
#define MAX_EVENTS 64
#define UPLOAD_DIR "uploads"
#define OUT_DIR "processed"
#define PRIO_NUM 3
#define BUF_SIZE 		8192
#define FILENAME_SIZE 	256

#define MIN_THREADS  4
#define MAX_THREADS 10


// supported transform types
typedef enum {
	TR_UNKNOWN,
	TR_NOP,					// No operation, only for testing
	TR_UPPER,
	TR_REVERSE,
	TR_CHECKSUM
} transform_type_t;

typedef struct {
    int priority;
    int client_fd;              		// control fd of client who made the request
    char filename[FILENAME_SIZE];       // filename of the uploaded file
    transform_type_t transform;
} work_item_t;

// Priority queue
typedef struct pq_node {
    work_item_t *it;
    struct pq_node *next;
} pq_node_t;

typedef struct {
    pq_node_t *heads[PRIO_NUM];
    pq_node_t *tails[PRIO_NUM];
} priority_queue_t;

// Client structure to maintain control+data sockets and pending flags
typedef struct {
    int id;
    int ctrl_fd;
    int data_fd;    		// data channel fd, set when client uploads
    int pendingJob;    		// 0 = no outstanding job, 1 = job pending
    int pendingUpload;    	// 0 = no upload running, 1 = upload is ongoing
    pthread_mutex_t lock;
} client_t;


// Global variables
pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;
priority_queue_t *work_queue = NULL;

client_t *clients[MAX_CLIENTS];
pthread_mutex_t clients_registry_lock = PTHREAD_MUTEX_INITIALIZER;


static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Make sure directories exist
static void check_dirs() {
    struct stat st;
    if (stat(UPLOAD_DIR, &st) == -1) mkdir(UPLOAD_DIR, 0755);
    if (stat(OUT_DIR, &st) == -1) mkdir(OUT_DIR, 0755);
}

const char * get_filename(const char * path) {
	const char *base = strrchr(path, '/');
	base = base ? base + 1 : path;
	return base;
}

void get_path_on_server(char * out, int outSize, const char * filename, int id) {
	snprintf(out, outSize, "%s/upload_c%d_%s", UPLOAD_DIR, id, filename);
}


// Priority queue functions

/**
 * @brief Initialize the queue
 *
 * @return Nothing
 */
void pq_init() {
    work_queue = malloc(sizeof(*work_queue));
    for (int i = 0; i < PRIO_NUM; i++) {
		work_queue->heads[i] = work_queue->tails[i] = NULL;
	}
}

/**
 * @brief Push an item inside the queue
 *
 * Item are loaded in the pertinent queue according
 * to their priority.
 *
 * @param it Pointer to the item to be added
 * @return Nothing
 */
void pq_push(work_item_t *it) {
    if (!it) return;

    if (it->priority < 1) it->priority = 1;
    if (it->priority > PRIO_NUM) it->priority = PRIO_NUM;

    pq_node_t *n = malloc(sizeof(*n));
    n->it = it; n->next = NULL;

    pthread_mutex_lock(&queue_lock);

    if (!work_queue->heads[it->priority - 1]) {
		work_queue->heads[it->priority - 1] = n;
	}
    else {
		work_queue->tails[it->priority - 1]->next = n;
	}
	work_queue->tails[it->priority - 1] = n;

    pthread_cond_signal(&queue_cond);
    pthread_mutex_unlock(&queue_lock);
}

/**
 * @brief Pop the next item from the queue
 *
 * It implements a simple priority scheme,
 * servicing first the items in the high priority queue.
 *
 * Note: the method is blocking: if no item is available,
 * execution stalls until an item is loaded.
 *
 * @return A pointer to the popped item
 */
work_item_t *pq_pop_blocking() {
    pthread_mutex_lock(&queue_lock);

    while (1) {
        for (int p = 0; p < PRIO_NUM; p++) {					//Pop items according to their priority

            if (work_queue->heads[p]) {
                pq_node_t *n = work_queue->heads[p];
                work_queue->heads[p] = n->next;
                if (!work_queue->heads[p]) {
					work_queue->tails[p] = NULL;
				}
                work_item_t *it = n->it;
                free(n);
                pthread_mutex_unlock(&queue_lock);
                return it;
            }
        }
        pthread_cond_wait(&queue_cond, &queue_lock);
    }
}


/**
 * @brief Register a new client connection
 *
 * The function registers a client, assigning a distinct ID.
 * It is called as soon as the server receives an incoming connection.
 * Client ID is kept until connection is closed or after an
 * unrecoverable error.
 *
 * Note: clients are allocated without a particular order.
 *
 * @param ctrl_fd The file descriptor of the control channel
 * @return The index of the allocated client on success, -1 on error
 */
int register_client(int ctrl_fd) {

    pthread_mutex_lock(&clients_registry_lock);

    for (int i = 0; i < MAX_CLIENTS; i++) {

        if (!clients[i]) {

            client_t *c = malloc(sizeof(*c));
            c->id = i;
			c->ctrl_fd = ctrl_fd;
			c->data_fd = -1;
			c->pendingJob = 0;
			c->pendingUpload = 0;
            pthread_mutex_init(&c->lock, NULL);

            clients[i] = c;
            pthread_mutex_unlock(&clients_registry_lock);
            return i;
        }
    }

    pthread_mutex_unlock(&clients_registry_lock);
    return -1;
}

/**
 * @brief Get the client from its id
 *
 * @param id The id of the desired client
 * @return A pointer to the client object, 0 on error
 */
client_t *get_client_by_id(int id) {

    if (id < 0 || id >= MAX_CLIENTS) return NULL;

    return clients[id];
}

/**
 * @brief Get the client from its file descriptor
 *
 * @param id The descriptor of the desired client
 * @return A pointer to the client object, 0 on error
 */
client_t *get_client_by_ctrlfd(int fd) {

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] && clients[i]->ctrl_fd == fd) return clients[i];
    }
    return NULL;
}

/**
 * @brief Unregister a client connection
 *
 * The function removes the specified client from the array,
 * cleaning up allocated memory.
 *
 * @param id The id of the client to be removed
 * @return Nothing
 */
void unregister_client(int id) {

    pthread_mutex_lock(&clients_registry_lock);

    if (clients[id]) {
        close(clients[id]->ctrl_fd);

        if (clients[id]->data_fd >= 0) {
			close(clients[id]->data_fd);
		}

        pthread_mutex_destroy(&clients[id]->lock);

        free(clients[id]);
        clients[id] = NULL;
    }

    pthread_mutex_unlock(&clients_registry_lock);
}

#define MIN(a,b)	( (a) < (b) ? (a) : (b) )
#define MAX(a,b)	( (a) > (b) ? (a) : (b) )

/**
 * @brief Parse a string value from a JSON string
 *
 * It looks for the provided key inside the JSON string,
 * and return the found value up to 'outsz' characters.
 *
 * @param s Pointer to the entire JSON string
 * @param key Key we are looking for
 * @param out Pointer to the parse value string, if found. If not found is NULL
 * @param outsz Size of the output buffer
 * @return Nothing
 */
static void parse_str_field(const char *s, const char *key, char *out, size_t outsz) {

	jsmn_parser parser;
	jsmntok_t tokens[20]; 		// Array to hold JSON tokens

    jsmn_init(&parser);
    int ret = jsmn_parse(&parser, s, strlen(s), tokens, 30);

	const char *valPtr;
    int valLen;

    for (int i = 0; i < ret - 1; i++) {
	   if ((tokens[i].type == JSMN_STRING) && (tokens[i + 1].type == JSMN_STRING)) {
		   const char *keyPtr = s + tokens[i].start;
		   int keyLen = tokens[i].end - tokens[i].start;

		   if (strncmp(keyPtr, key, keyLen) == 0) {

			   valPtr = s + tokens[i + 1].start;
			   valLen = tokens[i + 1].end - tokens[i + 1].start;

			   int maxLen = MIN(outsz - 1, valLen);
			   strncpy (out, valPtr, maxLen);
			   out[maxLen] = '\0';
			   return;
		   }
	   }
    }

    out[0] = '\0';
}

/**
 * @brief Parse an integer value from a JSON string
 *
 * It looks for the provided key inside the JSON string,
 * and return the found value.
 *
 * @param s Pointer to the entire JSON string
 * @param key Key we are looking for
 * @return The parse value on success, -1 if value is not found.
 */
static int parse_int_field(const char *s, const char *key) {

	jsmn_parser parser;
	jsmntok_t tokens[20]; 		// Array to hold JSON tokens

    jsmn_init(&parser);
    int ret = jsmn_parse(&parser, s, strlen(s), tokens, 30);

    for (int i = 0; i < ret - 1; i++) {
	   if ((tokens[i].type == JSMN_STRING) && (tokens[i + 1].type == JSMN_PRIMITIVE)) {
		   const char *keyPtr = s + tokens[i].start;
		   int keyLen = tokens[i].end - tokens[i].start;

		   if (strncmp(keyPtr, key, keyLen) == 0) {
			   const char *valPtr = s + tokens[i + 1].start;
			   return strtol (valPtr, NULL, 10);
		   }
	   }
    }

    return -1;
}


/**
 * @brief Decode the received transform string
 *
 * It converts the string to a more manageable enum
 *
 * @param s Pointer to the string
 * @return The enum value if operation is supported, otherwise TR_UNKNOWN
 */
static transform_type_t parse_transform_field(const char *s) {

    char tmp[10];
    parse_str_field(s, "transform", tmp, sizeof(tmp));

    if (strcmp(tmp, "uppercase") == 0) return TR_UPPER;
    if (strcmp(tmp, "reverse") == 0) return TR_REVERSE;
    if (strcmp(tmp, "checksum") == 0) return TR_CHECKSUM;
    if (strcmp(tmp, "nop") == 0) return TR_NOP;

    return TR_UNKNOWN;
}

// Elaborations on files

/**
 * @brief Calculates the checksum on the provided file
 *
 * The function works on the provided filepath, loading the
 * file from the disk and calculating the checksum.
 *
 * @param path Filepath of input file
 * @return The calculated checksum, 0 in case of errors
 */
unsigned long calc_checksum(const char *path) {

    unsigned long sum = 0;

    FILE *f = fopen(path, "rb");
    if (!f) return 0;

    unsigned char buf[4096];
    size_t r;
    while ((r = fread(buf, 1, sizeof(buf), f)) > 0) {
        for (size_t i = 0; i < r; i++) sum += buf[i];
    }

    fclose(f);
    return sum;
}

/**
 * @brief Converts the content of a file to uppercase
 *
 * @param in Filepath of input file
 * @param out Filepath of output file
 * @return Nothing
 */
void transform_upper(const char *in, const char *out) {

    FILE *fi = fopen(in, "rb");
    FILE *fo = fopen(out, "wb");

    if (!fi || !fo) { if (fi) fclose(fi); if (fo) fclose(fo); return; }

    int c;
    while ((c = fgetc(fi)) != EOF) fputc(toupper(c), fo);

    fclose(fi); fclose(fo);
}

/**
 * @brief Reverts the content of a file
 *
 * @param in Filepath of input file
 * @param out Filepath of output file
 * @return Nothing
 */
void transform_reverse_file(const char *in, const char *out) {

    FILE *fi = fopen(in, "rb");
    FILE *fo = fopen(out, "wb");

    if (!fi || !fo) { if (fi) fclose(fi); if (fo) fclose(fo); return; }

    fseek(fi, 0, SEEK_END);
    long sz = ftell(fi);
    for (long pos = sz - 1; pos >= 0; pos--) {
        fseek(fi, pos, SEEK_SET);
        int c = fgetc(fi);
        fputc(c, fo);
    }

    fclose(fi); fclose(fo);
}

/**
 * @brief Dummy operation
 *
 * It simply copies the file without changing it
 *
 * @param in Filepath of input file
 * @param out Filepath of output file
 * @return Nothing
 */
void transform_nop_file(const char *in, const char *out) {

    FILE *fi = fopen(in, "rb");
    FILE *fo = fopen(out, "wb");

    if (!fi || !fo) { if (fi) fclose(fi); if (fo) fclose(fo); return; }

    int c;
    while ((c = fgetc(fi)) != EOF) fputc(c, fo);

    fclose(fi); fclose(fo);
}


// TODO The following methods could be optimized with a variadic function

/**
 * @brief Send JSON string (1 string value)
 *
 * Format the JSON string according to the provided parameters,
 * and send it on the socket.
 *
 * @param fd Descriptor of the socket
 * @param key JSON key
 * @param val JSON value
 * @return Nothing
 */
void send_json_string(int fd, const char *key, const char *val) {

	char buff[64];
	snprintf(buff, sizeof(buff), "{\"%s\":\"%s\"}", key, val);

    send(fd, buff, strlen(buff), 0);
}

/**
 * @brief Send JSON string (1 longint value)
 *
 * Format the JSON string according to the provided parameters,
 * and send it on the socket.
 *
 * @param fd Descriptor of the socket
 * @param key JSON key
 * @param val JSON value
 * @return Nothing
 */
void send_json_long(int fd, const char *key, long val) {

	char buff[64];
	snprintf(buff, sizeof(buff), "{\"%s\":%ld}", key, val);

    send(fd, buff, strlen(buff), 0);
}

/**
 * @brief Send JSON string (1 string value + 1 longint value)
 *
 * Format the JSON string according to the provided parameters,
 * and send it on the socket.
 *
 * @param fd Descriptor of the socket
 * @param key1 JSON key
 * @param val1 JSON value
 * @param key2 JSON key
 * @param val2 JSON value
 * @return Nothing
 */
void send_json_string_long(int fd, const char *key1, const char *val1, const char *key2, long val2) {

	char buff[64];
	snprintf(buff, sizeof(buff), "{\"%s\":\"%s\",\"%s\":%ld}", key1, val1, key2, val2);

    send(fd, buff, strlen(buff), 0);
}


/**
 * @brief Send a file
 *
 * Load the file from the provided path and send it to the provided
 * socket. An JSON header containing the filesize is sent before the raw bytes.
 *
 * @param fd Descriptor of the socket
 * @param path Filepath of the file
 * @return Nothing
 */
void send_file(int fd, const char *path) {

    struct stat st;
    if (stat(path, &st) != 0) return;

    // Send the header
    send_json_long(fd, "filesize", (long)st.st_size);    send(fd, "\n", 1, 0);


    // Send the raw bytes
    FILE *f = fopen(path, "rb");
    if (!f) return;

    char buf[BUF_SIZE];
    size_t r;
    while ((r = fread(buf, 1, sizeof(buf), f)) > 0) {
        ssize_t w = send(fd, buf, r, 0);
        if (w <= 0) break;
    }
    fclose(f);
}

/**
 * @brief Processing thread
 *
 * The thread which performs the elaboration on the enqueued items.
 * It pops from the queue getting a pointer to the work_item_t.
 * It waits for the end of file upload before starting working.
 * After the elaboration, it destroys the item.
 *
 * @param arg Arguments passed to thread (not used)
 * @return Nothing
 */
void *worker_thread(void *arg) {

    (void)arg;

    while (1) {
        work_item_t *it = pq_pop_blocking();
        if (!it) continue;

        client_t *c = get_client_by_id(it->client_fd);
        if (!c) { free(it); continue; }

        //Wait until upload is complete
        while (c->pendingUpload == 1)
        {
        	//Not so elegant, we need another solution...
        }

		char pathOnServer[FILENAME_SIZE + 20];
		get_path_on_server(pathOnServer, sizeof(pathOnServer), it->filename, c->id);

        // Process
        switch (it->transform) {

			case TR_CHECKSUM:
			{
				unsigned long chk = calc_checksum(pathOnServer);
				send_json_string_long(c->ctrl_fd, "result", "checksum", "value", chk);
				break;
			}

			case TR_UPPER:
			case TR_REVERSE:
			case TR_NOP:
			{
				// build output path
				char outpath[FILENAME_SIZE + 20];
				time_t t = time(NULL);

				snprintf(outpath, sizeof(outpath), "%s/out_c%d_%lu_%s", OUT_DIR, c->id, (unsigned long)t, it->filename);

				//Simulate a heavy elaboration
				sleep(5);

				if (it->transform == TR_UPPER) {
					transform_upper(pathOnServer, outpath);
				}
				else if (it->transform == TR_REVERSE) {
					transform_reverse_file(pathOnServer, outpath);
				}
				else {
					transform_nop_file(pathOnServer, outpath);
				}

				// notify on ctrl channel...
				send_json_string(c->ctrl_fd, "result", "file");
				// ...and send via data channel
				send_file(c->data_fd, outpath);
				break;
			}

			default:
			case TR_UNKNOWN:
			{
				//send error message
				send_json_string(c->ctrl_fd, "status", "UNKNOWN_OP");
				break;
			}
        }
        // clear pending flag
        pthread_mutex_lock(&c->lock);
        c->pendingJob = 0;
        pthread_mutex_unlock(&c->lock);
        free(it);
    }
    return NULL;
}

/**
 * @brief Data listener thread
 *
 * It accepts data connections, reads a single JSON header line then reads file bytes.
 * The header is a single line JSON: {"client_id":N,"filename":"name","filesize":123}
 * Received file in saved in UPLOAD_DIR.
 * The data socket is kept open in client->data_fd for sending results later.
 *
 * @param arg The port where the thread listen for connections
 * @return Nothing
 */
void *data_listener(void *arg) {

    int data_port = *(int*)arg;

    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
	if (listenfd < 0) { perror("Error while opening listenfd"); exit(1); }

    int on = 1;
	setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
	addr.sin_port = htons(data_port);
	addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listenfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("Error while binding data"); exit(1); }

    if (listen(listenfd, MAX_CONN) < 0) { perror("Error while listening on listenfd"); exit(1); }

    while (1) {

        int fd = accept(listenfd, NULL, NULL);
        if (fd < 0) continue;

        // read header line
        char hdr[128];
		int pos = 0;

        while (pos < (int)sizeof(hdr) - 1) {
            int r = recv(fd, hdr + pos, 1, 0);
            if (r <= 0) break;
            if (hdr[pos] == '\n') { hdr[pos] = 0; break; }
            pos += r;
        }
        if (pos == 0) { close(fd); continue; }

        int client_id = parse_int_field(hdr, "client_id");
        char fname[FILENAME_SIZE]; parse_str_field(hdr, "filename", fname, sizeof(fname));
        int filesize = parse_int_field(hdr, "filesize");

        if (client_id < 0 || filesize <= 0 || fname[0] == '\0') { close(fd); continue; }

        client_t *c = get_client_by_id(client_id);
        if (!c) { close(fd); continue; }

		const char *fileName = get_filename(fname);

		char pathOnServer[FILENAME_SIZE + 20];
		get_path_on_server(pathOnServer, sizeof(pathOnServer), fileName, client_id);

        FILE *f = fopen(pathOnServer, "wb");
        if (!f) { close(fd); continue; }

        int remaining = filesize;
        char buf[BUF_SIZE];
        while (remaining > 0) {
            int want = remaining < (int)sizeof(buf) ? remaining : (int)sizeof(buf);
            int r = recv(fd, buf, want, 0);
            if (r <= 0) break;
            fwrite(buf, 1, r, f);
            remaining -= r;
        }
        fclose(f);

        // attach data socket to client for future sends
        pthread_mutex_lock(&c->lock);
        if (c->data_fd >= 0) close(c->data_fd); // replace existing one, if any
        c->data_fd = fd;
        c->pendingUpload = 0;
        pthread_mutex_unlock(&c->lock);


        // notify via ctrl channel the end of operation
		send_json_string(c->ctrl_fd, "status", "UPLOAD_COMPLETE");

        // loop to accept next connection
    }
    return NULL;
}


/**
 * @brief Handler for incoming connections
 *
 * It manages commands received on control socket.
 * Protocol:
 *  - After accept, server sends {"client_id":N}
 *  - Client sends JSON commands like: {"cmd":"request","filename":"name","transform":"uppercase","priority":2}
 *  - Server responds with:
 *  	-ENQUEUED: the command has been accepted and enqueued, client can proceed uploading the file on data channel.
 *  	-UNKNOWN_CMD: the command is not supported (at the moment, only "request" is supported).
 *  	-BUSY: command refused because there is already an ongoing elaboration for this client.
 *   	-UNKNOWN_OP: the requested transform is not supported.
 *
 *  Note: Control connection is persistent.
 *
 * @param ctrl_fd The descriptor of control channel
 * @return 0 on success, -1 in case of errors
 */
int handle_control_message(int ctrl_fd) {

    char buf[FILENAME_SIZE + 128];

    int n = recv(ctrl_fd, buf, sizeof(buf)-1, 0);
    if (n <= 0) return -1;

    buf[n] = 0;
    client_t *c = get_client_by_ctrlfd(ctrl_fd);
    if (!c) return -1;

    char cmd[10];
    parse_str_field(buf, "cmd", cmd, sizeof(cmd));

    if (strcmp(cmd, "request") != 0) {
    	send_json_string(c->ctrl_fd, "status", "UNKNOWN_CMD");
        return -1;
    }

	pthread_mutex_lock(&c->lock);

	if (c->pendingJob) {
		pthread_mutex_unlock(&c->lock);
		send_json_string(c->ctrl_fd, "status", "BUSY");
		return -1;
	}

	c->pendingJob = 1;

	pthread_mutex_unlock(&c->lock);


	// parse fields
	char filepath[FILENAME_SIZE]; parse_str_field(buf, "filename", filepath, sizeof(filepath));
	transform_type_t tr = parse_transform_field(buf);
	int pr = parse_int_field(buf, "priority");

	if (tr == TR_UNKNOWN) {
    	send_json_string(c->ctrl_fd, "status", "UNKNOWN_OP");
        return -1;
	}


	// map filename to saved upload path
	const char *fileName = get_filename(filepath);

	char pathOnServer[FILENAME_SIZE + 20];
	get_path_on_server(pathOnServer, sizeof(pathOnServer), fileName, c->id);

	// create work item using required name and push to queue
	work_item_t *it = malloc(sizeof(*it));
	it->priority = pr;
	it->client_fd = c->id; 		// client index
	c->pendingUpload = 1;
	strncpy(it->filename, fileName, sizeof(it->filename)-1);
	it->transform = tr;
	pq_push(it);

	send_json_string(c->ctrl_fd, "status", "ENQUEUED");

	return 0;
}


/**
 * @brief main
 *
 * It initializes variables, checks the environment and the arguments.
 * It then creates the thread pool according to the requested number of working threads,
 * plus an additional thread to manage the data upload.
 * Finally, it opens the listening socket on the specified port.
 *
 */
int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    int thread_pool_size = 4;
    int port = 8080;

    // parse args
    for (int i = 1; i < argc; i++) {

    	if (strcmp(argv[i], "-threads") == 0 && i + 1 < argc) thread_pool_size = atoi(argv[++i]);
    	else if (strcmp(argv[i], "-port") == 0 && i + 1 < argc) port = atoi(argv[++i]);
        else { fprintf(stderr, "Unknown arg %s\n", argv[i]); return 1; }

	}

    if (thread_pool_size < MIN_THREADS)	thread_pool_size = MIN_THREADS;
    if (thread_pool_size > MAX_THREADS)	thread_pool_size = MAX_THREADS;


    check_dirs();
    pq_init();

    // spawn worker threads
    pthread_t *wth = calloc(thread_pool_size, sizeof(pthread_t));
    for (int i = 0; i < thread_pool_size; i++) {
		pthread_create(&wth[i], NULL, worker_thread, NULL);
	}

    // start data listener thread on port+1
    pthread_t dt;
    int data_port = port + 1;
    pthread_create(&dt, NULL, data_listener, &data_port);


    // create control listening socket and use epoll to accept/manage many clients
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
	if (listenfd < 0) { perror("opening listenfd"); return 1; }

    int on = 1;
	setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listenfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("binding data"); return 1; }

    if (listen(listenfd, MAX_CONN) < 0) { perror("listening on listenfd"); return 1; }

    int efd = epoll_create1(0);
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = listenfd;
    epoll_ctl(efd, EPOLL_CTL_ADD, listenfd, &ev);

    struct epoll_event events[MAX_EVENTS];

    printf("Server listening with %d threads (control port %d, data port %d)\n", thread_pool_size, port, data_port);

    while (1) {

        int n = epoll_wait(efd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait"); break;
        }

        for (int i = 0; i < n; i++) {

            if (events[i].data.fd == listenfd) {

                // new connection
                int cfd = accept(listenfd, NULL, NULL);
                if (cfd < 0) continue;

                set_nonblocking(cfd);

                int cid = register_client(cfd);
                if (cid < 0) { close(cfd); continue; }

                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = cfd;
                epoll_ctl(efd, EPOLL_CTL_ADD, cfd, &ev);

                // send assigned id to client
                send_json_long(cfd, "client_id", cid);

            } else {

                int fd = events[i].data.fd;

                if (events[i].events & EPOLLIN) {

                    if (handle_control_message(fd) < 0) {

                        // client closed or error
                        client_t *c = get_client_by_ctrlfd(fd);

                        if (c) {
                            unregister_client(c->id);
                        } else close(fd);

                        epoll_ctl(efd, EPOLL_CTL_DEL, fd, NULL);
                    }
                }
            }
        }
    }

    return 0;
}
