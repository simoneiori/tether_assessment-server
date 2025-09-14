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

#include "server.h"
#include "queue.h"
#include "json.h"
#include "transform.h"



client_t *clients[MAX_CLIENTS];
pthread_mutex_t clients_registry_lock = PTHREAD_MUTEX_INITIALIZER;



/**
 * @brief Utils method to make sure that directories exist
 *
 * @param path The complete filepath
 * @return A pointer to the first char of filename
 */
void check_dirs() {
    struct stat st;
    if (stat(UPLOAD_DIR, &st) == -1) mkdir(UPLOAD_DIR, 0755);
    if (stat(OUT_DIR, &st) == -1) mkdir(OUT_DIR, 0755);
}

/**
 * @brief Utils method to get the filename from the complete filepath
 *
 * @param path The complete filepath
 * @return A pointer to the first char of filename
 */
const char * get_filename(const char * path) {
	const char *base = strrchr(path, '/');
	base = base ? base + 1 : path;
	return base;
}

/**
 * @brief Get the complete filepath for a file on the server
 *
 * @param out The output string which stores the filepath
 * @param outSize The size of the output buffer
 * @param filename The string containing the filename
 * @param id The client ID (which is stored in filename)
 * @return Nothing
 */
void get_path_on_server(char * out, int outSize, const char * filename, int id) {
	snprintf(out, outSize, "%s/upload_c%d_%s", UPLOAD_DIR, id, filename);
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
	char buff[64];
    format_json_long(buff, sizeof(buff), "filesize", (long)st.st_size);

    send(fd, buff, strlen(buff), 0);
    send(fd, "\n", 1, 0);


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
		char buff[64];
        work_item_t *it = pq_pop_blocking();
        if (!it) continue;

        client_t *c = get_client_by_id(it->client_fd);
        if (!c) { free(it); continue; }

        //Wait until upload is complete
        while (c->pendingUpload == 1)
        {
        	//Not so elegant, we need another solution...
        }

		char pathOnServer[FILENAME_SIZE + 30];
		get_path_on_server(pathOnServer, sizeof(pathOnServer), it->filename, c->id);

        // Process
        switch (it->transform) {

			case TR_CHECKSUM:
			{
				unsigned long chk = calc_checksum(pathOnServer);
				format_json_string_long(buff, sizeof(buff), "result", "checksum", "value", chk);
			    send(c->ctrl_fd, buff, strlen(buff), 0);
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
				format_json_string(buff, sizeof(buff), "result", "file");
			    send(c->ctrl_fd, buff, strlen(buff), 0);
				// ...and send via data channel
				send_file(c->data_fd, outpath);
				break;
			}

			default:
			case TR_UNKNOWN:
			{
				//send error message
				format_json_string(buff, sizeof(buff), "status", "UNKNOWN_OP");
			    send(c->ctrl_fd, buff, strlen(buff), 0);
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

		char pathOnServer[FILENAME_SIZE + 30];
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
		char buff[64];
		format_json_string(buff, sizeof(buff), "status", "UPLOAD_COMPLETE");
	    send(c->ctrl_fd, buff, strlen(buff), 0);

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
	char reply[64];

    int n = recv(ctrl_fd, buf, sizeof(buf)-1, 0);
    if (n <= 0) return -1;

    buf[n] = 0;
    client_t *c = get_client_by_ctrlfd(ctrl_fd);
    if (!c) return -1;

    char cmd[10];
    parse_str_field(buf, "cmd", cmd, sizeof(cmd));


    if (strcmp(cmd, "request") != 0) {
    	format_json_string(reply, sizeof(reply), "status", "UNKNOWN_CMD");
	    send(c->ctrl_fd, reply, strlen(reply), 0);
        return -1;
    }

	pthread_mutex_lock(&c->lock);

	if (c->pendingJob) {
		pthread_mutex_unlock(&c->lock);
		format_json_string(reply, sizeof(reply), "status", "BUSY");
	    send(c->ctrl_fd, reply, strlen(reply), 0);
		return -1;
	}

	c->pendingJob = 1;

	pthread_mutex_unlock(&c->lock);


	// parse fields
	char filepath[FILENAME_SIZE]; parse_str_field(buf, "filename", filepath, sizeof(filepath));
	transform_type_t tr = parse_transform_field(buf);
	int pr = parse_int_field(buf, "priority");

	if (tr == TR_UNKNOWN) {
    	format_json_string(reply, sizeof(reply), "status", "UNKNOWN_OP");
	    send(c->ctrl_fd, reply, strlen(reply), 0);
        return -1;
	}


	// map filename to saved upload path
	const char *fileName = get_filename(filepath);

	char pathOnServer[FILENAME_SIZE + 30];
	get_path_on_server(pathOnServer, sizeof(pathOnServer), fileName, c->id);

	// create work item using required name and push to queue
	work_item_t *it = malloc(sizeof(*it));
	it->priority = pr;
	it->client_fd = c->id; 		// client index
	c->pendingUpload = 1;
	strncpy(it->filename, fileName, sizeof(it->filename)-1);
	it->transform = tr;
	pq_push(it);

	format_json_string(reply, sizeof(reply), "status", "ENQUEUED");
    send(c->ctrl_fd, reply, strlen(reply), 0);

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

                // set connection as non blocking
                int flags = fcntl(cfd, F_GETFL, 0);
                fcntl(cfd, F_SETFL, flags | O_NONBLOCK);

                int cid = register_client(cfd);
                if (cid < 0) { close(cfd); continue; }

                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = cfd;
                epoll_ctl(efd, EPOLL_CTL_ADD, cfd, &ev);

                // send assigned id to client
				char buff[64];
                format_json_long(buff, sizeof(buff), "client_id", cid);
                send(cfd, buff, strlen(buff), 0);

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
