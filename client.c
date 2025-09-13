/*
 server.c


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
#define BUFSZ 8192
#define PRIO_NUM 3


jsmn_parser parser;
jsmntok_t tokens[20]; // Array to hold tokens


/* transform types */
typedef enum { TR_UNKNOWN, TR_UPPER, TR_REVERSE, TR_CHECKSUM } transform_type_t;

typedef struct {
    int priority;
    int client_fd;              /* control fd of client who requested */
    char filename[256];         /* server-side saved uploaded file path */
    transform_type_t transform;
} work_item_t;

/* Priority queue */
typedef struct pq_node {
    work_item_t *it;
    struct pq_node *next;
} pq_node_t;

typedef struct {
    pq_node_t *heads[PRIO_NUM];
    pq_node_t *tails[PRIO_NUM];
} priority_queue_t;

/* Global variables */
pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;
priority_queue_t *work_queue = NULL;

/* Client structure to maintain control+data sockets and pending flag */
typedef struct {
    int id;
    int ctrl_fd;
    int data_fd;    		/* data channel fd, set when client uploads */
    int pendingJob;    		/* 0 = no outstanding job, 1 = job pending */
    int pendingUpload;    	/*  */
    pthread_mutex_t lock;
} client_t;

client_t *clients[MAX_CLIENTS];
pthread_mutex_t clients_registry_lock = PTHREAD_MUTEX_INITIALIZER;


static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* Make sure directories exist */
static void check_dirs() {
    struct stat st;
    if (stat(UPLOAD_DIR, &st) == -1) mkdir(UPLOAD_DIR, 0755);
    if (stat(OUT_DIR, &st) == -1) mkdir(OUT_DIR, 0755);
}

/* Priority queue functions */
void pq_init() {
    work_queue = malloc(sizeof(*work_queue));
    for (int i = 0; i < PRIO_NUM; i++) {
		work_queue->heads[i] = work_queue->tails[i] = NULL;
	}
}
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

/* Client registration */
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

client_t *get_client_by_id(int id) {

    if (id < 0 || id >= MAX_CLIENTS) return NULL;

    return clients[id];
}

client_t *get_client_by_ctrlfd(int fd) {

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] && clients[i]->ctrl_fd == fd) return clients[i];
    }
    return NULL;
}

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

static void parse_str_field(const char *s, const char *key, char *out, size_t outsz) {

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

static int parse_int_field(const char *s, const char *key) {

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

static transform_type_t parse_transform_field(const char *s) {

    char tmp[64];
    parse_str_field(s, "transform", tmp, sizeof(tmp));

    if (strcmp(tmp, "uppercase") == 0) return TR_UPPER;
    if (strcmp(tmp, "reverse") == 0) return TR_REVERSE;
    if (strcmp(tmp, "checksum") == 0) return TR_CHECKSUM;

    return TR_UNKNOWN;
}

/* File transforms */
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
void transform_upper(const char *in, const char *out) {
    FILE *fi = fopen(in, "rb");
    FILE *fo = fopen(out, "wb");
    if (!fi || !fo) { if (fi) fclose(fi); if (fo) fclose(fo); return; }

    int c;
    while ((c = fgetc(fi)) != EOF) fputc(toupper(c), fo);

    fclose(fi); fclose(fo);
}
void transform_reverse_file(const char *in, const char *out) {
    FILE *fi = fopen(in, "rb");
    FILE *fo = fopen(out, "wb");
    if (!fi || !fo) { if (fi) fclose(fi); if (fo) fclose(fo); return; }

    fseek(fi, 0, SEEK_END);
    long sz = ftell(fi);
    for (long pos = sz - 1; pos >= 0; --pos) {
        fseek(fi, pos, SEEK_SET);
        int c = fgetc(fi);
        fputc(c, fo);
    }

    fclose(fi); fclose(fo);
}

/* Send JSON reply on control socket */
void send_json_ctrl(int fd, const char *key, const char *val) {

	char buff[64];
	snprintf(buff, sizeof(buff), "{\"%s\":\"%s\"}", key, val);

    send(fd, buff, strlen(buff), 0);
}
void send_json_ctrl2(int fd, const char *key, const char *val, const char *key2, const char *val2) {

	char buff[64];
	snprintf(buff, sizeof(buff), "{\"%s\":\"%s\",\"%s\":\"%s\"}", key, val, key2, val2);

    send(fd, buff, strlen(buff), 0);
}
void send_json_ctrl3(int fd, const char *key, const char *val, const char *key2, int val2) {

	char buff[64];
	snprintf(buff, sizeof(buff), "{\"%s\":\"%s\",\"%s\":%d}", key, val, key2, val2);

    send(fd, buff, strlen(buff), 0);
}

/* Send file over data_fd stored in client */
void send_file_via_data(client_t *c, const char *path) {

    if (!c || c->data_fd < 0) return;

    struct stat st;
    if (stat(path, &st) != 0) return;

    char header[128];
    int n = snprintf(header, sizeof(header), "{\"filesize\":%ld}\n", (long)st.st_size);

    /* Also send a small control notification */
//    send_json_ctrl(c->ctrl_fd, "{\"status\":\"SENDING_FILE\"}\n");

    /* Send header then raw bytes on data channel */
    send(c->data_fd, header, n, 0);
    FILE *f = fopen(path, "rb");
    if (!f) return;

    char buf[BUFSZ];
    size_t r;
    while ((r = fread(buf, 1, sizeof(buf), f)) > 0) {
        ssize_t w = send(c->data_fd, buf, r, 0);
        if (w <= 0) break;
    }
    fclose(f);
}

/* Worker thread as required */
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

        /* Process */
        switch (it->transform) {

			case TR_CHECKSUM:
			{
				unsigned long chk = calc_checksum(it->filename);
				send_json_ctrl3(c->ctrl_fd, "result", "checksum", "value", chk);
				break;
			}

			case TR_UPPER:
			case TR_REVERSE:
			{
				// build output path
				char outpath[256];
				time_t t = time(NULL);
				snprintf(outpath, sizeof(outpath), "%s/out_c%d_%lu_%s",
						 OUT_DIR, c->id, (unsigned long)t, strrchr(it->filename, '/') ? strrchr(it->filename, '/')+1 : it->filename);

				//Simulate a heavy elaboration
				sleep(5);

				if (it->transform == TR_UPPER) {
					transform_upper(it->filename, outpath);
				}
				else {
					transform_reverse_file(it->filename, outpath);
				}

				// notify on ctrl channel and send via data channel
				send_json_ctrl(c->ctrl_fd, "result", "file");
				send_file_via_data(c, outpath);
				break;
			}

			default:
			case TR_UNKNOWN:
			{
				//send error message
				send_json_ctrl(c->ctrl_fd, "status", "UNKNOWN_OP");
				break;
			}
        }
        /* clear pending flag */
        pthread_mutex_lock(&c->lock);
        c->pendingJob = 0;
        pthread_mutex_unlock(&c->lock);
        free(it);
    }
    return NULL;
}

/* Data listener: accepts data connections, reads a single JSON header line then reads file bytes.
   Header on data socket (single line JSON): {"client_id":N,"filename":"name","filesize":123}
   Saves file to UPLOAD_DIR/upload_c<N>_<basename>
   Keeps data socket open in client->data_fd for sending results later.
   Spawns minimal per-connection handler inside this single thread (blocking reads are fine).
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

        /* read header line */
        char hdr[512];
		int pos = 0;

        while (pos < (int)sizeof(hdr) - 1) {
            int r = recv(fd, hdr + pos, 1, 0);
            if (r <= 0) break;
            if (hdr[pos] == '\n') { hdr[pos] = 0; break; }
            pos += r;
        }
        if (pos == 0) { close(fd); continue; }

        int client_id = parse_int_field(hdr, "client_id");
        char fname[256]; parse_str_field(hdr, "filename", fname, sizeof(fname));
        int filesize = parse_int_field(hdr, "filesize");

        if (client_id < 0 || filesize <= 0 || fname[0] == '\0') { close(fd); continue; }

        client_t *c = get_client_by_id(client_id);
        if (!c) { close(fd); continue; }

        /* compute save path */
        const char *base = strrchr(fname, '/');
		base = base ? base + 1 : fname;

        char savepath[512];
        snprintf(savepath, sizeof(savepath), "%s/upload_c%d_%s", UPLOAD_DIR, client_id, base);

        FILE *f = fopen(savepath, "wb");
        if (!f) { close(fd); continue; }

        int remaining = filesize;
        char buf[BUFSZ];
        while (remaining > 0) {
            int want = remaining < (int)sizeof(buf) ? remaining : (int)sizeof(buf);
            int r = recv(fd, buf, want, 0);
            if (r <= 0) break;
            fwrite(buf, 1, r, f);
            remaining -= r;
        }
        fclose(f);

        /* attach data socket to client for future sends */
        pthread_mutex_lock(&c->lock);
        if (c->data_fd >= 0) close(c->data_fd); // replace existing one, if any
        c->data_fd = fd;
        c->pendingUpload = 0;
        pthread_mutex_unlock(&c->lock);


        /* notify via control channel upload complete */
		send_json_ctrl(c->ctrl_fd, "status", "UPLOAD_COMPLETE");

        /* loop to accept next connection */
    }
    return NULL;
}

/* Handle control commands on control socket.
   Protocol:
    - After accept, server sends {"client_id":N}\n
    - Client sends JSON commands like:
      {"cmd":"request","filename":"name","transform":"uppercase","priority":2}
    - Server responds with BUSY/ENQUEUED etc.
   Note: Control connection is persistent.
*/
int handle_control_message(int ctrl_fd) {

    char buf[1024];

    int n = recv(ctrl_fd, buf, sizeof(buf)-1, 0);
    if (n <= 0) return -1;

    buf[n] = 0;
    client_t *c = get_client_by_ctrlfd(ctrl_fd);
    if (!c) return -1;

    char cmd[10];
    parse_str_field(buf, "cmd", cmd, sizeof(cmd));

    if (strstr(cmd, "request") == NULL) {
    	send_json_ctrl(c->ctrl_fd, "status", "UNKNOWN_CMD");
        return 1;
    }

	pthread_mutex_lock(&c->lock);

	if (c->pendingJob) {
		pthread_mutex_unlock(&c->lock);
		send_json_ctrl(c->ctrl_fd, "status", "BUSY");
		return 0;
	}

	c->pendingJob = 1;

	pthread_mutex_unlock(&c->lock);


	/* parse fields */
	char filename[256]; parse_str_field(buf, "filename", filename, sizeof(filename));
	transform_type_t tr = parse_transform_field(buf);
	int pr = parse_int_field(buf, "priority");

	if (tr == TR_UNKNOWN) {
    	send_json_ctrl(c->ctrl_fd, "status", "UNKNOWN_OP");
        return 1;
	}


	/* map filename to saved upload path */
	const char *base = strrchr(filename, '/'); base = base ? base + 1 : filename;
	char saved[512];
	snprintf(saved, sizeof(saved), "%s/upload_c%d_%s", UPLOAD_DIR, c->id, base);

	/* create work item using required name and push to queue */
	work_item_t *it = malloc(sizeof(*it));
	it->priority = pr;
	it->client_fd = c->id; /* client index */
	c->pendingUpload = 1;
	strncpy(it->filename, saved, sizeof(it->filename)-1);
	it->transform = tr;
	pq_push(it);
	send_json_ctrl(c->ctrl_fd, "status", "ENQUEUED");
	return 0;

}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    jsmn_init(&parser);

    int thread_pool_size = 4;		// Init with default values
    int port = 8080;

    /* parse args */
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-threads") == 0 && i + 1 < argc) thread_pool_size = atoi(argv[++i]);
        else if (strcmp(argv[i], "-port") == 0 && i + 1 < argc) port = atoi(argv[++i]);
        else { fprintf(stderr, "Unknown arg %s\n", argv[i]); exit(1); }
    }

    check_dirs();
    pq_init();

    /* spawn worker threads */
    pthread_t *wth = calloc(thread_pool_size, sizeof(pthread_t));
    for (int i = 0; i < thread_pool_size; i++) {
		pthread_create(&wth[i], NULL, worker_thread, NULL);
	}

    /* start data listener thread on port+1 */
    pthread_t dt;
    int data_port = port + 1;
    pthread_create(&dt, NULL, data_listener, &data_port);


    /* create control listening socket and use epoll to accept/manage many clients */
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
	if (listenfd < 0) { perror("Error while opening listenfd"); return 1; }

    int on = 1;
	setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listenfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("Error while binding data"); return 1; }

    if (listen(listenfd, MAX_CONN) < 0) { perror("Error while listening on listenfd"); return 1; }

    int efd = epoll_create1(0);
    struct epoll_event ev = {0};
    ev.events = EPOLLIN; ev.data.fd = listenfd;
    epoll_ctl(efd, EPOLL_CTL_ADD, listenfd, &ev);

    struct epoll_event events[MAX_EVENTS];

    printf("Server control port %d data port %d threads %d\n", port, data_port, thread_pool_size);

    while (1) {

        int n = epoll_wait(efd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait"); break;
        }

        for (int i = 0; i < n; i++) {

            if (events[i].data.fd == listenfd) {

                /* new connection */
                int cfd = accept(listenfd, NULL, NULL);
                if (cfd < 0) continue;

                set_nonblocking(cfd);

                int cid = register_client(cfd);
                if (cid < 0) { close(cfd); continue; }

                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = cfd;
                epoll_ctl(efd, EPOLL_CTL_ADD, cfd, &ev);

                /* send assigned id to client */
                char alloc[64];
                snprintf(alloc, sizeof(alloc), "{\"client_id\":%d}\n", cid);
                send(cfd, alloc, strlen(alloc), 0);

            } else {

                int fd = events[i].data.fd;

                if (events[i].events & EPOLLIN) {

                    if (handle_control_message(fd) < 0) {
                        /* client closed or error */
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
