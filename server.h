#ifndef SERVER_H
#define SERVER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>



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




#ifdef __cplusplus
}
#endif

#endif /* SERVER_H */
