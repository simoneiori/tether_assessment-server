/**
 * @file queue.c
 * @brief Utilities for managing the priority queue
 *
 *
 *
 * @author Simone Iori
 * @date 14.09.2025
 */

#include <stdlib.h>
#include <string.h>
#include <pthread.h>


#include "server.h"
#include "queue.h"



priority_queue_t *work_queue = NULL;
pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;


/**
 * @brief Initialize the queue
 *
 * @return Nothing
 */
void pq_init(void) {
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
work_item_t *pq_pop_blocking(void) {
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

