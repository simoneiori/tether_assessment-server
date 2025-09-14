#ifndef QUEUE_H
#define QUEUE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

#include "server.h"


void pq_init(void);
void pq_push(work_item_t *it);
work_item_t *pq_pop_blocking(void);



#ifdef __cplusplus
}
#endif

#endif /* QUEUE_H */
