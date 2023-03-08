#include "producer-consumer.h"
#include <stdlib.h>

int pcq_create(pc_queue_t *queue, size_t capacity) {
    // Allocate memory for buffer
    queue->pcq_buffer = malloc(capacity * sizeof(void *));
    if (!queue->pcq_buffer) {
        return -1;
    }

    // Initialize locks and condition variables
    if (pthread_mutex_init(&queue->pcq_current_size_lock, NULL) != 0) {
        return -1;
    }
    if (pthread_mutex_init(&queue->pcq_head_lock, NULL) != 0) {
        return -1;
    }
    if (pthread_mutex_init(&queue->pcq_tail_lock, NULL) != 0) {
        return -1;
    }
    if (pthread_cond_init(&queue->pcq_pusher_condvar, NULL) != 0) {
        return -1;
    }
    if (pthread_cond_init(&queue->pcq_popper_condvar, NULL) != 0) {
        return -1;
    }

    // Initialize other fields
    queue->pcq_capacity = capacity;
    queue->pcq_current_size = 0;
    queue->pcq_head = 0;
    queue->pcq_tail = 0;

    return 0;
}

int pcq_destroy(pc_queue_t *queue) {
    // Free buffer memory
    free(queue->pcq_buffer);

    // Destroy locks and condition variables
    pthread_mutex_destroy(&queue->pcq_current_size_lock);
    pthread_mutex_destroy(&queue->pcq_head_lock);
    pthread_mutex_destroy(&queue->pcq_tail_lock);
    pthread_cond_destroy(&queue->pcq_pusher_condvar);
    pthread_cond_destroy(&queue->pcq_popper_condvar);

    return 0;
}

int pcq_enqueue(pc_queue_t *queue, void *elem) {
    pthread_mutex_lock(&queue->pcq_current_size_lock);
    while (queue->pcq_current_size >= queue->pcq_capacity) {
        pthread_cond_wait(&queue->pcq_pusher_condvar,
                          &queue->pcq_current_size_lock);
    }

    // Insert element at the back of the queue
    pthread_mutex_lock(&queue->pcq_tail_lock);
    queue->pcq_buffer[queue->pcq_tail] = elem;
    queue->pcq_tail = (queue->pcq_tail + 1) % queue->pcq_capacity;
    queue->pcq_current_size++;
    pthread_mutex_unlock(&queue->pcq_tail_lock);

    // Signal that there's a new element in the queue
    pthread_cond_signal(&queue->pcq_popper_condvar);
    pthread_mutex_unlock(&queue->pcq_current_size_lock);
    return 0;
}

void *pcq_dequeue(pc_queue_t *queue) {
    pthread_mutex_lock(&queue->pcq_current_size_lock);
    while (queue->pcq_current_size == 0) {
        pthread_cond_wait(&queue->pcq_popper_condvar,
                          &queue->pcq_current_size_lock);
    }

    // Remove element from the front of the queue
    pthread_mutex_lock(&queue->pcq_head_lock);
    void *elem = queue->pcq_buffer[queue->pcq_head];
    queue->pcq_head = (queue->pcq_head + 1) % queue->pcq_capacity;
    queue->pcq_current_size--;
    pthread_mutex_unlock(&queue->pcq_head_lock);

    // Signal that there's a new empty space in the queue
    pthread_cond_signal(&queue->pcq_pusher_condvar);
    pthread_mutex_unlock(&queue->pcq_current_size_lock);

    return elem;
}