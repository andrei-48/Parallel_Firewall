// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	ring->cap = cap;
	ring->len = 0;
	ring->read_pos = 0;
	ring->write_pos = 0;
	ring->data = malloc(cap);
	DIE(!ring->data, "malloc failed");

	ring->is_done = 0;
	ring->deq_ind = 0;
	pthread_mutex_init(&ring->mutex, NULL);
	pthread_cond_init(&ring->empty_cond, NULL);
	pthread_cond_init(&ring->full_cond, NULL);

	return 1;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	pthread_mutex_lock(&ring->mutex);
	while (!ring->is_done && ring->len == ring->cap)
		pthread_cond_wait(&ring->full_cond, &ring->mutex);

	if (ring->is_done) {
		pthread_mutex_unlock(&ring->mutex);
		return 0;
	}

	memcpy(ring->data + ring->write_pos, data, size);
	ring->write_pos = (ring->write_pos + size) % ring->cap;

	ring->len += size;
	pthread_mutex_unlock(&ring->mutex);

	pthread_cond_signal(&ring->empty_cond);
	return 0;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	ssize_t res;

	pthread_mutex_lock(&ring->mutex);
	while (!ring->is_done && ring->len == 0)
		pthread_cond_wait(&ring->empty_cond, &ring->mutex);


	if (ring->is_done && ring->len == 0) {
		pthread_mutex_unlock(&ring->mutex);
		return -1;
	}

	memcpy(data, ring->data + ring->read_pos, size);

	ring->read_pos = (ring->read_pos + size) % ring->cap;
	ring->len -= size;
	res = ring->deq_ind;
	ring->deq_ind++;
	pthread_mutex_unlock(&ring->mutex);

	pthread_cond_signal(&ring->full_cond);

	return res;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	free(ring->data);

	pthread_mutex_destroy(&ring->mutex);
	pthread_cond_destroy(&ring->empty_cond);
	pthread_cond_destroy(&ring->full_cond);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	ring->is_done = 1;
	pthread_cond_broadcast(&ring->empty_cond);
	pthread_cond_broadcast(&ring->full_cond);
}
