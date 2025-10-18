// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

void consumer_thread(so_consumer_ctx_t *ctx)
{
	so_packet_t pkt;
	char out_buf[PKT_SZ];
	ssize_t res = ring_buffer_dequeue(ctx->producer_rb, &pkt, PKT_SZ);

	while (res != -1) {
		int action = process_packet(&pkt);
		unsigned long hash = packet_hash(&pkt);
		unsigned long timestamp = pkt.hdr.timestamp;

		int len = snprintf(out_buf, 256, "%s %016lx %lu\n",
			RES_TO_STR(action), hash, timestamp);

		pthread_mutex_lock(&ctx->mutex);
		while (ctx->curr_write_ind != res)
			pthread_cond_wait(&ctx->time_cond, &ctx->mutex);

		write(ctx->fd, out_buf, len);
		ctx->curr_write_ind++;
		pthread_cond_broadcast(&ctx->time_cond);
		pthread_mutex_unlock(&ctx->mutex);

		res = ring_buffer_dequeue(ctx->producer_rb, &pkt, PKT_SZ);
	}
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{
	so_consumer_ctx_t *ctx;

	ctx = malloc(sizeof(so_consumer_ctx_t));
	ctx->producer_rb = rb;
	ctx->curr_write_ind = 0;
	pthread_cond_init(&ctx->time_cond, NULL);
	pthread_mutex_init(&ctx->mutex, NULL);
	ctx->fd = open(out_filename, O_RDWR|O_CREAT|O_TRUNC, 0666);
	for (int i = 0; i < num_consumers; i++) {
		pthread_create(tids + i, NULL, (void *)&consumer_thread, ctx);
		DIE(!tids[i], "thread create");
	}

	return num_consumers;
}
