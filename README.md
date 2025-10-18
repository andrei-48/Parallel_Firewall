# Firewall Parallel Processing
### Andrei-Bogdan Marinescu

## Overview
This project implements a multithreaded firewall simulation using a producer-consumer architecture. A producer thread generates packets (containing a mock source, destination, timestamp, and payload) and stores them in a shared circular buffer. Multiple consumer threads retrieve these packets, apply filtering logic, and log the final DROP/PASS decisions.

The goal of the assignment is to:
- Implement a synchronized ring buffer for communication between threads
- Implement consumer threads that process packets without busy waiting
- Maintain a log file sorted by packet timestamp

## Project Structure
```
src/         → contains starter code and TODOs (parallel logic)
utils/       → logging & debugging utilities
tests/       → unit and integration tests
```

## Implementation Details
### Threads
- **Producer thread** inserts packets into the shared circular buffer
- **Consumer threads** remove packets, process them, and write results to the log
- Number of consumers is given as the 3rd command-line argument

### Ring Buffer
The ring buffer:
- Is implemented as a fixed-size circular array
- Supports FIFO operations via `ring_buffer_enqueue()` and `ring_buffer_dequeue()`
- Uses pthread synchronization primitives (mutexes / condition variables)
- Prevents busy waiting by blocking when empty/full

### Logging
- Consumers write decisions (PASS/DROP), packet hash, and timestamp
- Logs must remain sorted by **timestamp** (arrival order)
- Logging follows the format in `src/serial.c`

## Building
Use the provided `Makefile`:
```
make
```
This builds the parallel implementation.

## Running
The program expects three arguments:
```
./firewall <input_file> <output_file> <num_threads>
```
Example:
```
./firewall packets.in firewall.log 4
```

## Cleanup
To remove compiled files:
```
make clean
```

## Synchronization Strategy
This implementation uses two distinct synchronization layers:

### 1. Ring Buffer (Producer ↔ Consumers)
The ring buffer uses:
- **`ring->mutex`** to protect shared state (`len`, `read_pos`, `write_pos`)
- **`empty_cond`** condition variable
  - Consumers wait when the buffer is empty
  - Producer signals when new packets become available
- **`full_cond`** condition variable
  - Producer waits when the buffer is full
  - Consumers signal when space becomes available

This ensures no busy waiting and correct FIFO ordering at the buffer level.

### 2. Timestamp-Ordered Logging Between Consumers
Multiple consumer threads could finish processing packets in parallel, but the log must remain sorted by timestamp (input order). To guarantee this:
- The consumers use a **separate mutex** (`ctx->mutex`) to coordinate logging
- Each dequeue returns a logical index (`res`)
- A consumer may only write its result when `curr_write_ind == res`
- Other consumers wait on `time_cond` until it is their turn

This enforces strictly sequential writes to the log file while still allowing concurrent packet processing.
