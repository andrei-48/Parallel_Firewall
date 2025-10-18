# Firewall Parallel Processing - README

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

## Notes
- No busy waiting is allowed (no infinite `while()` loops or sleeps)
- The serial reference implementation is provided in `src/serial.c`

## Author
Andrei-Bogdan Marinescu

