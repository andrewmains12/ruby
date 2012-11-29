#include "ruby/ruby.h"
#include "gc.h"
#include <assert.h>
#include <stdio.h>
#include <pthread.h>

#define MODE_SINGLE_THREAD 0
#define MODE_MULTITHREAD 1
#define MODE_DUAL 2
#define MODE_SINGLE_THREAD_TWICE 3

static int mode = MODE_MULTITHREAD;

#define NTHREADS 1
#define GLOBAL_QUEUE_SIZE 500 /*TODO*/
#define GLOBAL_QUEUE_SIZE_MIN (GLOBAL_QUEUE_SIZE / 4)
#define LOCAL_QUEUE_SIZE 200 /*TODO*/
#define MAX_WORK_TO_GRAB 4
#define MAX_WORK_TO_OFFER 4
#define DEQUE_FULL 0
#define DEQUE_EMPTY -1

#define GC_THREADING_DEBUG 0

#if GC_THREADING_DEBUG
#define debug_print(...)                        \
    printf(__VA_ARGS__)
#else
#define debug_print(...)                        \
    //noop
#endif

/* A mod function that always gives positive results */
#define POS_MOD(a, b) \
    (((a) % (b) + b) % b)

#define MIN(a,b) \
    ((a) < (b) ? (a) : (b))


/**
 * Deque
 */

#define ASSERT_IN_BOUNDS_EXCL(_val, _deque)    \
    assert(_val >= 0);                         \
    assert(_val < _deque->max_length);          

#define ASSERT_IN_BOUNDS_INCL(_val, _deque)    \
    assert(_val >= 0);                         \
    assert(_val <= _deque->max_length);          


#define ASSERT_SANE_DEQUE(d) do {               \
        assert(d != NULL);                      \
        assert(d->max_length >= 0);             \
        ASSERT_IN_BOUNDS_INCL(d->length, d);    \
        ASSERT_IN_BOUNDS_EXCL(d->tail, d);      \
        ASSERT_IN_BOUNDS_EXCL(d->head, d);      \
    } while(0);

typedef struct deque_struct {
    VALUE* buffer;
    int max_length; //Should be the same size as buffer
    int length;
    int head;
    int tail;
} deque_t;

pthread_key_t thread_id_k;
static void deque_init(deque_t* deque, int max_length) {
    //TODO: check error and handle this reasonably
    VALUE* buffer = (VALUE*) malloc(sizeof(VALUE)*max_length);

    deque->buffer = buffer;
    deque->max_length = max_length;
    deque->length = 0;
    deque->head = deque->tail = 0;
}

static void deque_destroy(deque_t* deque) {
    if (deque != NULL) {
        deque->length = -1; /* sentinel */
        deque->head = -1; /* sentinel */
        deque->tail = -1; /* sentinel */

        free(deque->buffer);
    }
}

static void deque_destroy_callback(void* deque) {
    deque_destroy((deque_t*) deque);
}

static int deque_empty_p(deque_t* deque) {
  ASSERT_SANE_DEQUE(deque);

  return deque->length == 0;
}

static int deque_full_p(deque_t* deque) {
  ASSERT_SANE_DEQUE(deque);

  return deque->length == deque->max_length;
}


static int deque_push(deque_t* deque, VALUE val) {
  ASSERT_SANE_DEQUE(deque);
  
  if (deque_full_p(deque))
    return 0;

  if (! deque_empty_p(deque))
      deque->tail = POS_MOD(deque->tail + 1, deque->max_length);

  deque->buffer[deque->tail] = val;
  deque->length++;
  ASSERT_SANE_DEQUE(deque);
  return 1;
}

static VALUE deque_pop(deque_t* deque) {
  VALUE rtn;

  ASSERT_SANE_DEQUE(deque);

  assert(! deque_empty_p(deque));
  rtn = deque->buffer[deque->tail];

  deque->tail = POS_MOD(deque->tail - 1, deque->max_length);

  deque->length--;

  ASSERT_SANE_DEQUE(deque);
  return rtn;
}

static VALUE deque_pop_back(deque_t* deque) {
  VALUE rtn;
  int index;

  ASSERT_SANE_DEQUE(deque);

  assert(! deque_empty_p(deque));

  index = deque->head;
  assert(index >= 0 && index < deque->max_length);
  rtn = deque->buffer[index];

  deque->head = POS_MOD(deque->head - 1, deque->max_length);

  deque->length--;
  ASSERT_SANE_DEQUE(deque);
  return rtn;
}

/**
 * Global Queue
 */

typedef struct global_queue_struct {
    unsigned int waiters;
    deque_t deque;
    pthread_mutex_t lock;
    pthread_cond_t wait_condition;
    unsigned int complete;
} global_queue_t;

#define global_queue_count global_queue->deque.length

static void global_queue_init(global_queue_t* global_queue) {
    global_queue->waiters = 0;
    deque_init(&(global_queue->deque), GLOBAL_QUEUE_SIZE);
    pthread_mutex_init(&global_queue->lock, NULL);
    pthread_cond_init(&global_queue->wait_condition, NULL);
    global_queue->complete = 0;
}

static void global_queue_destroy(global_queue_t* global_queue) {
    deque_destroy(&(global_queue->deque));
    pthread_mutex_destroy(&global_queue->lock);
    pthread_cond_destroy(&global_queue->wait_condition);
}

static void global_queue_pop_work(unsigned long thread_id, global_queue_t* global_queue, deque_t* local_queue) {
    int i, work_to_grab;

    debug_print("Thread %lu aquiring global queue lock\n", thread_id);
    pthread_mutex_lock(&global_queue->lock);
    debug_print("Thread %lu aquired global queue lock\n", thread_id);
    while (global_queue_count == 0 && !global_queue->complete) {
        global_queue->waiters++;
        debug_print("Thread %lu checking wait condition. Waiters: %d NTHREADS: %d\n", thread_id, global_queue->waiters, NTHREADS);
        if (global_queue->waiters == NTHREADS) {
            debug_print("Marking complete + waking threads\n");
            global_queue->complete = 1;
            pthread_cond_broadcast(&global_queue->wait_condition);
        } else {
            // Release the lock and go to sleep until someone signals
            debug_print("Thread %lu waiting. Waiters: %d\n", thread_id, global_queue->waiters);
            pthread_cond_wait(&global_queue->wait_condition, &global_queue->lock);
            debug_print("Thread %lu awoken\n", thread_id);
        }
        global_queue->waiters--;
    }
    work_to_grab = MIN(global_queue_count, MAX_WORK_TO_GRAB);
    for (i = 0; i < work_to_grab; i++)  {
        deque_push(local_queue, deque_pop(&(global_queue->deque)));
    }
    debug_print("Thread %lu took %d items from global\n", thread_id, work_to_grab);

    pthread_mutex_unlock(&global_queue->lock);
}

static void global_queue_offer_work(global_queue_t* global_queue, deque_t* local_queue) {
    int i;
    int localqueuesize = local_queue->length;
    int items_to_offer, free_slots;

    if ((global_queue->waiters && localqueuesize > 2) ||
            (global_queue_count < GLOBAL_QUEUE_SIZE_MIN &&
             localqueuesize > LOCAL_QUEUE_SIZE / 2)) {

        pthread_mutex_lock(&global_queue->lock);

        free_slots = GLOBAL_QUEUE_SIZE - global_queue_count;
        items_to_offer = MIN(localqueuesize / 2, free_slots);
        //Offer to global
        debug_print("Thread %lu offering %d items to global\n",
               *((long*)pthread_getspecific(thread_id_k)),
               items_to_offer);

        for (i = 0; i < items_to_offer; i++) {
            assert(deque_push(&(global_queue->deque), deque_pop_back(local_queue)));
        }
        if (global_queue->waiters) {
            pthread_cond_broadcast(&global_queue->wait_condition);
        }
        pthread_mutex_unlock(&global_queue->lock);

    }
}

/**
 * Threading code
 */

void* active_objspace;
global_queue_t* global_queue;
pthread_key_t thread_local_deque_k;


static void* mark_run_loop(void* arg) {
    long thread_id = (long) arg;
    deque_t deque;
    VALUE v;
    debug_print("Thread %lu started\n", thread_id);

    deque_init(&deque, LOCAL_QUEUE_SIZE);
    pthread_setspecific(thread_local_deque_k, &deque);
    pthread_setspecific(thread_id_k, &thread_id);
    if (thread_id == 0) {
        debug_print("Thread 0 running start_mark\n");
        gc_start_mark(active_objspace);
        debug_print("Thread 0 finished start_mark\n");
    }
    while (1) {
        global_queue_offer_work(global_queue, &deque);
        if (deque_empty_p(&deque)) {
            debug_print("Thread %lu taking work from the global queue\n", thread_id);
            global_queue_pop_work(thread_id, global_queue, &deque);
        }
        if (global_queue->complete) {
            break;
        }
        v = deque_pop(&deque);
        //        debug_print("Thread %lu marking %lu\n", thread_id, v);
        gc_do_mark(active_objspace, v);
    }
    return NULL;
}

static void gc_mark_parallel(void* objspace) {
    global_queue_t queuedata;
    pthread_attr_t attr;
    pthread_t threads[NTHREADS];
    long t;
    void* status;

    active_objspace = objspace;
    global_queue = &queuedata;
    global_queue_init(global_queue);

    pthread_key_create(&thread_local_deque_k, deque_destroy_callback);
    pthread_key_create(&thread_id_k, NULL);

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    for (t = 1; t < NTHREADS; t++) {
        pthread_create(&threads[t], &attr, mark_run_loop, (void*)t);
        //TODO: handle error codes
    }
    //Set master thread to doing the same work as its slaves
    //We need the master thread to not be created with pthread_create in order to
    //use the stack information present in the master thread to obtain the root set
    mark_run_loop(0);
    pthread_attr_destroy(&attr);

    //Wait for everyone to finish
    for (t = 1; t < NTHREADS; t++) {
        pthread_join(threads[t], &status);
        //TODO: handle error codes
    }

    global_queue_destroy(global_queue);
}

void gc_mark_defer(void *objspace, VALUE ptr, int lev) {
    int tid = *((int*)pthread_getspecific(thread_id_k));
    deque_t* deque = (deque_t*) pthread_getspecific(thread_local_deque_k);
    if (deque_push(deque, ptr) == 0) {
        global_queue_offer_work(global_queue, deque);
        if (deque_push(deque, ptr) == 0) {
            printf("Thread %d: Both queues full\n", tid);
            gc_do_mark(objspace, ptr);
        }
    }
}

void gc_markall(void* objspace) {
    switch (mode) {
        case MODE_SINGLE_THREAD:
            gc_defer_mark = 0;
            gc_start_mark(objspace);
            break;
        case MODE_MULTITHREAD:
            gc_defer_mark = 1;
            gc_mark_parallel(objspace);
            break;
        case MODE_DUAL:
            gc_defer_mark = 1;
            GC_TEST_LOG("A\n");
            gc_mark_parallel(objspace);
            GC_TEST_LOG("END\n");
            gc_mark_reset(objspace);
            gc_defer_mark = 0;
            GC_TEST_LOG("B\n");
            gc_start_mark(objspace);
            GC_TEST_LOG("END\n");
            break;
        case MODE_SINGLE_THREAD_TWICE:
            gc_defer_mark = 0;
            GC_TEST_LOG("A\n");
            gc_start_mark(objspace);
            GC_TEST_LOG("END\n");
            gc_mark_reset(objspace);
            GC_TEST_LOG("B\n");
            gc_start_mark(objspace);
            GC_TEST_LOG("END\n");
            break;
    }
}
