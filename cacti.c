#include "cacti.h"
#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>

typedef struct actor_queue_node
{
    actor_id_t id;
    struct actor_queue_node *next;
} actor_queue_node_t;

actor_queue_node_t *create_actor_queue_node(actor_id_t id)
{
    actor_queue_node_t *node = (actor_queue_node_t *)malloc(sizeof(actor_queue_node_t));
    if (node == NULL)
    {
        exit(1);
    }
    node->id = id;
    node->next = NULL;
    return node;
}

typedef struct actor_queue
{
    actor_queue_node_t *first;
    actor_queue_node_t *last;
    size_t size;
} actor_queue;

actor_queue *create_actor_queue()
{
    actor_queue *queue = (actor_queue *)malloc(sizeof(actor_queue));
    if (queue == NULL)
    {
        exit(1);
    }
    queue->first = NULL;
    queue->last = NULL;
    queue->size = 0;
    return queue;
}

void push_actor_queue(actor_queue *queue, actor_id_t id)
{
    actor_queue_node_t *node = create_actor_queue_node(id);

    if (queue->last == NULL)
    {
        queue->first = node;
    }
    else
    {
        queue->last->next = node;
    }
    queue->last = node;
    queue->size++;
}

void pop_actor_queue(actor_queue *queue)
{
    if (queue->first != NULL)
    {
        actor_queue_node_t *node = queue->first;
        queue->first = queue->first->next;
        if (queue->first == NULL)
        {
            queue->last = NULL;
        }
        queue->size--;
        free(node);
    }
}

actor_id_t get_front_actor_queue(actor_queue *queue)
{
    return queue->first->id;
}

bool is_actor_queue_empty(actor_queue *queue)
{
    return queue->first == NULL ? true : false;
}

typedef struct message_queue_node
{
    message_t data;
    struct message_queue_node *next;
} message_queue_node_t;

message_queue_node_t *create_message_queue_node(message_t data)
{
    message_queue_node_t *node = (message_queue_node_t *)malloc(sizeof(message_queue_node_t));
    if (node == NULL)
    {
        exit(1);
    }
    node->data = data;
    node->next = NULL;
    return node;
}

typedef struct message_queue
{
    message_queue_node_t *first;
    message_queue_node_t *last;
    size_t size;
} message_queue;

message_queue *create_message_queue()
{
    message_queue *queue = (message_queue *)malloc(sizeof(message_queue));
    if (queue == NULL)
    {
        exit(1);
    }
    queue->first = NULL;
    queue->last = NULL;
    queue->size = 0;
    return queue;
}

void push_message_queue(message_queue *queue, message_t data)
{
    message_queue_node_t *node = create_message_queue_node(data);

    if (queue->last == NULL)
    {
        queue->first = node;
    }
    else
    {
        queue->last->next = node;
    }
    queue->last = node;
    queue->size++;
}

void pop_message_queue(message_queue *queue)
{
    if (queue->first != NULL)
    {
        message_queue_node_t *node = queue->first;
        queue->first = queue->first->next;
        if (queue->first == NULL)
        {
            queue->last = NULL;
        }
        free(node);
        queue->size--;
    }
}

message_t get_front_message_queue(message_queue *queue)
{
    return queue->first->data;
}

bool is_message_queue_empty(message_queue *queue)
{
    return queue->first == NULL ? true : false;
}

typedef struct actor
{
    bool is_dead;
    role_t act_role;
    void *state;
    pthread_mutex_t work_mutex;
    message_queue *queue;
} actor_t;

typedef struct actor_system
{
    actor_t *actors;

    pthread_mutex_t terminate_mutex;

    size_t actor_number;
    size_t not_empty;
    size_t actor_dead;
    size_t actor_capacity;
    uint64_t messages;
    bool is_sig;
    bool termimnating;
    bool joining;
    bool terminated;

} actor_system;

actor_system *sys = NULL;

typedef struct tpool
{
    actor_queue *system_queue;
    pthread_mutex_t work_mutex;
    pthread_cond_t work_cond;
    pthread_cond_t working_cond;
    size_t working_count;
    size_t thread_count;
    bool stop;
} tpool_t;

pthread_key_t global_key;
int locked = 0, unlocked = 0;
static void *tpool_thread(void *arg)
{
    tpool_t *tp = (tpool_t *)arg;
    size_t diff;
    actor_id_t id;

    bool abort = false;
    while (abort == false)
    {
        pthread_mutex_lock(&(tp->work_mutex));
        while (tp->system_queue->first == NULL && !(tp->stop))
        {
            pthread_cond_wait(&(tp->work_cond), &(tp->work_mutex));
        }
        if (tp->stop == true)
        {
            break;
        }

        tp->working_count++;
        id = tp->system_queue->first->id;
        diff = 0;

        pthread_mutex_lock(&(sys->actors[id].work_mutex));
        locked++;
        if (tp->system_queue->first != NULL && tp->system_queue->first->id == id && sys->actors[id].queue->first != NULL)
        {
            pthread_setspecific(global_key, (void *)id);
            message_queue *temp_queue = create_message_queue();
            while (tp->system_queue->first != NULL && tp->system_queue->first->id == id && sys->actors[id].queue->first != NULL)
            {
                push_message_queue(temp_queue, get_front_message_queue(sys->actors[id].queue));
                pop_message_queue(sys->actors[id].queue);
                pop_actor_queue(tp->system_queue);
                diff++;
            }

            pthread_mutex_unlock(&(tp->work_mutex));
            for (size_t i = 0; i < diff; ++i)
            {
                message_t new_mes = get_front_message_queue(temp_queue);

                if (new_mes.message_type == MSG_SPAWN && (sys->is_sig == false && sys->actors[id].is_dead == false))
                {
                    actor_t new_actor;
                    role_t *new_role = (role_t *)new_mes.data;
                    new_actor.act_role = *new_role;
                    new_actor.is_dead = false;
                    new_actor.state = NULL;
                    new_actor.queue = create_message_queue();
                    pthread_mutex_init(&(new_actor.work_mutex), NULL);

                    pthread_mutex_unlock(&(sys->actors[id].work_mutex));
                    pthread_mutex_lock(&(tp->work_mutex));
                    if (sys->actor_capacity == sys->actor_number + 1)
                    {
                        sys->actor_capacity = (sys->actor_capacity * 2 > CAST_LIMIT ? CAST_LIMIT : sys->actor_capacity * 2);
                        actor_t *temp, *actors = (actor_t *)malloc(sys->actor_capacity * sizeof(actor_t));
                        if (actors == NULL)
                        {
                            exit(1);
                        }

                        for (size_t j = 0; j < sys->actor_number; ++j)
                        {
                            actors[j] = sys->actors[j];
                        }
                        temp = sys->actors;
                        sys->actors = actors;
                        free(temp);
                    }

                    if (sys->actor_number + 1 > sys->actor_capacity)
                    {
                        exit(1);
                    }
                    sys->actors[sys->actor_number] = new_actor;
                    sys->actor_number++;
                    pthread_mutex_unlock(&(tp->work_mutex));
                    pthread_mutex_lock(&(sys->actors[id].work_mutex));
                    actor_id_t my_id = id;
                    message_t msg = {MSG_HELLO, sizeof(actor_id_t), (void *)&my_id};

                    pthread_mutex_unlock(&(sys->actors[id].work_mutex));
                    send_message(sys->actor_number - 1, msg);
                    pthread_mutex_lock(&(sys->actors[id].work_mutex));
                }
                else if (new_mes.message_type == MSG_GODIE)
                {
                    if (sys->actors[id].is_dead == false)
                    {
                        pthread_mutex_lock(&(tp->work_mutex));
                        sys->actors[id].is_dead = true;
                        sys->actor_dead++;
                        pthread_mutex_unlock(&(tp->work_mutex));
                    }
                }
                else
                {
                    for (size_t i = 0; i < sys->actors[id].act_role.nprompts; ++i)
                    {

                        if ((size_t)new_mes.message_type == i)
                        {
                            pthread_mutex_unlock(&(sys->actors[id].work_mutex));
                            sys->actors[id].act_role.prompts[i]((void **)&sys->actors[id].state, new_mes.nbytes, new_mes.data);
                            pthread_mutex_lock(&(sys->actors[id].work_mutex));
                        }
                    }
                }
                pop_message_queue(temp_queue);
                pthread_mutex_unlock(&(sys->actors[id].work_mutex));
                pthread_mutex_lock(&(tp->work_mutex));
                sys->messages--;
                if (tp->system_queue->size == 0 && sys->messages == 0 && sys->actor_dead == sys->actor_number)
                {
                    pthread_mutex_unlock(&(tp->work_mutex));
                    pthread_mutex_lock(&(sys->terminate_mutex));
                    sys->terminated = true;
                    pthread_mutex_unlock(&(sys->terminate_mutex));
                    pthread_mutex_lock(&(tp->work_mutex));
                    abort = true;
                    tp->stop = true;
                    pthread_mutex_unlock(&(tp->work_mutex));
                }
                else
                {
                    pthread_mutex_unlock(&(tp->work_mutex));
                }
                pthread_mutex_lock(&(sys->actors[id].work_mutex));
            }
            free(temp_queue);
        }
        else
        {
            pthread_mutex_unlock(&(tp->work_mutex));
        }

        pthread_mutex_unlock(&(sys->actors[id].work_mutex));
        unlocked++;
        pthread_mutex_lock(&(tp->work_mutex));
        tp->working_count--;
        if (!(tp->stop) && tp->working_count == 0 && tp->system_queue->first == NULL)
        {
            pthread_cond_signal(&(tp->working_cond));
        }
        if (abort == false)
        {
            pthread_mutex_unlock(&(tp->work_mutex));
        }
    }
    tp->thread_count--;
    pthread_cond_signal(&(tp->working_cond));
    pthread_cond_broadcast(&(tp->work_cond));
    pthread_mutex_unlock(&(tp->work_mutex));
    return tp;
}

tpool_t *tpool_create(size_t n)
{
    tpool_t *tp;
    pthread_t thread;

    tp = (tpool_t *)malloc(sizeof(*tp));
    if (tp == NULL)
    {
        exit(1);
    }
    pthread_cond_init(&(tp->work_cond), NULL);
    pthread_cond_init(&(tp->working_cond), NULL);
    pthread_mutex_init(&(tp->work_mutex), NULL);

    tp->system_queue = create_actor_queue();
    tp->stop = false;
    tp->thread_count = n;
    tp->working_count = 0;

    pthread_key_create(&global_key, 0);

    for (size_t i = 0; i < n; ++i)
    {
        pthread_create(&thread, NULL, tpool_thread, tp);
        pthread_detach(thread);
    }
    return tp;
}

void tpool_wait(tpool_t *tp)
{
    if (tp == NULL)
    {
        return;
    }
    pthread_mutex_lock(&(tp->work_mutex));
    while (1)
    {
        if ((!(tp->stop) && tp->working_count > 0) || (tp->stop && tp->thread_count > 0) || sys->messages > 0 ||
            (sys->actor_dead < sys->actor_number && sys->is_sig == false))
        {
            pthread_cond_wait(&(tp->working_cond), &(tp->work_mutex));
        }
        else
        {
            break;
        }
    }
    pthread_mutex_unlock(&(tp->work_mutex));
}

void tpool_destroy(tpool_t *tp)
{
    if (tp == NULL)
    {
        return;
    }
    pthread_mutex_lock(&(tp->work_mutex));
    tp->stop = true;
    pthread_cond_broadcast(&(tp->work_cond));

    pthread_mutex_unlock(&(tp->work_mutex));
    tpool_wait(tp);

    pthread_cond_destroy(&(tp->work_cond));
    pthread_cond_destroy(&(tp->working_cond));
    pthread_mutex_destroy(&(tp->work_mutex));
    sys->is_sig = true;
    free(tp->system_queue);
    free(tp);
}

void tpool_add_actor_id(tpool_t *tp, actor_id_t id)
{
    if (tp == NULL)
    {
        return;
    }
    pthread_mutex_lock(&(tp->work_mutex));
    push_actor_queue(tp->system_queue, id);
    pthread_cond_broadcast(&(tp->work_cond));
    pthread_mutex_unlock(&(tp->work_mutex));
}

tpool_t *pool = NULL;
void sig_handler(int signum)
{
    if (sys->is_sig == false && signum == SIGINT)
    {
        sys->is_sig = true;
        pthread_mutex_lock(&(sys->terminate_mutex));
        if (sys->joining == true || sys->termimnating == true)
        {
            pthread_cond_signal(&(pool->working_cond));
            pthread_mutex_unlock(&(sys->terminate_mutex));
        }
        else if (sys->terminated == false)
        {
            sys->termimnating = true;
            pthread_mutex_unlock(&(sys->terminate_mutex));
            tpool_wait(pool);
            pthread_mutex_lock(&(sys->terminate_mutex));
            sys->terminated = true;
            pthread_mutex_unlock(&(sys->terminate_mutex));
        }
        else
        {
            pthread_mutex_unlock(&(sys->terminate_mutex));
        }
    }
}

void create_actor_system(actor_id_t *first_actor, role_t *role)
{
    signal(SIGINT, sig_handler);
    actor_system *new_sys = (actor_system *)malloc(sizeof(actor_system));
    if (new_sys == NULL)
    {
        exit(1);
    }
    *first_actor = 0;
    pthread_setspecific(global_key, (void *)0);
    new_sys->termimnating = false;
    new_sys->joining = false;
    new_sys->terminated = false;
    new_sys->actor_number = 1;
    new_sys->messages = 0;
    new_sys->is_sig = false;
    pthread_mutex_init(&(new_sys->terminate_mutex), NULL);
    new_sys->actor_dead = 0;
    new_sys->not_empty = 0;
    new_sys->actor_capacity = 128;
    new_sys->actors = (actor_t *)malloc(128 * sizeof(actor_t));
    if (new_sys->actors == NULL)
    {
        exit(1);
    }
    sys = new_sys;

    actor_t new_actor;
    new_actor.act_role = *role;
    new_actor.is_dead = false;
    new_actor.state = NULL;
    new_actor.queue = create_message_queue();
    pthread_mutex_init(&(new_actor.work_mutex), NULL);

    sys->actors[actor_id_self()] = new_actor;
    actor_id_t send_act = actor_id_self();
    message_t msg = {MSG_HELLO, sizeof(actor_id_t), (void *)&send_act};
    if (msg.data == NULL)
    {
        exit(1);
    }
    send_message(actor_id_self(), msg);
}

void delete_actor_system(actor_system *del_sys)
{
    if (del_sys == NULL)
    {
        return;
    }

    for (size_t i = 0; i < del_sys->actor_number; ++i)
    {
        pthread_mutex_destroy(&(del_sys->actors[i].work_mutex));
        free(del_sys->actors[i].queue);
    }
    free(del_sys->actors);
    pthread_mutex_lock(&(sys->terminate_mutex));
    pthread_mutex_unlock(&(sys->terminate_mutex));
    pthread_mutex_destroy(&(sys->terminate_mutex));
    free(del_sys);
}

int actor_system_create(actor_id_t *actor, role_t *role)
{
    if (sys == NULL)
    {
        pool = tpool_create(POOL_SIZE);
        if (pool == NULL)
        {
            exit(1);
        }
        create_actor_system(actor, role);
        if (pool == NULL)
        {
            exit(1);
        }
    }
    else
    {
        return -1;
    }
    return 0;
}

actor_id_t actor_id_self()
{
    return (actor_id_t)pthread_getspecific(global_key);
}

void actor_system_join(actor_id_t actor)
{
    sys->joining = true;
    pthread_mutex_lock(&(pool->work_mutex));
    if (sys != NULL && (size_t)actor < sys->actor_number)
    {
        pthread_mutex_unlock(&(pool->work_mutex));
        pthread_mutex_lock(&(sys->terminate_mutex));
        if (sys->termimnating == true && sys->terminated == false)
        {
            pthread_cond_signal(&(pool->working_cond));
            pthread_mutex_unlock(&(sys->terminate_mutex));
        }
        else if (sys->terminated == false)
        {
            pthread_mutex_unlock(&(sys->terminate_mutex));
            tpool_wait(pool);
            pthread_mutex_lock(&(sys->terminate_mutex));
            sys->terminated = true;
            pthread_mutex_unlock(&(sys->terminate_mutex));
        }
        else
        {
            pthread_mutex_unlock(&(sys->terminate_mutex));
        }

        tpool_destroy(pool);
        delete_actor_system(sys);
        sys = NULL;
        pool = NULL;
    }
    else
    {
        pthread_mutex_unlock(&(pool->work_mutex));
    }
}

int send_message(actor_id_t actor, message_t message)
{
    if (sys == NULL)
    {
        return -2;
    }
    pthread_mutex_lock(&(pool->work_mutex));
    if ((size_t)actor >= sys->actor_number)
    {
        pthread_mutex_unlock(&(pool->work_mutex));
        return -2;
    }
    else if (sys->is_sig == true || sys->actors[actor].is_dead == true)
    {
        pthread_mutex_unlock(&(pool->work_mutex));
        return -1;
    }
    else if (sys->actors[actor].queue->size == ACTOR_QUEUE_LIMIT)
    {
        pthread_mutex_unlock(&(pool->work_mutex));
        return -3;
    }
    else
    {

        pthread_mutex_lock(&(sys->terminate_mutex));
        if (sys->termimnating == true || sys->terminated == true)
        {
            pthread_mutex_unlock(&(sys->terminate_mutex));
            pthread_mutex_unlock(&(pool->work_mutex));
            return -1;
        }
        pthread_mutex_unlock(&(sys->terminate_mutex));
        sys->messages++;
        pthread_mutex_unlock(&(pool->work_mutex));

        pthread_mutex_lock(&(sys->actors[actor].work_mutex));

        push_message_queue(sys->actors[actor].queue, message);
        pthread_mutex_unlock(&(sys->actors[actor].work_mutex));

        tpool_add_actor_id(pool, actor);
    }
    return 0;
}
