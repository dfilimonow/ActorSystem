#include <stdio.h>
#include "cacti.h"

#define MSG_FATHER (message_type_t)0x0
#define MSG_SON (message_type_t)0x1
#define MSG_FACT (message_type_t)0x2

typedef struct factorial
{
	unsigned long long result;
	unsigned long long step;
	unsigned long long max_step;
} factorial_t;

void get_factorial(void **stateptr, size_t nbytes, void *data);
void get_father_info(void **stateptr, size_t nbytes, void *data);
void get_son_info(void **stateptr, size_t nbytes, void *data);

act_t funs[3] =
	{
		&get_father_info,
		&get_son_info,
		&get_factorial};

role_t actor_role =
	{
		.prompts = funs,
		.nprompts = 3};

actor_id_t first_actor_id;

// Voiding to disable warnings

void get_factorial(void **stateptr, size_t nbytes, void *data)
{
	(void)nbytes;
	(*stateptr) = data;

	((factorial_t *)(*stateptr))->result *= ((factorial_t *)(*stateptr))->step++;

	message_t msg;
	if (((factorial_t *)(*stateptr))->step > ((factorial_t *)(*stateptr))->max_step)
	{
		message_t dead = {MSG_GODIE, sizeof(NULL), NULL};
		msg = dead;
		printf("%llu\n", ((factorial_t *)(*stateptr))->result);
	}
	else
	{
		message_t spawn = {MSG_SPAWN, sizeof(role_t), (void *)&actor_role};
		msg = spawn;
	}
	send_message(actor_id_self(), msg);
}

void get_father_info(void **stateptr, size_t nbytes, void *data)
{
	(void)stateptr, (void)nbytes;
	actor_id_t me = actor_id_self();
	message_t son_message = {MSG_SON, sizeof(actor_id_t), (void *)me};
	if (me > first_actor_id)
	{
		send_message(*((actor_id_t *)data), son_message);
	}
}

void get_son_info(void **stateptr, size_t nbytes, void *data)
{
	(void)nbytes;
	message_t fact_message = {MSG_FACT, sizeof(stateptr), (void *)(*stateptr)};
	send_message((actor_id_t)data, fact_message);
	message_t dead_message = {MSG_GODIE, sizeof(NULL), NULL};
	send_message(actor_id_self(), dead_message);
}

int main()
{
	unsigned long long input;
	scanf("%llu", &input);
	factorial_t data = {1, 1, input};
	message_t message = {MSG_FACT, sizeof(message_t), (void *)&data};

	actor_system_create(&first_actor_id, &actor_role);
	send_message(first_actor_id, message);
	actor_system_join(first_actor_id);
	return 0;
}
