/* Copyright 2023 < Stefan Miruna Andreea, 314CA > */
#include <stdlib.h>
#include <string.h>

#include "server.h"


server_memory *init_server_memory()
{
	// allocate memory for the server_memory structure
	server_memory *server;
	server = (server_memory *)malloc(sizeof(server_memory));
	DIE(!server, "Failed to allocate memory\n");

	// create the hashtable inside the server_memory structure
	server->ht = ht_create(HMAX, hash_function_string,
				compare_function_strings);

	return server;
}

void server_store(server_memory *server, char *key, char *value) {
	ht_put(server->ht, key, strlen(key) + 1, value, strlen(value) + 1);
}

char *server_retrieve(server_memory *server, char *key) {
	return ht_get(server->ht, key);
}

void server_remove(server_memory *server, char *key) {
	ht_remove_entry(server->ht, key);
}

void free_server_memory(server_memory *server) {
	ht_free(server->ht);
	free(server);
}
