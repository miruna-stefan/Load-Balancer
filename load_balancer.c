/* Copyright 2023 < Stefan Miruna Andreea, 314CA > */
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "server.h"
#include "load_balancer.h"

#define MAX_SERVERS 99999

typedef unsigned int uint;

unsigned int hash_function_servers(void *a) {
    unsigned int uint_a = *((unsigned int *)a);

    uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
    uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
    uint_a = (uint_a >> 16u) ^ uint_a;
    return uint_a;
}

unsigned int hash_function_key(void *a) {
    unsigned char *puchar_a = (unsigned char *)a;
    unsigned int hash = 5381;
    int c;

    while ((c = *puchar_a++))
        hash = ((hash << 5u) + hash) + c;

    return hash;
}

load_balancer *init_load_balancer() {
    // allocate memory for the load balancer structure
    load_balancer *loader;
    loader = (load_balancer *)malloc(sizeof(load_balancer));
    // defensive programming
    DIE(!loader, "Failed to allocate memory\n");

    // allocate memory for the hashring, the array in itself
    loader->hashring = malloc(MAX_SERVERS * sizeof(server_details *));
    DIE(!loader->hashring, "Failed to allocate memory\n");

    loader->num_servers = 0;

    return loader;
}

/* function that allocates memory and initializes all the structure
fields for a new server copy and returns pointer to the new server copy */
server_details *create_new_server_copy(int server_id, uint tag)
{
    // allocate memory for the server_details structure
    server_details *copy;
    copy = (server_details *)malloc(sizeof(server_details));
    DIE(!copy, "Failed to allocate memory\n");

    // allocate memory for the server hashtable
    copy->server = init_server_memory();

    copy->server_id = server_id;
    copy->server_tag = tag * pow(10, 5) + server_id;
    copy->hash = hash_function_servers(&copy->server_tag);

    return copy;
}

// function that places only one copy into the hashring and retruns its position
int insert_a_sv_copy(load_balancer *main, server_details *copy, uint tag)
{
    int found = 0;
    uint i;
    for (i = 0 ; i < 3 * main->num_servers + tag; i++) {
        if (copy->hash < main->hashring[i]->hash) {
            // move all the elements afterwards to the right
            for (uint j = 3 * main->num_servers + tag; j > i; j--)
                main->hashring[j] = main->hashring[j - 1];
            main->hashring[i] = copy;
            found = 1;
            break;
        }
    }
    if (found == 0) {
        // place the server copy on the last position of the hashring
        main->hashring[i] = copy;
    }

    return i;
}

// balance the objects after inserting a new server copy in the hashring
void redistribute_for_add(load_balancer *main, server_details *copy,
                         uint pos, uint tag)
{
    /* pointer to the right neighbour (the next server copy in the hashring
    that follows after the one that we have just inserted) */
    server_details *right_neigh;

    // check if the new copy/server is the last one in the array
    if (pos == 3 * main->num_servers + tag)
        right_neigh = main->hashring[0];
    else
        right_neigh = main->hashring[pos + 1];

    // check if the right neighbour is also a copy of the same server
    if (right_neigh->server_id == copy->server_id)
        return;

    // right neighbour's hashtable
    hashtable_t *right_neigh_ht = ((server_memory *)right_neigh->server)->ht;

    if (right_neigh_ht->size == 0)
        return;

    // check if the new copy is the first element of the hashring
    if (pos == 0) {
        /* browse all the buckets in right_neigh server and then
        go through all the nodes of the lists */
        for (uint i = 0; i < right_neigh_ht->hmax; i++) {
            ll_node_t *curr = right_neigh_ht->buckets[i]->head;
            while (curr) {
                uint hash_key = hash_function_key(((info *)curr->data)->key);
                int last_index = 3 * main->num_servers + tag;

                if (hash_key <= copy->hash ||
                    hash_key >= main->hashring[last_index]->hash) {
                    // move it to the new server copy
                    char *key = ((info *)curr->data)->key;
                    char *value = ((info *)curr->data)->value;
                    server_store(copy->server, key, value);
                    curr = curr->next;
                    server_remove(right_neigh->server, key);
                    continue;
                }

                curr = curr->next;
            }
        }
        return;
    }

    /* browse all the buckets in right_neigh server and then
    go through all the nodes of the lists */
    for (uint i = 0; i < right_neigh_ht->hmax; i++) {
        ll_node_t *curr = right_neigh_ht->buckets[i]->head;
        while (curr) {
            uint hash_key = hash_function_key(((info *)curr->data)->key);

            if (hash_key <= copy->hash &&
                hash_key >= main->hashring[pos - 1]->hash) {
                // move it to the new server copy
                char *key = ((info *)curr->data)->key;
                char *value = ((info *)curr->data)->value;
                server_store(copy->server, key, value);
                curr = curr->next;
                server_remove(right_neigh->server, key);
                continue;
            }

            curr = curr->next;
        }
    }
}

void loader_add_server(load_balancer *main, int server_id)
{
    // create all the 3 copies of the new server
    server_details *copy0;
    server_details *copy1;
    server_details *copy2;
    copy0 = create_new_server_copy(server_id, 0);
    copy1 = create_new_server_copy(server_id, 1);
    copy2 = create_new_server_copy(server_id, 2);

    int i0, i1, i2;

    // check if there are any other servers in the array
    if (main->num_servers == 0) {
        main->hashring = realloc(main->hashring, 3 * sizeof(server_details *));
        DIE(!main->hashring, "Failed to reallocate memory\n");

        // insert all the new copies in the hashring
        main->hashring[0] = copy0;
        i0 = 0;
        i1 = insert_a_sv_copy(main, copy1, 1);
        i2 = insert_a_sv_copy(main, copy2, 2);

        main->num_servers++;
        return;
    }

    // there already are serves in the hashring

    // update hashring size
    int hr_size = (3 * (main->num_servers + 1));

    int struct_size = sizeof(server_details *);
    main->hashring = realloc(main->hashring, hr_size * struct_size);
    DIE(!main->hashring, "Failed to reallocate memory\n");

    // insert all the 3 new copies in the hashring and balance the objects

    i0 = insert_a_sv_copy(main, copy0, 0);
    redistribute_for_add(main, copy0, i0, 0);

    i1 = insert_a_sv_copy(main, copy1, 1);
    redistribute_for_add(main, copy1, i1, 1);

    i2 = insert_a_sv_copy(main, copy2, 2);
    redistribute_for_add(main, copy2, i2, 2);

    main->num_servers++;
}

void loader_remove_server(load_balancer *main, int server_id) {
    // look for all the copies of the server in the hashring

    // counts how many copies have been eliminated
    uint eliminated = 0;

    for (uint i = 0; i < 3 * main->num_servers - eliminated; i++) {
        // check if we have already eliminated all the server copies
        if (eliminated == 3)
            break;

        if (main->hashring[i]->server_id == server_id) {
            server_details *to_be_deleted = main->hashring[i];
            hashtable_t *to_be_deleted_ht = main->hashring[i]->server->ht;

            // move all the products on the next server

            int next_sv_pos;
            // check if it is the last server
            if (i == 3 * main->num_servers - eliminated - 1) {
                // the next server is actually the first server from the array
                next_sv_pos = 0;
            } else {
                next_sv_pos = i + 1;
            }

            server_memory *next_sv = main->hashring[next_sv_pos]->server;

            if (to_be_deleted_ht->size != 0) {
                /* browse all the buckets in the server that must be deleted and
                then go through all the nodes of the lists */
                for (uint k = 0; k < to_be_deleted_ht->hmax; k++) {
                    ll_node_t *curr = to_be_deleted_ht->buckets[k]->head;
                    while (curr) {
                        // move it to the next server
                        char *key = ((info *)curr->data)->key;
                        char *value = ((info *)curr->data)->value;
                        server_store(next_sv, key, value);
                        curr = curr->next;
                        server_remove(to_be_deleted->server, key);
                    }
                }
            }

            free_server_memory(to_be_deleted->server);
            free(to_be_deleted);

            // update hashring size
            uint hr_size = 3 * main->num_servers - eliminated;

            // move all the elements afterwards to the left
            for (unsigned int j = i; j < hr_size - 1; j++) {
		        main->hashring[j] = main->hashring[j + 1];
	        }

            eliminated++;
            i--;

            // update hashring size
            hr_size = 3 * main->num_servers - eliminated;

            int struct_size = sizeof(server_details *);
            main->hashring = realloc(main->hashring, hr_size * struct_size);
            DIE(!main->hashring, "Failed to reallocate memory\n");
        }
    }

    main->num_servers--;
}

void loader_store(load_balancer *main, char *key, char *value, int *server_id) {
    // find the server on which we have to store the key
    int found = 0;
    uint i = 0;
    for (i = 0; i < 3 * main->num_servers; i++)
        if (hash_function_key(key) < main->hashring[i]->hash) {
            found = 1;
            break;
        }

    /* the array is circular, so if we haven't found a server that satisfies
    the condition, it means that we should place the product on the first server */
    if (!found)
        i = 0;

    server_store(main->hashring[i]->server, key, value);

    // store the id of the server on which we put the element
    *server_id = main->hashring[i]->server_id;
}

char *loader_retrieve(load_balancer *main, char *key, int *server_id) {
    // find the server on which we have to store the key
    int found = 0;
    uint i = 0;
    for (i = 0; i < 3 * main->num_servers; i++)
        if (hash_function_key(key) < main->hashring[i]->hash) {
            found = 1;
            break;
        }

    /* the array is circular, so if we haven't found a server that satisfies
    the condition, it means that we should place the product on the first server */
    if (!found)
        i = 0;

    // store the id of the server from which we retrieve the element
    *server_id = main->hashring[i]->server_id;

    char *value;
    // vezi daca merge sa scoti paranteza cu sv info * din fata
    value = server_retrieve(main->hashring[i]->server, key);
    return value;
}

void free_load_balancer(load_balancer *main) {
    // free the memory for each element of the hashring
    for (uint i = 0; i < 3 * main->num_servers; i++) {
        free_server_memory(main->hashring[i]->server);
        free(main->hashring[i]);
    }
    free(main->hashring);
    free(main);
}
