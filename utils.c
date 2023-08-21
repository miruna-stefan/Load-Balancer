/* Copyright 2023 < Stefan Miruna Andreea, 314CA > */
#include <stdlib.h>
#include <stdio.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "utils.h"


linked_list_t*
ll_create(unsigned int data_size)
{
	linked_list_t* ll = calloc(1, sizeof(*ll));
	DIE(!ll, "calloc list");

	ll->data_size = data_size;

	return ll;
}

ll_node_t*
get_nth_node(linked_list_t* list, unsigned int n)
{
	unsigned int len = list->size - 1;
	unsigned int i;
	ll_node_t* node = list->head;

	n = MIN(n, len);

	for (i = 0; i < n; ++i)
		node = node->next;

	return node;
}

ll_node_t*
create_node(const void* new_data, unsigned int data_size)
{
	ll_node_t* node = calloc(1, sizeof(*node));
	DIE(!node, "calloc node");

	node->data = malloc(data_size);
	DIE(!node->data, "malloc data");

	memcpy(node->data, new_data, data_size);

	return node;
}

void
ll_add_nth_node(linked_list_t* list, unsigned int n, const void* new_data)
{
	ll_node_t *new_node, *prev_node;

	if (!list)
		return;

	new_node = create_node(new_data, list->data_size);

	if (!n || !list->size) {
		new_node->next = list->head;
		list->head = new_node;
	} else {
		prev_node = get_nth_node(list, n - 1);
		new_node->next = prev_node->next;
		prev_node->next = new_node;
	}

	++list->size;
}

ll_node_t*
ll_remove_nth_node(linked_list_t* list, unsigned int n)
{
	ll_node_t *prev_node, *removed_node;

	if (!list || !list->size)
		return NULL;

	if (!n) {
		removed_node = list->head;
		list->head = removed_node->next;
		removed_node->next = NULL;
	} else {
		prev_node = get_nth_node(list, n - 1);
		removed_node = prev_node->next;
		prev_node->next = removed_node->next;
		removed_node->next = NULL;
	}

	--list->size;

	return removed_node;
}

unsigned int
ll_get_size(linked_list_t* list)
{
	return !list ? 0 : list->size;
}

void
ll_free(linked_list_t** pp_list)
{
	ll_node_t* node;

	if (!pp_list || !*pp_list)
		return;

	while ((*pp_list)->size) {
		node = ll_remove_nth_node(*pp_list, 0);
		free(node->data);
		free(node);
	}

	free(*pp_list);
	*pp_list = NULL;
}

void
ll_print_int(linked_list_t* list)
{
	ll_node_t* node = list->head;

	for (; node; node = node->next)
		printf("%d ", *(int*)node->data);
	printf("\n");
}

void
ll_print_string(linked_list_t* list)
{
	ll_node_t* node = list->head;

	for (; node; node = node->next)
		printf("%s ", (char*)node->data);
	printf("\n");
}

int
compare_function_ints(void *a, void *b)
{
	int int_a = *((int *)a);
	int int_b = *((int *)b);

	if (int_a == int_b) {
		return 0;
	} else if (int_a < int_b) {
		return -1;
	} else {
		return 1;
	}
}

int
compare_function_strings(void *a, void *b)
{
	char *str_a = (char *)a;
	char *str_b = (char *)b;

	return strcmp(str_a, str_b);
}

unsigned int
hash_function_int(void *a)
{
	unsigned int uint_a = *((unsigned int *)a);

	uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
	uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
	uint_a = (uint_a >> 16u) ^ uint_a;
	return uint_a;
}

unsigned int
hash_function_string(void *a)
{
	unsigned char *puchar_a = (unsigned char*) a;
	unsigned long hash = 5381;
	int c;

	while ((c = *puchar_a++))
		hash = ((hash << 5u) + hash) + c; /* hash * 33 + c */

	return hash;
}

hashtable_t *
ht_create(unsigned int hmax, unsigned int (*hash_function)(void*),
		int (*compare_function)(void*, void*))
{
	if (!hash_function || !compare_function) {
        return NULL;
    }

    hashtable_t* map = malloc(sizeof(hashtable_t));
    DIE(!map, "Error");

    map->size = 0;
    map->hmax = hmax;
    map->hash_function = hash_function;
    map->compare_function = compare_function;

    map->buckets = malloc(map->hmax * sizeof(linked_list_t *));
    DIE(!map->buckets, "Error");
    for (unsigned int i = 0; i < map->hmax; ++i) {
        map->buckets[i] = ll_create(sizeof(info));
    }

    return map;
}

int
ht_has_key(hashtable_t *ht, void *key)
{
	if (!ht || !key) {
        return -1;
    }

    int hash_index = ht->hash_function(key) % ht->hmax;
    ll_node_t* node = ht->buckets[hash_index]->head;

    while (node != NULL) {
        info* data_info = (info *)node->data;
        if (!ht->compare_function(data_info->key, key)) {
            return 1;
        }
        node = node->next;
    }

	return 0;
}

void *
ht_get(hashtable_t *ht, void *key)
{
	if (!ht || !key || ht_has_key(ht, key) != 1) {
        return NULL;
    }

    int hash_index = ht->hash_function(key) % ht->hmax;
    ll_node_t* node = ht->buckets[hash_index]->head;

    while (node != NULL) {
        info* data_info = (info *)node->data;
        if (!ht->compare_function(data_info->key, key)) {
            return data_info->value;
        }
        node = node->next;
    }

	return NULL;
}

void
ht_put(hashtable_t *ht, void *key, unsigned int key_size,
	void *value, unsigned int value_size)
{
	if (!ht || !key || !value) {
        return;
    }

    int hash_index = ht->hash_function(key) % ht->hmax;

    if (ht_has_key(ht, key) == 1) {
        ll_node_t* node = ht->buckets[hash_index]->head;
        while (node != NULL) {
            info* data_info = node->data;

            if (!ht->compare_function(data_info->key, key)) {
                free(data_info->value);

                data_info->value = malloc(value_size);
                DIE(!data_info, "Error");

                memcpy(data_info->value, value, value_size);
                return;
            }

            node = node->next;
        }
    }

    info* data_info = malloc(sizeof(info));
    DIE(!data_info, "Error");

    data_info->key = malloc(key_size);
    DIE(!data_info->key, "Error");
    data_info->value = malloc(value_size);
    DIE(!data_info->value, "Error");

    memcpy(data_info->key, key, key_size);
    memcpy(data_info->value, value, value_size);

    ll_add_nth_node(ht->buckets[hash_index], 0, data_info);
    ht->size++;

    free(data_info);
}

void
ht_remove_entry(hashtable_t *ht, void *key)
{
	if (!ht || !key || ht_has_key(ht, key) != 1) {
        return;
    }

    int hash_index = ht->hash_function(key) % ht->hmax;
    ll_node_t* node = ht->buckets[hash_index]->head;

    unsigned int node_nr = 0;

    while (node != NULL) {
        info* data_info = (info *) node->data;

        if (!ht->compare_function(data_info->key, key)) {
            free(data_info->key);
            free(data_info->value);
            free(data_info);

            ll_node_t* deleted_node;
			int hash_idx = hash_index;
			deleted_node = ll_remove_nth_node(ht->buckets[hash_idx], node_nr);
            free(deleted_node);

            ht->size--;
            return;
        }

        node = node->next;
        node_nr++;
    }
}

void
ht_free(hashtable_t *ht)
{
	if (!ht) {
        return;
    }

    for (unsigned int i = 0; i < ht->hmax; ++i) {
        ll_node_t* node = ht->buckets[i]->head;

        while (node != NULL) {
            info* data_info = (info *)node->data;
            free(data_info->key);
            free(data_info->value);
            node = node->next;
        }

        ll_free(&ht->buckets[i]);
    }

    free(ht->buckets);
    free(ht);
}

unsigned int
ht_get_size(hashtable_t *ht)
{
	if (ht == NULL)
		return 0;

	return ht->size;
}

unsigned int
ht_get_hmax(hashtable_t *ht)
{
	if (ht == NULL)
		return 0;

	return ht->hmax;
}
