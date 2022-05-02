#include "stdlib.h"
#include "string.h"
#include "treemap.h"

MapEntry *new_map_entry(char *key, char *value)
{
    MapEntry *n = (MapEntry*)malloc(sizeof(MapEntry));
    if (n == NULL)
        return n;
    n->left = NULL;
    n->right = NULL;
    n->key = key;
    n->value = value;
    return n;
}

void free_map_entry(MapEntry *n)
{
    if (n == NULL) return;
    free_map_entry(n->left);
    free_map_entry(n->right);
    free(n->key);
    free(n->value);
    free(n);
}

int compare(const char *a, const char *b)
{
    return strcmp(a, b);
}

MapEntry *search(MapEntry *start, char *key)
{
    if (start == NULL)
        return NULL;
    int comp = compare(start->key, key);
    if (comp < 0)
        return search(start->left, key);
    else if (comp == 0)
        return start;
    else
        return search(start->right, key);
}

MapEntry *xsearch(MapEntry *start, char *key)
{
    if (start == NULL)
        return NULL;
    int comp;
    MapEntry *next = start;
    do {
        comp = compare(next->key, key);
        if (comp < 0) {
            next = next->left;
            continue;
        } else if (comp == 0) {
            return next;
        } else {
            next = next->right;
            continue;
        }
    } while (next != NULL);
    return NULL;
}

MapEntry *get(MapEntry *start, char *key)
{
    if (start == NULL)
        return NULL;
    return xsearch(start, key);
}

MapEntry *insert(MapEntry *start, char *key, char *value)
{
    if (start == NULL) {
        return new_map_entry(key, value);
    }
    int comp = compare(start->key, key);
    if (comp < 0) {
        start->left = insert(start->left, key, value);
    } else if (comp == 0) {
        start->value = value;
    } else {
        start->right = insert(start->right, key, value);
    }
    return start;
}

MapEntry *set(MapEntry *start, char *key, char *value)
{
    return insert(start, key, value);
}
