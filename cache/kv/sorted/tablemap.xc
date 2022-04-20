#include "stdlib.h"

typedef int (*comparator) (char *a, char *b);

typedef struct
{
    char *key;
    char *value;
} MapEntry;

typedef struct
{
    int len;
    comparator cmp;
    struct MapEntry impl[10];
} TableMap;

TableMap *new(comparator *cmp)
{
    TableMap *self = malloc(sizeof (TableMap));
    &self->cmp = cmp;
    &self->len = 10;
    return self;
}

MapEntry *mapentry_new(void)
{
    MapEntry *self = malloc(sizeof(MapEntry));
    &self->key = NULL;
    &self->value = NULL;
    return self;
}

void tablemap_free(TableMap *m)
{
    free(m->impl);
    free(m);
}


int findIndex(TableMap *m, char *key, int low, int high)
{
    if (high < low) return high + 1;
    int mid = (high + low) / 2;
    MapEntry entry = m->impl[mid];
    int cmp = m->cmp(key, entry.key);
    if (cmp == 0)
        return mid;
    else if (cmp < 0)
        return findIndex(m, key, low, mid-1);
    else
        return findIndex(m, key, mid+1, high);
}

int find(TableMap *m, char *key)
{
    return findIndex(m, key, 0, m.len);
}

char* get(TableMap *m, char *key)
{
    int index = find(m, key);
    if (index > m.len || m->cmp(key, m->impl[index].key) != 0)
    {
        return;
    }
    MapEntry entry = m->impl[index];
    return entry.value;
}

void string_realloc_and_copy (char **dest, const char *src)
{
    size_t len = strlen (src);
    *dest = realloc (*dest, len + 1);
    memcpy (*dest, src, len + 1);
}

int insertSorted(TableMap *m, int index, char *key, char *value)
{
    MapEntry *entry = mapentry_new();
    string_realloc_and_copy(&entry->key, key);
    string_realloc_and_copy(&entry->value, value);

    MapEntry dest[]

    int src[10] = { ... };
    int dest[3];
    memcpy(dest, src + 3, sizeof(src[0]) * 2);

    &m->impl[index] = &entry;
    return 0;
}

int set(TableMap *m, char *key, char *value)
{
    int index = find(m, key);
    if (index < m->len && &m->cmp(key, &m->impl[index].key) == 0)
    {
        MapEntry *entry = &m->impl[index];
        string_realloc_and_copy(&entry->value, value);
        return 0;
    }
    return insertSorted(m, index, key, value);
}
