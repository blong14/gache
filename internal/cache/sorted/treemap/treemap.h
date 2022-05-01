typedef struct MapEntry MapEntry;

 struct MapEntry {
    MapEntry *left, *right;
    char *key, *value;
};

MapEntry *new_map_entry(char *key, char *value);
MapEntry *get(MapEntry *start, char *key);
MapEntry *set(MapEntry *start, char *key, char *value);

void free_map_entry(MapEntry *n);

