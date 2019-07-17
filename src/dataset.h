#ifndef DATASET_H
#define DATASET_H

#include <stdlib.h>
#include <jansson.h>
#include "minilang/stringmap.h"

typedef enum {COLUMN_INVALID, COLUMN_STRING, COLUMN_REAL} column_type_t;

typedef struct column_t column_t;

column_type_t column_get_type(column_t *Column);
const char *column_get_id(column_t *Column);
size_t column_get_length(column_t *Column);

size_t column_string_get_length(column_t *Column, size_t Index);
void column_string_get_value(column_t *Column, size_t Index, char *Buffer);
void column_string_set(column_t *Column, size_t Index, const char *Value, int Length);

double column_real_get(column_t *Column, size_t Index);
void column_real_set(column_t *Column, size_t Index, double Value);

void column_watcher_add(column_t *Column, const char *Key, void *Value);
void column_watcher_remove(column_t *Column, const char *Key);
void column_watcher_foreach(column_t *Column, void *Data, int (*Callback)(const char *, void *, void *));

typedef struct dataset_t dataset_t;

dataset_t *dataset_create(const char *Path, const char *Name, size_t Length);
dataset_t *dataset_open(const char *Path);

json_t *dataset_get_info(dataset_t *Dataset);
size_t dataset_get_length(dataset_t *Dataset);

size_t dataset_get_column_count(dataset_t *Dataset);
column_type_t dataset_get_column_type(dataset_t *Dataset, const char *Id);
const char *dataset_get_column_name(dataset_t *Dataset, const char *Id);
json_t *dataset_get_column_info(dataset_t *Dataset, const char *Id);

column_t *dataset_column_create(dataset_t *Dataset, const char *Name, column_type_t Type);
column_t *dataset_column_open(dataset_t *Dataset, const char *Id);

void dataset_watcher_add(dataset_t *Dataset, const char *Key, void *Value);
void dataset_watcher_remove(dataset_t *Dataset, const char *Key);
void dataset_watcher_foreach(dataset_t *Dataset, void *Data, int (*Callback)(const char *, void *, void *));

void dataset_init(stringmap_t *Globals);

#endif
