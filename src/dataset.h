#ifndef DATASET_H
#define DATASET_H

#include <stdlib.h>
#include <jansson.h>
#include "minilang/stringmap.h"

typedef enum {COLUMN_STRING, COLUMN_REAL} column_type_t;

typedef struct column_t column_t;

column_type_t column_get_type(column_t *Column);
size_t column_get_length(column_t *Column);

size_t column_string_get_length(column_t *Column, size_t Index);
void column_string_get_value(column_t *Column, size_t Index, char *Buffer);
void column_string_set(column_t *Column, size_t Index, const char *Value, int Length);

double column_real_get(column_t *Column, size_t Index);
void column_real_set(column_t *Column, size_t Index, double Value);

typedef struct dataset_t dataset_t;

dataset_t *dataset_create(const char *Path, const char *Name, size_t Length);
dataset_t *dataset_open(const char *Path);

json_t *dataset_get_info(dataset_t *Dataset);
size_t dataset_get_length(dataset_t *Dataset);

size_t dataset_get_column_count(dataset_t *Dataset);
column_type_t dataset_get_column_type(dataset_t *Dataset, size_t Index);
const char *dataset_get_column_name(dataset_t *Dataset, size_t Index);

column_t *dataset_column_create(dataset_t *Dataset, const char *Name, column_type_t Type);
column_t *dataset_column_open(dataset_t *Dataset, size_t Index);

void dataset_init(stringmap_t *Globals);

#endif
